import pytest
import struct
import os
import math
from tob2toa.tob2toa import (
    _type_size,
    _decode_value,
    _format_value,
    _format_timestamp,
    _get_interval_seconds,
    parse_header,
    convert_tob3_to_toa5,
    tob3_to_dataframe,
    combine_tob3_files,
)

def test_type_size():
    assert _type_size("IEEE4") == 4
    assert _type_size("IEEE4B") == 4
    assert _type_size("FP4") == 4
    assert _type_size("INT4") == 4
    assert _type_size("UINT4") == 4
    assert _type_size("LONG") == 4
    assert _type_size("BOOL4") == 4
    assert _type_size("IEEE8") == 8
    assert _type_size("IEEE8B") == 8
    assert _type_size("INT2") == 2
    assert _type_size("UINT2") == 2
    assert _type_size("BOOL2") == 2
    assert _type_size("BOOL") == 1
    assert _type_size("BOOL1") == 1
    assert _type_size("ASCII(10)") == 10
    with pytest.raises(ValueError):
        _type_size("UNKNOWN")

def test_decode_value():
    # IEEE4
    assert _decode_value(struct.pack(">f", 1.23), "IEEE4") == pytest.approx(1.23)
    assert _decode_value(struct.pack(">f", float("nan")), "IEEE4") is None

    # IEEE8
    assert _decode_value(struct.pack(">d", 1.23456789), "IEEE8") == pytest.approx(1.23456789)

    # INT4
    assert _decode_value(struct.pack(">i", 12345), "INT4") == 12345
    assert _decode_value(struct.pack(">i", -2147483648), "INT4") is None

    # UINT4
    assert _decode_value(struct.pack(">I", 12345), "UINT4") == 12345

    # INT2
    assert _decode_value(struct.pack(">h", 123), "INT2") == 123
    assert _decode_value(struct.pack(">h", -32768), "INT2") is None

    # UINT2
    assert _decode_value(struct.pack(">H", 123), "UINT2") == 123

    # BOOL
    assert _decode_value(struct.pack("B", 1), "BOOL") == 1

    # ASCII
    assert _decode_value(b"Hello\x00\x00\x00\x00\x00", "ASCII(10)") == "Hello"

def test_format_value():
    assert _format_value(1.23, "IEEE4") == "1.23"
    assert _format_value(None, "IEEE4") == '"NAN"'
    assert _format_value(123, "INT4") == "123"
    assert _format_value("Hello", "ASCII(10)") == '"Hello"'

def test_format_timestamp():
    # 2023-01-01 00:00:00 is 1041379200 seconds after 1990-01-01 00:00:00?
    # No, CSI_EPOCH is 1990-01-01.
    # 2023-01-01 is 33 years after 1990.
    # 33 * 365.25 * 86400 approx 1,041,350,400
    from datetime import datetime
    delta = datetime(2023, 1, 1) - datetime(1990, 1, 1)
    seconds = int(delta.total_seconds())

    assert _format_timestamp(seconds, 0, "Sec100Usec") == "2023-01-01 00:00:00"
    assert _format_timestamp(seconds, 5000, "Sec100Usec") == "2023-01-01 00:00:00.5"
    assert _format_timestamp(seconds, 500, "SecMsec") == "2023-01-01 00:00:00.5"
    assert _format_timestamp(seconds, 500000, "SecUsec") == "2023-01-01 00:00:00.5"

def test_get_interval_seconds():
    assert _get_interval_seconds("1 SEC") == 1.0
    assert _get_interval_seconds("60 MIN") == 3600.0
    assert _get_interval_seconds("1 HR") == 3600.0
    assert _get_interval_seconds("100 MSEC") == 0.1
    assert _get_interval_seconds("1000 USEC") == 0.001
    assert _get_interval_seconds("INVALID") is None

def test_parse_header():
    header_text = (
        '"TOB3","Station1","CR1000X","12345","OS_1.0","Prog1","1234","2023-01-01 00:00:00"\r\n'
        '"MainTable","1 SEC",1024,10000,12345,"Sec100Usec","","",""\r\n'
        '"Temp","Press"\r\n'
        '"degC","hPa"\r\n'
        '"Smp","Smp"\r\n'
        '"IEEE4","IEEE4"\r\n'
    ).encode("ascii")

    hdr = parse_header(header_text)
    assert hdr["station_name"] == "Station1"
    assert hdr["table_name"] == "MainTable"
    assert hdr["frame_size"] == 1024
    assert hdr["validation_stamp"] == 12345
    assert hdr["field_names"] == ["Temp", "Press"]
    assert hdr["field_types"] == ["IEEE4", "IEEE4"]
    assert hdr["record_size"] == 8
    assert hdr["records_per_frame"] == (1024 - 16) // 8

def test_convert_tob3_to_toa5(tmp_path):
    header_text = (
        '"TOB3","Station1","CR1000X","12345","OS_1.0","Prog1","1234","2023-01-01 00:00:00"\r\n'
        '"MainTable","1 SEC",64,10000,12345,"Sec100Usec","","",""\r\n'
        '"Temp","Press"\r\n'
        '"degC","hPa"\r\n'
        '"Smp","Smp"\r\n'
        '"IEEE4","IEEE4"\r\n'
    ).encode("ascii")

    # Frame size 64. Overhead 16. Data 48. Record size 8. Records/frame 6.
    # Record: Temp (IEEE4), Press (IEEE4)

    from datetime import datetime
    delta = datetime(2023, 1, 1) - datetime(1990, 1, 1)
    base_sec = int(delta.total_seconds())

    # Frame 1
    # Header: sec (4B LE), subsec (4B LE), recno (4B LE)
    frame_hdr = struct.pack("<III", base_sec, 0, 1)
    # 6 records
    data = b""
    for i in range(6):
        data += struct.pack(">ff", 20.0 + i, 1013.0 + i)
    # Footer: flags (2B LE), vstamp (2B LE)
    footer = struct.pack("<HH", 0, 12345)

    frame = frame_hdr + data + footer
    assert len(frame) == 12 + 48 + 4 == 64

    tob_data = header_text + frame

    input_file = tmp_path / "test.dat"
    input_file.write_bytes(tob_data)

    output_file = tmp_path / "test_out.dat"
    convert_tob3_to_toa5(str(input_file), str(output_file), verbose=False)

    assert output_file.exists()
    content = output_file.read_text()
    lines = content.splitlines()

    assert '"TOA5","Station1","CR1000X","12345","OS_1.0","Prog1","1234","MainTable"' in lines[0]
    assert '"TIMESTAMP","RECORD","Temp","Press"' in lines[1]
    assert '"2023-01-01 00:00:00",1,20,1013' in lines[4]
    assert '"2023-01-01 00:00:05",6,25,1018' in lines[9]


def test_tob3_to_dataframe(tmp_path):
    header_text = (
        '"TOB3","Station1","CR1000X","12345","OS_1.0","Prog1","1234","2023-01-01 00:00:00"\r\n'
        '"MainTable","1 SEC",64,10000,12345,"Sec100Usec","","",""\r\n'
        '"Temp","Press"\r\n'
        '"degC","hPa"\r\n'
        '"Smp","Smp"\r\n'
        '"IEEE4","IEEE4"\r\n'
    ).encode("ascii")

    from datetime import datetime
    delta = datetime(2023, 1, 1) - datetime(1990, 1, 1)
    base_sec = int(delta.total_seconds())

    frame_hdr = struct.pack("<III", base_sec, 0, 1)
    data = b""
    for i in range(6):
        data += struct.pack(">ff", 20.0 + i, 1013.0 + i)
    footer = struct.pack("<HH", 0, 12345)
    frame = frame_hdr + data + footer

    input_file = tmp_path / "test_df.dat"
    input_file.write_bytes(header_text + frame)

    df = tob3_to_dataframe(str(input_file))

    assert list(df.columns) == ["TIMESTAMP", "RECORD", "Temp", "Press"]
    assert len(df) == 6
    assert df.iloc[0]["TIMESTAMP"] == "2023-01-01 00:00:00"
    assert df.iloc[0]["RECORD"] == 1
    assert df.iloc[0]["Temp"] == pytest.approx(20.0)
    assert df.iloc[-1]["TIMESTAMP"] == "2023-01-01 00:00:05"
    assert df.iloc[-1]["Press"] == pytest.approx(1018.0)


def _make_tob3_file(path, base_sec, start_recno, n_records, vstamp=12345):
    """Helper: write a minimal TOB3 file with n_records into path."""
    header_text = (
        '"TOB3","Station1","CR1000X","12345","OS_1.0","Prog1","1234","2023-01-01 00:00:00"\r\n'
        '"MainTable","1 SEC",64,10000,' + str(vstamp) + ',"Sec100Usec","","",""\r\n'
        '"Temp","Press"\r\n'
        '"degC","hPa"\r\n'
        '"Smp","Smp"\r\n'
        '"IEEE4","IEEE4"\r\n'
    ).encode("ascii")

    # Frame size 64 = 12 header + 48 data (6 records × 8 bytes) + 4 footer
    records_per_frame = 6
    data = b""
    for i in range(n_records):
        data += struct.pack(">ff", float(start_recno + i), float(1000 + start_recno + i))

    # Pad to full frames
    total_records_space = ((n_records + records_per_frame - 1) // records_per_frame) * records_per_frame
    for _ in range(total_records_space - n_records):
        data += struct.pack(">ff", 0.0, 0.0)

    frames = b""
    for frame_idx in range(total_records_space // records_per_frame):
        sec = base_sec + frame_idx * records_per_frame
        recno = start_recno + frame_idx * records_per_frame
        frame_hdr = struct.pack("<III", sec, 0, recno)
        frame_data = data[frame_idx * records_per_frame * 8 : (frame_idx + 1) * records_per_frame * 8]
        footer = struct.pack("<HH", 0, vstamp)
        frames += frame_hdr + frame_data + footer

    path.write_bytes(header_text + frames)


def test_combine_tob3_files_parquet(tmp_path):
    from datetime import datetime
    base_sec = int((datetime(2023, 1, 1) - datetime(1990, 1, 1)).total_seconds())

    _make_tob3_file(tmp_path / "file1.dat", base_sec, start_recno=1, n_records=6)
    _make_tob3_file(tmp_path / "file2.dat", base_sec + 6, start_recno=7, n_records=6)

    out = tmp_path / "combined.parquet"
    result = combine_tob3_files(str(tmp_path), str(out), verbose=False)

    assert result == str(out)
    assert out.exists()

    import pandas as pd
    df = pd.read_parquet(str(out))
    assert len(df) == 12
    assert list(df.columns) == ["TIMESTAMP", "RECORD", "Temp", "Press"]


def test_combine_tob3_files_sqlite(tmp_path):
    from datetime import datetime
    base_sec = int((datetime(2023, 1, 1) - datetime(1990, 1, 1)).total_seconds())

    _make_tob3_file(tmp_path / "file1.dat", base_sec, start_recno=1, n_records=6)
    _make_tob3_file(tmp_path / "file2.dat", base_sec + 6, start_recno=7, n_records=6)

    out = tmp_path / "combined.sqlite"
    result = combine_tob3_files(str(tmp_path), str(out), verbose=False)

    assert result == str(out)
    assert out.exists()

    import sqlite3
    conn = sqlite3.connect(str(out))
    rows = conn.execute("SELECT COUNT(*) FROM MainTable").fetchone()[0]
    conn.close()
    assert rows == 12


def test_combine_tob3_files_deduplication(tmp_path):
    from datetime import datetime
    base_sec = int((datetime(2023, 1, 1) - datetime(1990, 1, 1)).total_seconds())

    # Both files have the same record numbers → 6 unique after dedup
    _make_tob3_file(tmp_path / "file1.dat", base_sec, start_recno=1, n_records=6)
    _make_tob3_file(tmp_path / "file2.dat", base_sec, start_recno=1, n_records=6)

    out = tmp_path / "deduped.parquet"
    combine_tob3_files(str(tmp_path), str(out), deduplicate=True, verbose=False)

    import pandas as pd
    df = pd.read_parquet(str(out))
    assert len(df) == 6


def test_combine_tob3_files_explicit_format(tmp_path):
    from datetime import datetime
    base_sec = int((datetime(2023, 1, 1) - datetime(1990, 1, 1)).total_seconds())
    _make_tob3_file(tmp_path / "file1.dat", base_sec, start_recno=1, n_records=6)

    # Use explicit format with non-standard extension
    out = tmp_path / "output.bin"
    combine_tob3_files(str(tmp_path), str(out), output_format="parquet", verbose=False)

    import pandas as pd
    df = pd.read_parquet(str(out))
    assert len(df) == 6


def test_combine_tob3_files_no_files_raises(tmp_path):
    with pytest.raises(ValueError, match="No TOB3 files found"):
        combine_tob3_files(str(tmp_path), str(tmp_path / "out.parquet"), verbose=False)


def test_combine_tob3_files_unknown_format_raises(tmp_path):
    from datetime import datetime
    base_sec = int((datetime(2023, 1, 1) - datetime(1990, 1, 1)).total_seconds())
    _make_tob3_file(tmp_path / "file1.dat", base_sec, start_recno=1, n_records=6)

    with pytest.raises(ValueError, match="Cannot infer output format"):
        combine_tob3_files(str(tmp_path), str(tmp_path / "out.xyz"), verbose=False)


def test_combine_tob3_files_list_input(tmp_path):
    from datetime import datetime
    base_sec = int((datetime(2023, 1, 1) - datetime(1990, 1, 1)).total_seconds())

    f1 = tmp_path / "file1.dat"
    f2 = tmp_path / "file2.dat"
    _make_tob3_file(f1, base_sec, start_recno=1, n_records=6)
    _make_tob3_file(f2, base_sec + 6, start_recno=7, n_records=6)

    out = tmp_path / "combined.parquet"
    combine_tob3_files([str(f1), str(f2)], str(out), verbose=False)

    import pandas as pd
    df = pd.read_parquet(str(out))
    assert len(df) == 12
