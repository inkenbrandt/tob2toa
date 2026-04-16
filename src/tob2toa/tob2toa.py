"""
Campbell Scientific TOB3 Binary to TOA5 ASCII Converter

Converts TOB3 (.dat) binary data files from Campbell Scientific dataloggers
into the equivalent TOA5 comma-separated text format, matching the output
of Campbell's CardConvert utility.

TOB3 Binary Format Overview:
    - 6 text header lines (CRLF terminated), zero-padded to a block boundary
    - Data stored in fixed-size frames, each containing:
        - 12-byte frame header (4B seconds-since-1990 + 4B subseconds + 4B record number)
        - N bytes of record data (field values packed according to type definitions)
        - 4-byte frame footer (2B flags + 2B validation stamp)
    - Ring-buffer storage: only frames whose validation stamp matches the
      current stamp (from header line 1) contain current data

Supported data types: IEEE4B, IEEE4, FP4, IEEE8B, IEEE8, INT2, UINT2,
                      INT4, UINT4, BOOL, BOOL2, BOOL4, ASCII(N)

Usage:
    python tob3_to_toa5.py <input.dat> [<output.dat>]
    python tob3_to_toa5.py *.dat          # converts all non-TOA5 .dat files

If no output path is given, one is generated automatically:
    TOA5_<station>_<table>_<reccount>_<first_timestamp>.dat
"""

import struct
import csv
import io
import math
import sys
import os
import glob
from datetime import datetime, timedelta
import pandas as pd

# Base epoch for Campbell Scientific timestamps
CSI_EPOCH = datetime(1990, 1, 1)


def parse_header(raw_bytes):
    """Parse the 6 TOB3 header lines from the raw file bytes.

    Returns:
        dict with keys: lines (list of lists), header_end (int byte offset),
        table_name, frame_size, validation_stamp, time_resolution,
        field_names, field_units, field_proc, field_types,
        record_size, records_per_frame, station_name, logger_model,
        serial_no, os_version, program_name, program_sig, creation_ts
    """
    pos = 0
    lines = []
    for _ in range(6):
        end = raw_bytes.index(b"\r\n", pos)
        line_text = raw_bytes[pos:end].decode("ascii")
        fields = next(csv.reader(io.StringIO(line_text)))
        lines.append(fields)
        pos = end + 2

    # Line 0: file type info
    # "TOB3", station, logger_model, serial, os_version, program, prog_sig, creation_ts
    file_type = lines[0][0]
    if file_type != "TOB3":
        raise ValueError(f"Expected TOB3 file, got '{file_type}'")

    station_name = lines[0][1]
    logger_model = lines[0][2]
    serial_no = lines[0][3]
    os_version = lines[0][4]
    program_name = lines[0][5]
    program_sig = lines[0][6]
    creation_ts = lines[0][7] if len(lines[0]) > 7 else ""

    # Line 1: table metadata
    # table_name, interval, frame_size, intended_table_size, validation_stamp,
    # time_resolution, pad, pad, file_creation_seconds
    table_name = lines[1][0]
    frame_size = int(lines[1][2])
    validation_stamp = int(lines[1][4])
    time_resolution = lines[1][5].strip()

    # Lines 2-5: field names, units, processing, data types
    field_names = lines[2]
    field_units = lines[3]
    field_proc = lines[4]
    field_types = [t.strip() for t in lines[5]]

    # Compute record data size from types
    record_size = sum(_type_size(t) for t in field_types)

    # Frame layout: 12-byte header + data + 4-byte footer
    frame_overhead = 12 + 4  # header + footer
    data_per_frame = frame_size - frame_overhead
    records_per_frame = data_per_frame // record_size

    return {
        "lines": lines,
        "header_byte_end": pos,
        "table_name": table_name,
        "frame_size": frame_size,
        "validation_stamp": validation_stamp,
        "time_resolution": time_resolution,
        "field_names": field_names,
        "field_units": field_units,
        "field_proc": field_proc,
        "field_types": field_types,
        "record_size": record_size,
        "records_per_frame": records_per_frame,
        "station_name": station_name,
        "logger_model": logger_model,
        "serial_no": serial_no,
        "os_version": os_version,
        "program_name": program_name,
        "program_sig": program_sig,
        "creation_ts": creation_ts,
    }


def _type_size(dtype):
    """Return the byte size for a TOB3 data type string."""
    dtype_upper = dtype.upper()
    if dtype_upper in ("IEEE4B", "IEEE4", "FP4", "INT4", "UINT4", "LONG", "BOOL4"):
        return 4
    if dtype_upper in ("IEEE8B", "IEEE8"):
        return 8
    if dtype_upper in ("INT2", "UINT2", "BOOL2"):
        return 2
    if dtype_upper in ("BOOL", "BOOL1"):
        return 1
    if dtype_upper.startswith("ASCII("):
        return int(dtype_upper[6:].rstrip(")"))
    raise ValueError(f"Unknown data type: {dtype}")


def _decode_value(raw, dtype):
    """Decode a single field value from raw bytes according to its TOB3 type.

    Returns the decoded Python value (float, int, str, or None for NaN/invalid).
    """
    dtype_upper = dtype.upper()

    if dtype_upper in ("IEEE4B", "IEEE4", "FP4"):
        # Big-endian 4-byte IEEE 754 float (B suffix = big endian)
        val = struct.unpack(">f", raw)[0]
        if math.isnan(val) or math.isinf(val):
            return None  # Campbell NaN sentinel
        return val

    if dtype_upper in ("IEEE8B", "IEEE8"):
        val = struct.unpack(">d", raw)[0]
        if math.isnan(val) or math.isinf(val):
            return None
        return val

    if dtype_upper == "INT4":
        val = struct.unpack(">i", raw)[0]
        # Campbell INT4 NaN sentinel: -2147483648 (0x80000000)
        if val == -2147483648:
            return None
        return val

    if dtype_upper == "UINT4":
        val = struct.unpack(">I", raw)[0]
        return val

    if dtype_upper == "INT2":
        val = struct.unpack(">h", raw)[0]
        if val == -32768:
            return None
        return val

    if dtype_upper == "UINT2":
        return struct.unpack(">H", raw)[0]

    if dtype_upper in ("BOOL", "BOOL1"):
        return struct.unpack("B", raw)[0]

    if dtype_upper == "BOOL2":
        return struct.unpack(">H", raw)[0]

    if dtype_upper == "BOOL4":
        return struct.unpack(">I", raw)[0]

    if dtype_upper.startswith("ASCII("):
        # Fixed-width ASCII string, null-terminated within the field
        text = raw.split(b"\x00")[0].decode("ascii", errors="replace")
        return text

    raise ValueError(f"Cannot decode type: {dtype}")


def _format_value(val, dtype):
    """Format a decoded value as a TOA5 CSV field string."""
    dtype_upper = dtype.upper()

    if val is None:
        return '"NAN"'

    if dtype_upper.startswith("ASCII("):
        return f'"{val}"'

    if dtype_upper in ("IEEE4B", "IEEE4", "FP4"):
        return f"{val:.7G}"

    if dtype_upper in ("IEEE8B", "IEEE8"):
        return f"{val:.15G}"

    # Integer types
    if isinstance(val, int):
        return str(val)

    return str(val)


def _format_timestamp(seconds, subseconds, time_resolution):
    """Convert CSI epoch seconds + subseconds to a formatted timestamp string."""
    ts = CSI_EPOCH + timedelta(seconds=seconds)
    ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")

    # Add sub-second precision if non-zero
    if subseconds != 0:
        if time_resolution == "Sec100Usec":
            frac_seconds = subseconds * 100e-6
        elif time_resolution == "SecMsec":
            frac_seconds = subseconds * 1e-3
        elif time_resolution == "SecUsec":
            frac_seconds = subseconds * 1e-6
        else:
            frac_seconds = subseconds * 100e-6  # default

        # Format fractional part, strip trailing zeros
        frac_str = f"{frac_seconds:.6f}".rstrip("0").rstrip(".")
        if frac_str.startswith("0"):
            frac_str = frac_str[1:]  # remove leading 0, keep the dot
        ts_str += frac_str

    return ts_str


def _get_interval_seconds(interval_str):
    """Parse the interval string (e.g. '60 MIN', '100 MSEC') to seconds."""
    parts = interval_str.strip().split()
    if len(parts) != 2:
        return None
    amount = float(parts[0])
    unit = parts[1].upper()
    if unit in ("SEC", "SECONDS"):
        return amount
    if unit in ("MIN", "MINUTES"):
        return amount * 60
    if unit in ("HR", "HOUR", "HOURS"):
        return amount * 3600
    if unit == "MSEC":
        return amount / 1000.0
    if unit == "USEC":
        return amount / 1_000_000.0
    return None


def _extract_valid_records(file_data, hdr):
    """Extract valid TOB3 records from file bytes.

    Returns:
        list[tuple[int, int, int, list]]:
            Tuples are (seconds, subseconds, record_number, field_values).
    """
    frame_size = hdr["frame_size"]
    validation_stamp = hdr["validation_stamp"]
    record_size = hdr["record_size"]
    records_per_frame = hdr["records_per_frame"]
    field_types = hdr["field_types"]
    interval_str = hdr["lines"][1][1]

    # Data frames start immediately after the 6 header lines.
    data_start = hdr["header_byte_end"]
    total_frames = (len(file_data) - data_start) // frame_size

    # Compute field sizes and offsets for fast parsing.
    field_sizes = [_type_size(t) for t in field_types]
    field_offsets = []
    offset = 0
    for size in field_sizes:
        field_offsets.append(offset)
        offset += size

    interval_sec = _get_interval_seconds(interval_str)

    records = []
    for frame_idx in range(total_frames):
        frame_start = data_start + frame_idx * frame_size
        frame = file_data[frame_start : frame_start + frame_size]

        # Check validation stamp in footer (last 2 bytes of frame).
        vstamp = struct.unpack_from("<H", frame, frame_size - 2)[0]
        if vstamp != validation_stamp:
            continue

        # Frame header: seconds (4B LE) + subseconds (4B LE) + record number (4B LE).
        sec = struct.unpack_from("<I", frame, 0)[0]
        subsec = struct.unpack_from("<I", frame, 4)[0]
        base_recno = struct.unpack_from("<I", frame, 8)[0]

        for rec_idx in range(records_per_frame):
            rec_offset = 12 + rec_idx * record_size
            if rec_offset + record_size > frame_size - 4:
                break

            rec_data = frame[rec_offset : rec_offset + record_size]
            recno = base_recno + rec_idx

            # Advance timestamp for multi-record frames when interval is known.
            if rec_idx == 0:
                rec_sec = sec
                rec_subsec = subsec
            else:
                # Advance by interval for subsequent records in multi-record frames
                if interval_sec is not None:
                    total_usec = (
                        sec * 10_000 + subsec + int(rec_idx * interval_sec * 10_000)
                    )
                    rec_sec = total_usec // 10_000
                    rec_subsec = total_usec % 10_000
                else:
                    rec_sec = sec
                    rec_subsec = subsec

            values = []
            for i, dtype in enumerate(field_types):
                raw = rec_data[field_offsets[i] : field_offsets[i] + field_sizes[i]]
                values.append(_decode_value(raw, dtype))

            records.append((rec_sec, rec_subsec, recno, values))

    # Sort by record number to ensure chronological order.
    records.sort(key=lambda r: r[2])
    return records


def tob3_to_dataframe(input_path):
    """Parse a TOB3 binary file into a pandas DataFrame.

    Args:
        input_path: Path to the TOB3 .dat file.

    Returns:
        pandas.DataFrame with columns: TIMESTAMP, RECORD, and all TOB3 fields.
    """
    with open(input_path, "rb") as f:
        file_data = f.read()

    hdr = parse_header(file_data)
    records = _extract_valid_records(file_data, hdr)

    if not records:
        return pd.DataFrame(columns=["TIMESTAMP", "RECORD"] + hdr["field_names"])

    time_res = hdr["time_resolution"]
    columns = ["TIMESTAMP", "RECORD", *hdr["field_names"]]

    # Build row tuples directly to avoid per-row dict+zip overhead.
    rows = [
        (_format_timestamp(rec_sec, rec_subsec, time_res), recno, *values)
        for rec_sec, rec_subsec, recno, values in records
    ]

    return pd.DataFrame.from_records(rows, columns=columns)


def convert_tob3_to_toa5(input_path, output_path=None, verbose=True):
    """Convert a TOB3 binary file to TOA5 text format.

    Args:
        input_path: Path to the TOB3 .dat file
        output_path: Optional output path. Auto-generated if None.
        verbose: Print progress info

    Returns:
        Path to the output file
    """
    with open(input_path, "rb") as f:
        file_data = f.read()

    file_size = len(file_data)
    hdr = parse_header(file_data)

    frame_size = hdr["frame_size"]
    field_types = hdr["field_types"]
    field_names = hdr["field_names"]
    time_res = hdr["time_resolution"]
    data_start = hdr["header_byte_end"]
    total_frames = (file_size - data_start) // frame_size

    if verbose:
        print(f"  Table: {hdr['table_name']}")
        print(f"  Frame size: {frame_size}, Record size: {hdr['record_size']}")
        print(f"  Records/frame: {hdr['records_per_frame']}")
        print(f"  Total frames: {total_frames}")
        print(f"  Validation stamp: {hdr['validation_stamp']}")

    records = _extract_valid_records(file_data, hdr)

    if verbose:
        print(f"  Valid records: {len(records)}")

    if not records:
        print("  WARNING: No valid records found!")
        return None

    # Generate output filename if not provided
    if output_path is None:
        first_ts = _format_timestamp(records[0][0], records[0][1], time_res)
        ts_for_name = first_ts.replace("-", "_").replace(":", "").replace(" ", "_")
        # Remove subseconds from filename
        if "." in ts_for_name:
            ts_for_name = ts_for_name[: ts_for_name.index(".")]
        # Format: TOA5_<station>_<table>_<record_count>_<timestamp>.dat
        base_dir = os.path.dirname(input_path)
        output_path = os.path.join(
            base_dir,
            f"TOA5_{hdr['station_name']}_{hdr['table_name']}"
            f"_{len(records)}_{ts_for_name}.dat",
        )

    # Write TOA5 output
    with open(output_path, "w", newline="\r\n") as out:
        # Header line 0: environment info
        # Replace "TOB3" with "TOA5", replace creation_ts with table_name
        env_fields = [
            "TOA5",
            hdr["station_name"],
            hdr["logger_model"],
            hdr["serial_no"],
            hdr["os_version"],
            hdr["program_name"],
            hdr["program_sig"],
            hdr["table_name"],
        ]
        out.write(",".join(f'"{f}"' for f in env_fields) + "\n")

        # Header line 1: field names (prepend TIMESTAMP and RECORD)
        all_names = ["TIMESTAMP", "RECORD"] + field_names
        out.write(",".join(f'"{n}"' for n in all_names) + "\n")

        # Header line 2: units
        all_units = ["TS", "RN"] + hdr["field_units"]
        out.write(",".join(f'"{u}"' for u in all_units) + "\n")

        # Header line 3: processing types
        all_proc = ["", ""] + hdr["field_proc"]
        out.write(",".join(f'"{p}"' for p in all_proc) + "\n")

        # Data rows
        for rec_sec, rec_subsec, recno, values in records:
            ts_str = _format_timestamp(rec_sec, rec_subsec, time_res)
            formatted = [f'"{ts_str}"', str(recno)]
            for val, dtype in zip(values, field_types):
                formatted.append(_format_value(val, dtype))
            out.write(",".join(formatted) + "\n")

    if verbose:
        print(f"  Output: {output_path}")

    return output_path


def find_tob3_files(path):
    """Find all TOB3 .dat files in a directory (excluding TOA5 files)."""
    if os.path.isfile(path):
        return [path]

    dat_files = glob.glob(os.path.join(path, "*.dat"))
    tob3_files = []
    for f in dat_files:
        basename = os.path.basename(f)
        if basename.upper().startswith("TOA5"):
            continue
        # Quick check: read first few bytes to confirm TOB3
        try:
            with open(f, "rb") as fh:
                magic = fh.read(6)
            if magic == b'"TOB3"':
                tob3_files.append(f)
        except (IOError, OSError):
            pass

    return sorted(tob3_files)


def parse_toa5_header(file_path):
    """Parse the header of a TOA5 file to extract metadata.

    Returns:
        dict with keys: station_name, logger_model, serial_no, os_version,
        program_name, program_sig, table_name, field_names, field_units,
        field_proc
    """
    with open(file_path, "r") as f:
        lines = [next(f).strip() for _ in range(4)]

    # Line 0: environment info
    env_fields = next(csv.reader(io.StringIO(lines[0])))
    if env_fields[0] != "TOA5":
        raise ValueError(f"Expected TOA5 file, got '{env_fields[0]}'")

    return {
        "station_name": env_fields[1],
        "logger_model": env_fields[2],
        "serial_no": env_fields[3],
        "os_version": env_fields[4],
        "program_name": env_fields[5],
        "program_sig": env_fields[6],
        "table_name": env_fields[7],
        "field_names": next(csv.reader(io.StringIO(lines[1])))[
            2:
        ],  # skip TIMESTAMP, RECORD
        "field_units": next(csv.reader(io.StringIO(lines[2])))[2:],
        "field_proc": next(csv.reader(io.StringIO(lines[3])))[2:],
    }


def toa5_to_pandas(file_path):
    """Load a TOA5 file into a pandas DataFrame."""
    import pandas as pd

    header = parse_toa5_header(file_path)
    df = pd.read_csv(
        file_path,
        skiprows=4,
        names=["TIMESTAMP"] + ["RECORD"] + header["field_names"],
        parse_dates=True,
    )
    df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"], errors="coerce")
    return df, header


def main():
    if len(sys.argv) < 2:
        print("Usage: python tob3_to_toa5.py <input.dat> [output.dat]")
        print("       python tob3_to_toa5.py <directory>")
        print("       python tob3_to_toa5.py *.dat")
        sys.exit(1)

    args = sys.argv[1:]

    # Collect all input files
    input_files = []
    output_path = None

    if len(args) == 2 and not glob.has_magic(args[1]) and not os.path.isdir(args[1]):
        # Single file with explicit output
        input_files = find_tob3_files(args[0])
        output_path = args[1]
    else:
        for arg in args:
            if glob.has_magic(arg):
                for match in glob.glob(arg):
                    input_files.extend(find_tob3_files(match))
            elif os.path.isdir(arg):
                input_files.extend(find_tob3_files(arg))
            else:
                input_files.extend(find_tob3_files(arg))

    if not input_files:
        print("No TOB3 files found.")
        sys.exit(1)

    for i, input_file in enumerate(input_files):
        print(f"\nConverting [{i+1}/{len(input_files)}]: {input_file}")
        out = output_path if len(input_files) == 1 else None
        result = convert_tob3_to_toa5(input_file, out)
        if result:
            print(f"  Done: {result}")


if __name__ == "__main__":
    main()
