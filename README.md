# tob2toa

A Python utility to convert Campbell Scientific TOB3 binary data files to TOA5 ASCII format.

This tool aims to replicate the functionality of Campbell Scientific's CardConvert utility, allowing for easy conversion of binary data files on non-Windows systems or as part of automated data processing pipelines.

## Features

- Supports TOB3 binary format.
- Automatically handles various Campbell Scientific data types:
  - IEEE4, IEEE4B, FP4, IEEE8, IEEE8B
  - INT2, UINT2, INT4, UINT4
  - BOOL, BOOL1, BOOL2, BOOL4
  - ASCII(N)
- Correctly handles timestamps and record numbers.
- Supports batch conversion of multiple files.
- Command-line interface and Python API.

## Installation

You can install `tob2toa` from source:

```bash
git clone https://github.com/paultgriffiths/tob2toa.git
cd tob2toa
pip install .
```

For development:

```bash
pip install -e ".[dev]"
```

## Usage

### Command Line

Convert a single file:

```bash
python -m tob2toa.tob2toa input_file.dat
```

Convert a single file with a specific output name:

```bash
python -m tob2toa.tob2toa input_file.dat output_file.dat
```

Batch convert all `.dat` files in a directory:

```bash
python -m tob2toa.tob2toa *.dat
```

### Python API

```python
from tob2toa.tob2toa import convert_tob3_to_toa5

# Convert a file
output_path = convert_tob3_to_toa5("path/to/input.dat")
print(f"Converted file saved to: {output_path}")
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
