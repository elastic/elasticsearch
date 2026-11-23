"""
POSTs an ES|QL query to Elasticsearch, reads the Arrow IPC stream response with the official pyarrow
library, and prints a JSON description of what was read to stdout. Exits non-zero (message on stderr)
if the request fails or the stream cannot be decoded.

Inputs (environment): ARROW_URL, ARROW_BODY (JSON request body).

Output: {"batches": [row counts], "columns": [{"name", "type", "nullable", "metadata", "values"}]}

The output is normalized so that fetch_arrow.mjs produces exactly the same JSON for the same stream, which
lets tests keep a single table of expected values. Keep both scripts in sync. The conventions are:
- type names are pyarrow's, e.g. "int32", "string", "timestamp[ms]", "list<string>", "struct<a: int32>"
  (types we don't know are reported as "unknown:<native name>" so that they show up in tests);
- timestamps and dates are epoch milliseconds (sub-millisecond precision is truncated, the type name still
  carries the original unit);
- integers outside of the JS safe integer range are strings;
- floating point values that are integers are printed without a decimal part (JS makes no difference between 1.0 and 1);
- decimal values are not normalized (they are reported as unknown by Elasticsearch anyway);
- binary values are {"base64": "..."};
- NaN and infinities are the strings "NaN", "Infinity" and "-Infinity".
"""
import base64
import json
import math
import os
import sys
import urllib.error
import urllib.request

import pyarrow as pa
import pyarrow.ipc as ipc

MAX_SAFE_INTEGER = 2**53 - 1


def type_name(t):
    if pa.types.is_integer(t):
        return ("u" if pa.types.is_unsigned_integer(t) else "") + "int%d" % t.bit_width
    if pa.types.is_float16(t):
        return "halffloat"
    if pa.types.is_float32(t):
        return "float"
    if pa.types.is_float64(t):
        return "double"
    if pa.types.is_boolean(t):
        return "bool"
    if pa.types.is_null(t):
        return "null"
    if pa.types.is_string(t):
        return "string"
    if pa.types.is_large_string(t):
        return "large_string"
    if pa.types.is_binary(t):
        return "binary"
    if pa.types.is_large_binary(t):
        return "large_binary"
    if pa.types.is_fixed_size_binary(t):
        return "fixed_size_binary[%d]" % t.byte_width
    if pa.types.is_timestamp(t):
        return "timestamp[%s%s]" % (t.unit, ", tz=" + t.tz if t.tz else "")
    if pa.types.is_decimal128(t) or pa.types.is_decimal256(t):
        return "decimal%d(%d, %d)" % (t.bit_width, t.precision, t.scale)
    if pa.types.is_date32(t):
        return "date32"
    if pa.types.is_date64(t):
        return "date64"
    if pa.types.is_fixed_size_list(t):
        return "fixed_size_list<%s>[%d]" % (type_name(t.value_type), t.list_size)
    if pa.types.is_list(t):
        return "list<%s>" % type_name(t.value_type)
    if pa.types.is_struct(t):
        return "struct<%s>" % ", ".join("%s: %s" % (f.name, type_name(f.type)) for f in t)
    if pa.types.is_dictionary(t):
        return "dictionary<values=%s, indices=%s>" % (type_name(t.value_type), type_name(t.index_type))
    return "unknown:%s" % t


def to_millis(value, unit):
    """Converts a timestamp in the given unit to epoch milliseconds, rounding towards negative infinity."""
    if unit == "s":
        return value * 1000
    return value // {"ms": 1, "us": 1000, "ns": 1000000}[unit]


def number(v):
    if isinstance(v, float) and not math.isfinite(v):
        return "NaN" if math.isnan(v) else ("Infinity" if v > 0 else "-Infinity")
    if isinstance(v, float) and v.is_integer() and abs(v) <= MAX_SAFE_INTEGER:
        return int(v)  # JS can't tell 1.0 from 1
    if isinstance(v, int) and abs(v) > MAX_SAFE_INTEGER:
        return str(v)
    return v


def convert(scalar):
    """Converts a pyarrow scalar to its normalized JSON-able value."""
    if not scalar.is_valid:
        return None
    t = scalar.type
    if pa.types.is_timestamp(t):
        return number(to_millis(scalar.value, t.unit))
    if pa.types.is_date32(t):
        return number(scalar.value * 86400000)
    if pa.types.is_date64(t):
        return number(scalar.value)
    if pa.types.is_list(t) or pa.types.is_fixed_size_list(t):
        return [convert(v) for v in scalar.values]
    if pa.types.is_struct(t):
        return {name: convert(v) for name, v in scalar.items()}
    if pa.types.is_dictionary(t):
        return convert(scalar.value)
    value = scalar.as_py()
    if isinstance(value, (bytes, bytearray)):
        return {"base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return number(value)
    if isinstance(value, (bool, str)):
        return value
    return str(value)  # decimals, times, ...


def main():
    request = urllib.request.Request(
        os.environ["ARROW_URL"],
        data=os.environ["ARROW_BODY"].encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/vnd.apache.arrow.stream",
        },
        method="POST",
    )
    try:
        response = urllib.request.urlopen(request)
    except urllib.error.HTTPError as e:
        sys.exit("HTTP %d: %s" % (e.code, e.read().decode("utf-8", "replace")))

    with response:
        reader = ipc.open_stream(response)
        schema = reader.schema
        batch_sizes = []
        values = [[] for _ in schema]
        for batch in reader:
            batch_sizes.append(batch.num_rows)
            for i, column in enumerate(batch.columns):
                values[i].extend(convert(column[row]) for row in range(len(column)))

    columns = []
    for i, field in enumerate(schema):
        metadata = {k.decode(): v.decode() for k, v in (field.metadata or {}).items()}
        columns.append(
            {
                "name": field.name,
                "type": type_name(field.type),
                "nullable": field.nullable,
                "metadata": metadata,
                "values": values[i],
            }
        )
    json.dump({"batches": batch_sizes, "columns": columns}, sys.stdout, allow_nan=False)


if __name__ == "__main__":
    main()
