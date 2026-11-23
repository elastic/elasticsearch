// POSTs an ES|QL query to Elasticsearch, reads the Arrow IPC stream response with the official
// apache-arrow library, and prints a JSON description of what was read to stdout. Exits non-zero
// (message on stderr) if the request fails or the stream cannot be decoded.
//
// Inputs (environment): ARROW_URL, ARROW_BODY (JSON request body).
//
// Output: {"batches": [row counts], "columns": [{"name", "type", "nullable", "metadata", "values"}]}
//
// The output is normalized so that fetch_arrow.py produces exactly the same JSON for the same stream, which
// lets tests keep a single table of expected values. Keep both scripts in sync. The conventions are:
// - type names are pyarrow's, e.g. "int32", "string", "timestamp[ms]", "list<string>", "struct<a: int32>"
//   (types we don't know are reported as "unknown:<native name>" so that they show up in tests);
// - timestamps and dates are epoch milliseconds (sub-millisecond precision is truncated, the type name still
//   carries the original unit);
// - integers outside of the JS safe integer range are strings;
// - floating point values that are integers are printed without a decimal part (JS makes no difference between 1.0 and 1);
// - decimal values are not normalized (they are reported as unknown by Elasticsearch anyway);
// - binary values are {"base64": "..."};
// - NaN and infinities are the strings "NaN", "Infinity" and "-Infinity".
import { DateUnit, RecordBatchReader, TimeUnit, Type } from "apache-arrow";

const TIME_UNITS = {
  [TimeUnit.SECOND]: "s",
  [TimeUnit.MILLISECOND]: "ms",
  [TimeUnit.MICROSECOND]: "us",
  [TimeUnit.NANOSECOND]: "ns",
};

function typeName(t) {
  switch (t.typeId) {
    case Type.Int:
      return `${t.isSigned ? "" : "u"}int${t.bitWidth}`;
    case Type.Float:
      return ["halffloat", "float", "double"][t.precision];
    case Type.Bool:
      return "bool";
    case Type.Null:
      return "null";
    case Type.Utf8:
      return "string";
    case Type.LargeUtf8:
      return "large_string";
    case Type.Binary:
      return "binary";
    case Type.LargeBinary:
      return "large_binary";
    case Type.FixedSizeBinary:
      return `fixed_size_binary[${t.byteWidth}]`;
    case Type.Timestamp:
      return `timestamp[${TIME_UNITS[t.unit]}${t.timezone ? `, tz=${t.timezone}` : ""}]`;
    case Type.Decimal:
      return `decimal${t.bitWidth}(${t.precision}, ${t.scale})`;
    case Type.Date:
      return t.unit === DateUnit.DAY ? "date32" : "date64";
    case Type.FixedSizeList:
      return `fixed_size_list<${typeName(t.children[0].type)}>[${t.listSize}]`;
    case Type.List:
      return `list<${typeName(t.children[0].type)}>`;
    case Type.Struct:
      return `struct<${t.children.map((f) => `${f.name}: ${typeName(f.type)}`).join(", ")}>`;
    case Type.Dictionary:
      return `dictionary<values=${typeName(t.dictionary)}, indices=${typeName(t.indices)}>`;
    default:
      return `unknown:${t}`;
  }
}

/**
 * Applies the type-dependent conventions to a (possibly nested) value. apache-arrow returns timestamps as
 * (possibly fractional) epoch milliseconds whatever their unit, and dates as epoch milliseconds.
 */
function normalize(value, type) {
  if (value === null || value === undefined) {
    return null;
  }
  switch (type.typeId) {
    case Type.Timestamp:
      return Math.floor(Number(value));
    case Type.List:
    case Type.FixedSizeList:
      return Array.from(value, (v) => normalize(v, type.children[0].type));
    case Type.Struct:
      return Object.fromEntries(type.children.map((f) => [f.name, normalize(value[f.name], f.type)]));
    case Type.Dictionary:
      return normalize(value, type.dictionary);
    default:
      return value;
  }
}

/** JSON.stringify replacer: applies the value conventions to every (possibly nested) value. */
function toJson(_key, value) {
  if (typeof value === "bigint") {
    return value >= -(2n ** 53n - 1n) && value <= 2n ** 53n - 1n ? Number(value) : value.toString();
  }
  if (typeof value === "number") {
    if (Number.isNaN(value)) return "NaN";
    if (!Number.isFinite(value)) return value > 0 ? "Infinity" : "-Infinity";
    if (!Number.isSafeInteger(value) && Number.isInteger(value)) return value.toString();
    return value;
  }
  if (value instanceof Uint8Array) {
    return { base64: Buffer.from(value).toString("base64") };
  }
  return value;
}

async function main() {
  const response = await fetch(process.env.ARROW_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/vnd.apache.arrow.stream",
    },
    body: process.env.ARROW_BODY,
  });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${await response.text()}`);
  }

  const reader = await RecordBatchReader.from(response);
  await reader.open();
  const schema = reader.schema;
  const batches = [];
  const values = schema.fields.map(() => []);
  for await (const batch of reader) {
    batches.push(batch.numRows);
    for (let i = 0; i < schema.fields.length; i++) {
      for (const v of batch.getChildAt(i)) {
        values[i].push(normalize(v, schema.fields[i].type));
      }
    }
  }

  const columns = schema.fields.map((field, i) => ({
    name: field.name,
    type: typeName(field.type),
    nullable: field.nullable,
    metadata: Object.fromEntries(field.metadata ?? []),
    values: values[i],
  }));
  process.stdout.write(JSON.stringify({ batches, columns }, toJson));
}

main().catch((e) => {
  console.error(e?.stack ?? String(e));
  process.exit(1);
});
