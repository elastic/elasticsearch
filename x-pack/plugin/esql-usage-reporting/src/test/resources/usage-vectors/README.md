# Usage record test vectors

Each file is the body of one batch request to the Usage API (`POST /api/v1/usage`) for the records that
`HostedUsageRecordBuilder` builds from the fixed inputs below. `UsageRecordJsonTests` builds the records from those inputs and
compares the JSON with these files, as data (field order and whitespace do not matter). Another implementation of the same records,
such as the Serverless plugin's, can load the files and check that it produces the same JSON from the same inputs.

Common inputs:

| Input | Value |
|---|---|
| node id | `node-1` |
| deployment id | `deployment-1` |
| cluster id | `cluster-1` |
| provider, region | `aws`, `eu-west-1` |
| drain time | `2026-11-23T10:05:00.123Z` |
| creation time | `2026-11-23T10:05:00.456Z` |
| period | 300 seconds |

Rules the files show:

- `quantity` is milliseconds: the sum of the four components in nanoseconds divided by 1,000,000, the remainder dropped.
- `usage.metadata` holds the components that are not zero, in nanoseconds, as strings.
- `id` is `es-<node id>_<usage type>_<drain time>`, with `_<reason>` appended for exempt records; timestamps always have three
  fractional digits.
- Billed records have no `usage.exemption`; the other outcomes are exempt records of their own and are never summed into a billed one.
- `source.metadata.cluster_id` is required for hosted records: billing drops them without it.

| File | Outcome and components (nanoseconds) | What it shows |
|---|---|---|
| `billed.json` | success, execution 2,500,999,999 | the remainder below one millisecond is dropped (2500, not 2501); only `cpu_nanos` in the metadata |
| `exempt-failed.json` | failed, execution 3,000,000,000 | `usage.exemption` with reason `failed`; id suffix `_failed` |
| `exempt-cancelled.json` | cancelled, planning 1,500,000 | reason `cancelled`; a component other than execution; 1.5 ms becomes 1 |
| `multi-component.json` | success, execution 3,000,000,000, read 4,000,000, planning 1,000,000, split discovery 2,000,000 | the quantity is the sum of all four components (3007); all four metadata keys |
