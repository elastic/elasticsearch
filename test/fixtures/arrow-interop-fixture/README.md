# Arrow interoperability fixture

Runs the official Apache Arrow libraries of other languages, `pyarrow` (Python) and `apache-arrow`
(Node.js), against a running Elasticsearch node, to check that the Arrow output of ES|QL
(`POST /_query?format=arrow`) can be fetched over HTTP and decoded by real-world clients, and not only by
Arrow Java.

The fixture only fetches and decodes. It does not judge what it decoded: each client prints a JSON description
of what it read, and the test that uses the fixture asserts on it. The expected values live in Java only.

The tests that use it are in `x-pack/plugin/esql/qa/server/arrow-interop`.

## How it works

```
 Java test                                 Docker container (one per call)
 ---------                                 --------------------------------
 ArrowInteropContainer.fetch(              builds the image from docker/<language>/Dockerfile
     language, url, jsonBody)  ----------> runs fetch_arrow.py / fetch_arrow.mjs, which
                                             1. POSTs ARROW_BODY to ARROW_URL
                                                (Accept: application/vnd.apache.arrow.stream)
                                             2. reads the response stream with the Arrow library
                                             3. prints a JSON description on stdout
 parses the JSON, asserts on it <--------- exits (non-zero + message on stderr if anything failed)
```

```java
String url = "http://" + ArrowInteropContainer.HOST_ADDRESS + ":" + port + "/_query?format=arrow";
String json = ArrowInteropContainer.fetch(Language.PYTHON, url, "{\"query\": \"FROM my-index | LIMIT 10\"}");
```

* `fetch` returns what the client printed on stdout. If the client fails (HTTP error, malformed stream, ...)
  it throws an `AssertionError` whose message contains the client's stderr.
* The request is made by the client itself, not by Java, so that the HTTP part (content negotiation, chunked
  streaming of the response) is also exercised from the client's side.
* The client gets its inputs from two environment variables: `ARROW_URL` and `ARROW_BODY` (the JSON request
  body, which also allows setting things like `pragma`).

### Requirements on the test

* **Docker** must be available. If not, the test is skipped through a JUnit assumption, except on CI where
  Docker is expected and its absence is a failure (see `DockerAvailability`).
* The node under test runs on the Docker host, and the container reaches it as `host.docker.internal`
  (`ArrowInteropContainer.HOST_ADDRESS`, mapped to the host with `host-gateway` so that it also works on
  Linux). The node must therefore listen on a non-loopback interface, e.g. `http.host: 0.0.0.0`, which
  test clusters don't do by default.
* Network access to Docker Hub, PyPI and the npm registry is needed to build the images, unless they are
  already in the Docker image store (see below).

### Images

The images are built locally from `src/main/resources/docker/<language>/` and are not published.

The fixture applies `elasticsearch.docker-image-cache` and overrides `CacheableTestFixture.cache()`, so that
building CI agent images builds both images and leaves them in the Docker image store. The tests rebuild the
images, which then only reuses cached layers. Changing a Dockerfile or a dependency version invalidates the
affected layers until the agent images are rebuilt, so the first CI run after such a change needs network access.

## Output of the clients

Both clients print a single JSON document on stdout, **identical for the same Arrow stream**, so that tests need
a single table of expected values for all languages:

```json
{
  "batches": [2, 1],
  "columns": [
    {
      "name": "i",
      "type": "int32",
      "nullable": true,
      "metadata": { "elastic:type": "integer" },
      "values": [1, 2, 3]
    },
    {
      "name": "ip",
      "type": "binary",
      "nullable": true,
      "metadata": { "elastic:type": "ip" },
      "values": [{ "base64": "wKgAAQ==" }, null, { "base64": "IAENuAAAAAAAAQAAAAAAAQ==" }]
    }
  ]
}
```

(`batches` is illustrative here: how a result is split in batches depends on how ES|QL splits it in pages.)

| Field                | Description |
|----------------------|-------------|
| `batches`            | Number of rows of each record batch of the stream, in order. Its length is the number of batches, and its sum is the total number of rows. An empty result still has its `columns`, with empty `values`. |
| `columns`            | One entry per field of the schema, in order. Duplicate names are possible, so this is an array and not an object. |
| `columns[].name`     | Name of the field. |
| `columns[].type`     | Arrow type of the field, see [Type names](#type-names). |
| `columns[].nullable` | Whether the field is nullable. |
| `columns[].metadata` | Custom metadata of the field (`{}` if none), as a string to string object. Includes `elastic:type`, the exact ES|QL type, and for extension types the `ARROW:extension:name` and `ARROW:extension:metadata` keys. |
| `columns[].values`   | The values of the field for all rows of all batches, concatenated. A null is `null`. |

Lists are JSON arrays, structs are JSON objects keyed by field name, and dictionary-encoded values are
decoded.

If the request or the decoding fails, nothing is printed on stdout: the client prints the error on stderr and
exits with a non-zero code.

### Normalization

The two libraries represent some things differently. To print the same JSON, the scripts normalize:

| Subject | Rule |
|---------|------|
| Type names | Always [pyarrow's names](#type-names). |
| Timestamps and dates | Epoch milliseconds (an integer), whatever the unit of the Arrow type (`date32` is converted from days). Sub-millisecond precision is rounded down to the millisecond. The type name still shows the original unit. |
| Large integers | `int64` and `uint64` values outside of `-(2^53-1)..2^53-1`, which JavaScript can't represent exactly, are strings (e.g. `"1152921504606846976"`). Others are numbers. |
| Floating point | Values that are whole numbers are printed without a decimal part (`1`, not `1.0`), as JavaScript doesn't distinguish them. `NaN`, `Infinity` and `-Infinity` are the strings `"NaN"`, `"Infinity"` and `"-Infinity"`, as JSON has no such numbers. |
| Binary | `{ "base64": "..." }`, with standard base64 and padding. |
| Decimals | Only the type name is normalized (`decimal128(p, s)`). Values are not, and ES|QL doesn't produce them. |

The normalization is implemented twice, in `fetch_arrow.py` and `fetch_arrow.mjs`, with the same list of rules
in the header of each. **When changing one, change the other**, and check that they still print the same thing.

### Type names

Types are named like pyarrow's string representation, for the types that Elasticsearch produces:

| Arrow type | Name |
|------------|------|
| signed / unsigned integers | `int8` ... `int64`, `uint8` ... `uint64` |
| floating point | `halffloat`, `float`, `double` |
| boolean | `bool` |
| null | `null` |
| UTF-8 string | `string`, `large_string` |
| binary | `binary`, `large_binary`, `fixed_size_binary[<width>]` |
| timestamp | `timestamp[s]`, `timestamp[ms]`, `timestamp[us]`, `timestamp[ns]`, with `, tz=<zone>` inside the brackets if the type has a time zone |
| dates | `date32`, `date64` |
| decimal | `decimal128(<precision>, <scale>)`, `decimal256(...)` |
| list | `list<<item type>>`, e.g. `list<string>` |
| fixed size list | `fixed_size_list<<item type>>[<size>]` |
| struct | `struct<<name>: <type>, ...>`, e.g. `struct<min: double, max: double>` |
| dictionary | `dictionary<values=<type>, indices=<type>>` |
| anything else | `unknown:<name given by the library>`. The two libraries give different names, so a test sees these types and can decide what to do with them. |

Extension types (such as `geoarrow.wkb`) are not interpreted: libraries that don't know them read the storage
type (e.g. `binary`), and the extension is visible in the field's `metadata`.

## Adding a language

1. Add `docker/<language>/` with a `Dockerfile` and a script that follows the contract above.
2. Add the language to `ArrowInteropContainer.Language`, with the files of its Docker context.
3. Check that the new client prints the same JSON as the existing ones for the same query. The test module runs
   every scenario with every language, so no other change is needed there.
