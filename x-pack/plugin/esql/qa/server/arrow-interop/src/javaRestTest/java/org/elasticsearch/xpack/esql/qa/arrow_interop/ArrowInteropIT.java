/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.arrow_interop;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.fixtures.arrowinterop.ArrowInteropContainer;
import org.elasticsearch.test.fixtures.arrowinterop.ArrowInteropContainer.Language;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Checks that the official Arrow libraries of other languages ({@code pyarrow} and {@code apache-arrow} for Node.js)
 * can fetch and decode ES|QL Arrow responses over HTTP.
 * <p>
 * The clients run in Docker containers (see {@link ArrowInteropContainer}), which print a normalized JSON description of
 * what they decoded. As both languages print the same JSON, a single table of expectations is used for all of them.
 * <p>
 * Test data and expectations are in {@code src/javaRestTest/resources/arrow-interop}:
 * <ul>
 * <li>{@code mapping.json} and {@code bulk.ndjson} define the {@value #INDEX} index that most tests query. It has one
 * column per type under test, and three documents: a first one with values, a second one with only nulls, and a last one
 * with other values;</li>
 * <li>{@code expected/<scenario>.json} is the array of columns that clients must decode for a scenario.</li>
 * </ul>
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, TestContainersThreadFilter.class })
public class ArrowInteropIT extends ESRestTestCase {

    private static final String INDEX = "arrow-interop";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        // The Arrow clients run in Docker containers, which can't reach a node that only listens on loopback.
        // This is acceptable as test nodes run on ephemeral hosts.
        .setting("http.host", "0.0.0.0")
        .build();

    /**
     * The Arrow clients are started once for all tests, as each test then only runs a client in its container.
     * The tests are skipped if Docker is not available.
     */
    @ClassRule
    public static ArrowInteropContainer pythonClient = new ArrowInteropContainer(Language.PYTHON);

    @ClassRule
    public static ArrowInteropContainer nodeClient = new ArrowInteropContainer(Language.NODE);

    private static List<ArrowInteropContainer> clients() {
        return List.of(pythonClient, nodeClient);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void initIndex() throws IOException {
        Request request = new Request("PUT", "/" + INDEX);
        request.setJsonEntity(resource("mapping.json"));
        assertOK(client().performRequest(request));

        request = new Request("POST", "/_bulk?index=" + INDEX + "&refresh=true");
        request.setJsonEntity(resource("bulk.ndjson"));
        assertBulkSuccess(client().performRequest(request));
    }

    public void testBasicTypes() throws IOException {
        assertColumns("FROM arrow-interop | SORT i | KEEP i, l, d, k, b, t, mv", "basic_types");
    }

    /** IPv4 addresses are 4 bytes, IPv6 ones are 16 bytes. */
    public void testIp() throws IOException {
        assertColumns("FROM arrow-interop | SORT i | KEEP ip", "ip");
    }

    /** Versions are binary-encoded in ES|QL, but are turned into strings in the Arrow output. */
    public void testVersion() throws IOException {
        assertColumns("FROM arrow-interop | SORT i | KEEP v", "version");
    }

    /**
     * T-Digests are the only histogram type with an Arrow representation (others are not implemented yet).
     * A struct is null for a missing digest, and an empty digest is a struct with empty lists and null min/max/sum.
     */
    public void testTDigest() throws IOException {
        assertColumns("FROM arrow-interop | SORT i | KEEP td", "tdigest");
    }

    /** A struct of nullable metrics, which is null for a missing summary. */
    public void testAggregateMetricDouble() throws IOException {
        assertColumns("FROM arrow-interop | SORT i | KEEP m", "aggregate_metric_double");
    }

    /**
     * Geometries are WKB binary, tagged as GeoArrow through the field metadata (what Arrow calls an extension type, which
     * libraries that don't know it read as the underlying binary type). Only geographic types have a CRS.
     * Coordinates are chosen so that they are exactly represented by the encoding of the geo types.
     */
    public void testGeo() throws IOException {
        assertColumns("FROM arrow-interop | SORT i | KEEP gp, gs, cp, cs", "geo");
    }

    /** Unsigned longs are unsigned 64-bit integers in Arrow, nanosecond dates keep their unit, and text is a string. */
    public void testNumericAndTextTypes() throws IOException {
        assertColumns("FROM arrow-interop | SORT i | KEEP big, ul, dn, tx", "numeric_and_text");
    }

    /** Dense vectors are lists of {@code float32}, with no sign of the {@code dims} in the Arrow type. */
    public void testDenseVector() throws IOException {
        assertColumns("FROM arrow-interop | SORT i | KEEP dv", "dense_vector");
    }

    /** Classic histograms are structs of parallel values and counts lists. */
    public void testHistogram() throws IOException {
        assertColumns("FROM arrow-interop | SORT i | KEEP hi", "histogram");
    }

    /** Exponential histograms are structs with summary values, a zero bucket and positive and negative buckets. */
    public void testExponentialHistogram() throws IOException {
        assertColumns("FROM arrow-interop | SORT i | KEEP eh", "exponential_histogram");
    }

    /** {@code _source} and {@code flattened} are JSON text, tagged with the {@code arrow.json} extension type. */
    public void testJson() throws IOException {
        assertColumns("FROM arrow-interop METADATA _source | SORT i | KEEP fl, _source", "json");
    }

    /** Ranges are structs with a {@code from} and a {@code to} field. */
    public void testRanges() throws IOException {
        assertColumns("FROM arrow-interop | SORT i | KEEP dr, dbr", "ranges");
    }

    /** Geohash and geohex are integers, and geotile is a struct of its zoom, x and y components. */
    public void testGeoGrids() throws IOException {
        assertColumns(
            "FROM arrow-interop | SORT i | EVAL gh = ST_GEOHASH(gp, 4), gx = ST_GEOHEX(gp, 3), gt = ST_GEOTILE(gp, 3) | KEEP gh, gx, gt",
            "geo_grids"
        );
    }

    /** Fields that ES|QL can't handle are reported as the {@code null} type, as is a literal {@code null}. */
    public void testNullAndUnsupported() throws IOException {
        assertColumns("FROM arrow-interop | SORT i | KEEP bin | EVAL n = null", "null_and_unsupported");
    }

    /** Counters have the type of their underlying number, and time series IDs are binary. */
    public void testTimeSeries() throws IOException {
        Request request = new Request("PUT", "/arrow-interop-ts");
        request.setJsonEntity(resource("time_series_mapping.json"));
        assertOK(client().performRequest(request));

        request = new Request("POST", "/_bulk?index=arrow-interop-ts&refresh=true");
        request.setJsonEntity(resource("time_series_bulk.ndjson"));
        assertBulkSuccess(client().performRequest(request));

        // The _tsid is a hash that is an implementation detail: only check that it is present
        assertColumns("TS arrow-interop-ts METADATA _tsid | SORT host | KEEP ci, cl, cd, _tsid", "time_series", Set.of("_tsid"));
    }

    /** Every other test asks for Arrow with {@code format=arrow}, which has precedence: this one only has the {@code Accept} header. */
    public void testAcceptHeaderNegotiation() throws IOException {
        List<?> expected = (List<?>) parseJson("{\"columns\":" + resource("expected/ip.json") + "}").get("columns");
        for (ArrowInteropContainer client : clients()) {
            Map<String, Object> result = fetch(client, "FROM arrow-interop | SORT i | KEEP ip", Map.of(), false);
            assertEquals("Columns decoded by " + client.language(), expected, result.get("columns"));
        }
    }

    /** The schema is sent even if there are no rows. */
    public void testEmptyResult() throws IOException {
        assertColumns("FROM arrow-interop | WHERE i > 100 | KEEP i", "empty_result");
    }

    /**
     * A result that is larger than one ES|QL page is streamed as several Arrow record batches, that the clients
     * must read to the end. A small page size makes the result span several pages without needing a lot of documents.
     */
    public void testMultipleBatches() throws IOException {
        int docCount = 5_000;
        int pageSize = 1_000;
        indexNumbers("arrow-interop-big", docCount);

        for (ArrowInteropContainer client : clients()) {
            String language = client.language().toString();
            Map<String, Object> result = fetch(
                client,
                "FROM arrow-interop-big | KEEP n | LIMIT " + docCount,
                Map.of("pragma", Map.of("page_size", pageSize), "accept_pragma_risks", true)
            );
            List<?> batches = (List<?>) result.get("batches");
            assertThat("Batches decoded by " + language, batches.size(), greaterThanOrEqualTo(docCount / pageSize));
            assertEquals("Rows in batches decoded by " + language, docCount, batches.stream().mapToInt(b -> (Integer) b).sum());

            // Rows are not sorted, check that every value is there exactly once
            List<?> columns = (List<?>) result.get("columns");
            List<?> values = (List<?>) ((Map<?, ?>) columns.get(0)).get("values");
            assertEquals("Values decoded by " + language, IntStream.range(0, docCount).boxed().collect(toSet()), Set.copyOf(values));
            assertEquals("Values decoded by " + language, docCount, values.size());
        }
    }

    /** A bulk request that has failing items still returns HTTP 200, which would silently drop test documents. */
    private static void assertBulkSuccess(Response response) throws IOException {
        assertOK(response);
        Map<String, Object> body = entityAsMap(response);
        assertEquals(body.toString(), false, body.get("errors"));
    }

    /** Creates an index with an integer field {@code n} that has the values {@code 0..count-1}. */
    private void indexNumbers(String index, int count) throws IOException {
        Request request = new Request("PUT", "/" + index);
        request.setJsonEntity("""
            { "mappings": { "properties": { "n": { "type": "integer" } } } }""");
        assertOK(client().performRequest(request));

        StringBuilder bulk = new StringBuilder();
        for (int n = 0; n < count; n++) {
            bulk.append("{\"index\": {}}\n{\"n\": ").append(n).append("}\n");
        }
        request = new Request("POST", "/_bulk?index=" + index + "&refresh=true");
        request.setJsonEntity(bulk.toString());
        assertBulkSuccess(client().performRequest(request));
    }

    /**
     * Runs the query with every Arrow client, and checks that the columns they decoded are the expected ones.
     *
     * @param expectedColumns name of the resource {@code expected/<name>.json}, a JSON array of the expected columns
     *                        in the format printed by the clients
     */
    private void assertColumns(String query, String expectedColumns) throws IOException {
        assertColumns(query, expectedColumns, Set.of());
    }

    /**
     * Same, where the values of the columns named in {@code presenceOnly} are not compared. Instead, the column
     * is compared with a list of booleans that tell for each row if it has a value.
     */
    private void assertColumns(String query, String expectedColumns, Set<String> presenceOnly) throws IOException {
        // Parsed the same way as the clients' output, so that numbers have the same Java types
        List<?> expected = (List<?>) parseJson("{\"columns\":" + resource("expected/" + expectedColumns + ".json") + "}").get("columns");
        for (ArrowInteropContainer client : clients()) {
            List<?> actual = (List<?>) fetch(client, query).get("columns");
            actual = actual.stream().map(c -> presenceOnly(c, presenceOnly)).toList();
            assertEquals("Columns decoded by " + client.language(), expected, actual);
        }
    }

    private static Object presenceOnly(Object column, Set<String> names) {
        Map<?, ?> map = (Map<?, ?>) column;
        if (names.contains(map.get("name")) == false) {
            return column;
        }
        Map<Object, Object> copy = new LinkedHashMap<>(map);
        copy.put("values", ((List<?>) map.get("values")).stream().map(Objects::nonNull).toList());
        return copy;
    }

    /** Runs the query with the given Arrow client, and returns the JSON it printed. */
    private Map<String, Object> fetch(ArrowInteropContainer client, String query) throws IOException {
        return fetch(client, query, Map.of());
    }

    /** Same, with additional fields in the request body. */
    private Map<String, Object> fetch(ArrowInteropContainer client, String query, Map<String, Object> extraBody) throws IOException {
        return fetch(client, query, extraBody, true);
    }

    /** Same, optionally leaving the {@code format} parameter out so that the format is chosen from the {@code Accept} header only. */
    private Map<String, Object> fetch(ArrowInteropContainer client, String query, Map<String, Object> extraBody, boolean formatParam)
        throws IOException {
        String url = "http://" + ArrowInteropContainer.HOST_ADDRESS + ":" + httpPort() + "/_query" + (formatParam ? "?format=arrow" : "");
        String body = Strings.toString(
            JsonXContent.contentBuilder().startObject().field("query", query).mapContents(extraBody).endObject()
        );
        return parseJson(client.fetch(url, body));
    }

    private int httpPort() {
        String address = cluster.getHttpAddresses().split(",")[0];
        return Integer.parseInt(address.substring(address.lastIndexOf(':') + 1));
    }

    private static Map<String, Object> parseJson(String json) throws IOException {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);
    }

    private static String resource(String name) throws IOException {
        try (InputStream in = ArrowInteropIT.class.getResourceAsStream("/arrow-interop/" + name)) {
            assertNotNull("Missing resource " + name, in);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
