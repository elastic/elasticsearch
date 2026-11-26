/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.Before;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;

/**
 * Heap-attack coverage for {@code SET unmapped_fields="LOAD_ALL"}, which returns every {@code _source} leaf of a result row in a
 * single {@code _unmapped_fields} column holding one flattened JSON object.
 * <p>
 * {@link #testManyDocsWithManyDistinctFields} and {@link #testFewDocsWithHugeNumberOfDistinctFields} attack the response shape:
 * each indexes millions of distinct unmapped leaves - far more names than fit in the heap if each were a column - and expects the
 * response to stay a single {@code _unmapped_fields} column next to the mapped one, uncapped and without a warning. They differ only
 * in where the distinct names come from: spread thinly across many moderately sized documents, or packed into a handful of huge ones.
 * <p>
 * {@link #testArrayOfTinyObjectsCircuitBreaksInSourceLoading} attacks a different limit: a single structure-heavy {@code _source} that
 * materialises into an object graph many times its own size, which must circuit break gracefully rather than OOM.
 * <p>
 * Each index maps a single {@link #MAPPED_FIELD}, so the queries exercise {@code LOAD_ALL} discovery rather than whatever an index with no
 * mapped fields at all would plan to.
 */
public class HeapAttackUnmappedLoadAllIT extends HeapAttackTestCase {
    private static final String MAPPED_FIELD = "id";

    /**
     * The request breaker of the tests that lower it, as a size and not a percentage of the heap: a percentage grows with the heap of
     * the cluster under test, and the data of these tests does not. It is 15 percent of the 512MB heap of this suite.
     */
    private static final String LOWERED_REQUEST_BREAKER = "80mb";

    /**
     * The request breaker of {@link #testConcurrentWideQueriesCircuitBreakAndEveryNodeStillAnswers}. A query over the wide documents of
     * that test builds about 110MB of rows and passes alone, so two of them at once on a node need more than this.
     */
    private static final String CONCURRENT_REQUEST_BREAKER = "200mb";

    @Before
    public void requireLoadAll() {
        assumeTrue("requires the LOAD_ALL single column", EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL_SINGLE_SOURCE_COLUMN.isEnabled());
    }

    /**
     * Index:
     * <ul>
     *     <li>5,000 documents with 1,000 top-level unmapped leaves each</li>
     *     <li>every leaf name is distinct across the index: 5M names, ~70MB of {@code _source}</li>
     * </ul>
     * Query:
     * <ul>
     *     <li>{@code LOAD_ALL} over every document</li>
     * </ul>
     * Expected: the columns {@code id} and {@code _unmapped_fields}, and no warning
     */
    public void testManyDocsWithManyDistinctFields() throws IOException {
        String index = "load_all_many_docs";
        int docs = 5_000;
        indexManyDocsWithManyDistinctFields(index, docs, 1_000);

        assertLoadAllSingleColumn(index, docs);
    }

    /** {@code docs} documents with {@code fieldsPerDoc} unmapped leaves each, every name distinct across the index. */
    private void indexManyDocsWithManyDistinctFields(String index, int docs, int fieldsPerDoc) throws IOException {
        createMostlyUnmappedIndex(index);
        StringBuilder bulk = new StringBuilder();
        int docsPerBulk = 500;
        for (int d = 0; d < docs; d++) {
            bulk.append("{\"create\":{}}\n{\"").append(MAPPED_FIELD).append("\":\"").append(d).append('"');
            for (int f = 0; f < fieldsPerDoc; f++) {
                bulk.append(",\"").append(manyDocsFieldName(d * fieldsPerDoc + f)).append("\":1");
            }
            bulk.append("}\n");
            if (d % docsPerBulk == docsPerBulk - 1) {
                bulk(index, bulk.toString());
                bulk.setLength(0);
            }
        }
        // Sends whatever is left over (nothing, if docs is a multiple of docsPerBulk), then force-merges and refreshes.
        initIndex(index, bulk.toString());
    }

    /**
     * Index:
     * <ul>
     *     <li>5 documents with 1M unmapped leaves each, nested 1,000 to an object: {@code {"d0_g000": {"f000": 1, ...}, ...}}</li>
     *     <li>every leaf name is distinct across the index: 5M names, ~9MB of {@code _source} per document</li>
     * </ul>
     * Query:
     * <ul>
     *     <li>{@code LOAD_ALL} over every document</li>
     * </ul>
     * Expected: the columns {@code id} and {@code _unmapped_fields}, and no warning
     * <p>
     * Serverless has a bigger heap but 6 shards, so documents are read concurrently; the block loader reserves a multiple of a
     * document's {@code _source} while reading it, and 9MB documents then trip the request breaker long before the field names are an
     * issue. There the same 5M names are spread over ten times as many, ten times smaller documents: still 100k fields in each.
     */
    public void testFewDocsWithHugeNumberOfDistinctFields() throws IOException {
        String index = "load_all_huge_docs";
        boolean serverless = isServerless();
        int docs = 50;
        int objectsPerDoc = 1_000;
        int leavesPerObject = 100;
        createMostlyUnmappedIndex(index);
        for (int d = 0; d < docs; d++) {
            StringBuilder bulk = new StringBuilder();
            bulk.append("{\"create\":{}}\n{\"").append(MAPPED_FIELD).append("\":\"").append(d).append('"');
            for (int o = 0; o < objectsPerDoc; o++) {
                bulk.append(",\"").append(hugeDocsObjectName(d, o)).append("\":{");
                for (int l = 0; l < leavesPerObject; l++) {
                    if (l > 0) {
                        bulk.append(',');
                    }
                    bulk.append('"').append(hugeDocsLeafName(l)).append("\":1");
                }
                bulk.append('}');
            }
            bulk.append("}\n");
            // One document per bulk keeps each request well under the indexing pressure limit.
            bulk(index, bulk.toString());
        }
        // Everything is indexed already; this only force-merges and refreshes.
        initIndex(index, "");

        assertLoadAllSingleColumn(index, docs);
    }

    /**
     * A single document whose {@code _source} is one array of many tiny objects ({@code "a":[{"x":1},{"x":1},...]}): a few bytes
     * per element, but parsed into a {@code Map}/{@code List} graph it takes many times its own size while flattening to a single
     * {@code a.x} leaf - so it is the parse that grows, not the output. This
     * must trip a graceful {@code circuit_breaking_exception} rather than exhaust the heap.
     * <p>
     * Before reading {@code _source} the data node reserves {@code SOURCE_RESERVATION_FACTOR} (10x) its length on the request
     * breaker (in {@code UnmappedFields#blockLoader}). That reservation is proportional to the {@code _source} length, so on this
     * small 512MB-heap node a document big enough to cross the default breaker can no longer be indexed. The request breaker is
     * lowered instead, standing in for the headroom a normally provisioned node has: the 10x reservation on a small, easily indexed
     * document then crosses it and breaks up front, before {@code _source} is read. The base class resets cluster settings after
     * the test.
     */
    public void testArrayOfTinyObjectsCircuitBreaksInSourceLoading() throws IOException {
        setRequestBreakerLimit("15%");
        assertCircuitBreaksVia(attempt -> {
            // ~1.5M+ {"x":1} elements is ~12MB+ of _source; the 10x reservation (~120MB+) then crosses the 15% breaker (~80MB).
            int elements = 1_500_000 * attempt;
            String index = "load_all_array_" + attempt;
            createMostlyUnmappedIndex(index);
            StringBuilder doc = new StringBuilder(elements * 8 + 64);
            doc.append("{\"create\":{}}\n{\"").append(MAPPED_FIELD).append("\":\"0\",\"a\":[");
            for (int e = 0; e < elements; e++) {
                if (e > 0) {
                    doc.append(',');
                }
                doc.append("{\"x\":1}");
            }
            doc.append("]}\n");
            bulk(index, doc.toString());
            initIndex(index, "");

            StringBuilder query = startQuery();
            query.append("SET unmapped_fields=\\\"LOAD_ALL\\\";\n");
            query.append("FROM ").append(index).append("\n| LIMIT 1\"}");
            return responseAsMap(query(query.toString(), "columns"));
        }, "UnmappedFields$AllFieldsBlockLoader");
    }

    /** The circuit break of the values held while an array is read, not that of the reservation made before. */
    private static final MapMatcher HELD_VALUES_BREAK = circuitBreakOn("unmapped fields held values");

    /** The circuit break of the JSON that the read builds, not that of the reservation made before or of the values it holds. */
    private static final MapMatcher WRITTEN_JSON_BREAK = circuitBreakOn("unmapped fields written json");

    private static MapMatcher circuitBreakOn(String label) {
        return matchesMap().entry("status", 429)
            .entry(
                "error",
                matchesMap().extraOk().entry("type", "circuit_breaking_exception").entry("reason", containsString("[" + label + "]"))
            );
    }

    /**
     * Index:
     * <ul>
     *     <li>One document with an array of 7M one-character strings, indexed as CBOR: 14MB of {@code _source}</li>
     * </ul>
     * Query:
     * <ul>
     *     <li>{@code LOAD_ALL} over every document</li>
     * </ul>
     * Expected: circuit break while the values of the array are held
     * <p>
     * An element of 2 bytes of CBOR {@code _source} is held as a {@code String} that takes about 50, so what is held is more than the
     * reservation of 10 times the size of {@code _source} stands for. On the 512MB heap of this suite an array of this many strings,
     * with the request breaker at its default limit, killed the node before the held values were charged. The cluster allows the
     * document to be indexed, see {@link Clusters}, and CBOR keeps it small enough to be indexed on a larger heap as well, see
     * {@link #indexCborDocument}. The document and its reservation fit under the breaker, and what is held does not.
     */
    public void testArrayOfShortStringsCircuitBreaksWhileItsValuesAreHeld() throws IOException {
        String index = "load_all_short_strings";
        indexDocumentWithArrays(index, scaleToHeap(7_000_000), false);
        assertCircuitBreaks(attempt -> loadAllOneRow(index), HELD_VALUES_BREAK);
    }

    /**
     * Index:
     * <ul>
     *     <li>One document with {@code "a.b"} and {@code "a": {"b": ...}} each an array of 3M one-character strings, indexed as CBOR:
     *     12MB of {@code _source}</li>
     * </ul>
     * Query:
     * <ul>
     *     <li>{@code LOAD_ALL} over every document</li>
     * </ul>
     * Expected: circuit break while the values are held
     * <p>
     * A document that spells a field in two ways is read again with all of its values held, not just those of an array.
     */
    public void testFieldSpelledInTwoWaysCircuitBreaksWhileItsValuesAreHeld() throws IOException {
        String index = "load_all_two_spellings";
        indexDocumentWithArrays(index, scaleToHeap(3_000_000), true);
        assertCircuitBreaks(attempt -> loadAllOneRow(index), HELD_VALUES_BREAK);
    }

    /**
     * Index:
     * <ul>
     *     <li>One document with a key of 10,000 characters above 100,000 short numbers, indexed as CBOR: under a megabyte of
     *     {@code _source}</li>
     * </ul>
     * Query:
     * <ul>
     *     <li>{@code LOAD_ALL} over every document</li>
     * </ul>
     * Expected: circuit break while the JSON of {@code _unmapped_fields} is built
     * <p>
     * That JSON spells each leaf as its full path, so it takes the number of leaves times the length of the key above them: about a
     * gigabyte here, whatever the size of {@code _source}. It is built in a buffer that the reservation of 10 times the size of
     * {@code _source} cannot cover, and that once killed the node. The document is indexed with {@code dynamic: false}, which keeps the
     * limits on the number and the length of field names from rejecting it.
     */
    public void testLongKeyAboveManyLeavesCircuitBreaksWhileTheJsonIsBuilt() throws IOException {
        String index = "load_all_long_key";
        createMostlyUnmappedIndex(index);
        int leaves = scaleToHeap(100_000);
        try (XContentBuilder doc = XContentFactory.cborBuilder()) {
            doc.startObject().field(MAPPED_FIELD, "0");
            doc.startObject("p".repeat(10_000));
            for (int leaf = 0; leaf < leaves; leaf++) {
                doc.field("k" + leaf, 0);
            }
            doc.endObject().endObject();
            indexCborDocument(index, doc);
        }
        initIndex(index, "");
        assertCircuitBreaks(attempt -> loadAllOneRow(index), WRITTEN_JSON_BREAK);
    }

    /**
     * Index:
     * <ul>
     *     <li>60,000 documents with 100 unmapped leaves each, every name distinct across the index: about 2KB of JSON per row</li>
     * </ul>
     * Query:
     * <ul>
     *     <li>{@code LOAD_ALL}, {@code DEDUP}, then the last row by the mapped field, which needs every row to have passed</li>
     * </ul>
     * Expected: circuit break while the keys of the groups are held
     * <p>
     * Since {@code DEDUP} compares {@code _unmapped_fields}, the JSON of every distinct row is a key that the grouping keeps until the
     * end, and no row is a duplicate here. The documents are narrow, so that one page of them is far under the breaker and what
     * crosses it is the keys that add up over many pages. The request breaker is lowered to a fixed size, see
     * {@link #LOWERED_REQUEST_BREAKER}, to stand in for the headroom of a larger heap. A retry appends another 60,000 documents to the
     * same index.
     * <p>
     * The query cannot end with {@code STATS}: a command that does not return the unmapped fields removes {@code _unmapped_fields} from
     * the plan, and then nothing carries the JSON. A {@code LIMIT} alone lets the query finish after the first rows. The last row by
     * the mapped field needs every row, and loads the JSON only for the one that it returns.
     */
    public void testDedupCircuitBreaksWhileTheJsonOfEveryGroupIsHeld() throws IOException {
        setRequestBreakerLimit(LOWERED_REQUEST_BREAKER);
        String index = "load_all_dedup";
        createMostlyUnmappedIndex(index);
        assertCircuitBreaksVia(attempt -> {
            // A failure of the indexing is not a pass: only a breaker response to the query is.
            setUp(() -> indexDistinctNarrowRows(index, 60_000 * (attempt - 1), 60_000 * attempt));

            StringBuilder query = startQuery();
            query.append("SET unmapped_fields=\\\"LOAD_ALL\\\";\n");
            query.append("FROM ").append(index).append("\n| DEDUP\n| SORT ").append(MAPPED_FIELD).append(" DESC\n| LIMIT 1\"}");
            return responseAsMap(query(query.toString(), "columns"));
        }, "GroupedLimitOperator");
    }

    /** Documents {@code from} up to {@code to}, with 100 unmapped leaves each and no name used twice in the index. */
    private void indexDistinctNarrowRows(String index, int from, int to) throws IOException {
        StringBuilder bulk = new StringBuilder();
        int docsPerBulk = 500;
        for (int d = from; d < to; d++) {
            bulk.append("{\"create\":{}}\n{\"").append(MAPPED_FIELD).append("\":\"").append(String.format(Locale.ROOT, "%08d", d));
            bulk.append('"');
            for (int f = 0; f < 100; f++) {
                bulk.append(",\"").append(manyDocsFieldName(d * 100 + f)).append("\":1");
            }
            bulk.append("}\n");
            if (d % docsPerBulk == docsPerBulk - 1) {
                bulk(index, bulk.toString());
                bulk.setLength(0);
            }
        }
        // Sends whatever is left over, then force-merges and refreshes.
        initIndex(index, bulk.toString());
    }

    /**
     * Runs the indexing of a test that expects a circuit break from a query. A circuit break or any other error response of the
     * indexing fails the test, and does not pass it as a break of the query would.
     */
    private static void setUp(SetUp setUp) throws IOException {
        try {
            setUp.run();
        } catch (ResponseException e) {
            throw new AssertionError("the indexing failed, which is not the circuit break that the test expects of the query", e);
        }
    }

    private interface SetUp {
        void run() throws IOException;
    }

    /**
     * Index:
     * <ul>
     *     <li>One document with 1,000 unmapped leaves, about 12KB of JSON, and a mapped {@code tags} field of 20,000 values or more:
     *     a few hundred KB of {@code _source}</li>
     * </ul>
     * Query:
     * <ul>
     *     <li>{@code LOAD_ALL}, {@code MV_EXPAND tags}, {@code DEDUP}, then the last row by {@code tags}</li>
     * </ul>
     * Expected: circuit break while the JSON of every expanded row is held
     * <p>
     * {@code MV_EXPAND} makes one small document as many rows as there are values. The plan loads {@code _unmapped_fields} after the
     * expansion, because {@code DEDUP} needs it, so the 12KB of JSON is read for each of the rows: 240MB with the first size, far more
     * than the {@code _source} that a read reserves for. {@code DEDUP} then holds a key for each row, as in
     * {@link #testDedupCircuitBreaksWhileTheJsonOfEveryGroupIsHeld}. This tests the loading and the grouping after an expansion. It does
     * not test {@code MV_EXPAND} copying a block that is already loaded: the planner does not produce that for this query.
     * Where it breaks is not the point either: the next allocation to cross the limit is the one that fails. The request breaker is
     * lowered to a fixed size, see {@link #LOWERED_REQUEST_BREAKER}.
     * <p>
     * Without the {@code DEDUP} the query does not break: the JSON is then loaded only for the one row that comes back, so nothing holds
     * it.
     */
    public void testMvExpandThenDedupCircuitBreaksWhileTheJsonOfEveryRowIsHeld() throws IOException {
        setRequestBreakerLimit(LOWERED_REQUEST_BREAKER);
        assertCircuitBreaks(attempt -> {
            String index = "load_all_mv_expand_" + attempt;
            int tags = 20_000 * attempt;
            // A failure of the indexing is not a pass: only a breaker response to the query is.
            setUp(() -> {
                CreateIndexResponse response = createIndex(index, Settings.EMPTY, """
                    {
                      "dynamic": false,
                      "properties": {
                        "%s": { "type": "keyword" },
                        "tags": { "type": "keyword" }
                      }
                    }""".formatted(MAPPED_FIELD));
                assertTrue(response.isAcknowledged());
                StringBuilder doc = new StringBuilder();
                doc.append("{\"create\":{}}\n{\"").append(MAPPED_FIELD).append("\":\"0\",\"tags\":[");
                for (int t = 0; t < tags; t++) {
                    if (t > 0) {
                        doc.append(',');
                    }
                    doc.append("\"t").append(t).append('"');
                }
                doc.append(']');
                for (int f = 0; f < 1_000; f++) {
                    doc.append(",\"").append(manyDocsFieldName(f)).append("\":1");
                }
                doc.append("}\n");
                bulk(index, doc.toString());
                initIndex(index, "");
            });

            StringBuilder query = startQuery();
            query.append("SET unmapped_fields=\\\"LOAD_ALL\\\";\n");
            query.append("FROM ").append(index).append("\n| MV_EXPAND tags\n| DEDUP\n| SORT tags DESC\n| LIMIT 1\"}");
            return responseAsMap(query(query.toString(), "columns"));
        });
    }

    /**
     * Index:
     * <ul>
     *     <li>The 5,000 documents with 1,000 unmapped leaves each of {@link #testManyDocsWithManyDistinctFields}: a query over all of
     *     them passes alone, also with the request breaker at {@link #CONCURRENT_REQUEST_BREAKER}</li>
     * </ul>
     * Query:
     * <ul>
     *     <li>24 clients at once, each {@code LOAD_ALL} over every document, for up to three rounds, with each client sending to one
     *     node only</li>
     * </ul>
     * Expected: every query either succeeds or circuit breaks, some break, and every node answers a query afterwards
     * <p>
     * The request breaker is shared by the queries of a node, and each of them builds about 110MB of rows, so two of them at once on a
     * node need more than {@link #CONCURRENT_REQUEST_BREAKER}. Each node gets the queries of twelve clients, and the client of the
     * suite allows ten connections to a node, so at least ten queries are in flight on each of them. The breaker has to reject some, with
     * a 429, and nothing else may go wrong: no other status, no failed connection, and no node that stops answering. Each client talks
     * to one node because the shared client retries on the other node after a dropped connection or a 502, 503 or 504, which would let a
     * query that lost its node count as a success. Those clients do not carry the project of a multi-project run, so the test is skipped
     * there. With 8 clients and the default breaker, nothing broke.
     */
    public void testConcurrentWideQueriesCircuitBreakAndEveryNodeStillAnswers() throws Exception {
        assumeTrue(
            "the clients that are pinned to one node do not carry the project of a multi-project run",
            Booleans.parseBoolean(System.getProperty("tests.multi_project.enabled", "false")) == false
        );
        String index = "load_all_concurrent";
        int docs = 5_000;
        int clients = 24;
        indexManyDocsWithManyDistinctFields(index, docs, 1_000);
        setRequestBreakerLimit(CONCURRENT_REQUEST_BREAKER);
        assertLoadAllSingleColumn(index, docs);

        try (NodeClients clientsOfNodes = new NodeClients()) {
            List<RestClient> nodes = clientsOfNodes.clients;
            for (HttpHost host : getClusterHosts()) {
                nodes.add(buildClient(restClientSettings(), new HttpHost[] { host }));
            }
            boolean broke = false;
            for (int round = 0; round < 3 && broke == false; round++) {
                ExecutorService clientsPool = Executors.newFixedThreadPool(clients);
                AtomicBoolean abandoned = new AtomicBoolean();
                Throwable failure = null;
                try {
                    CountDownLatch go = new CountDownLatch(1);
                    List<Future<Boolean>> outcomes = new ArrayList<>(clients);
                    boolean allSubmitted = false;
                    try {
                        for (int c = 0; c < clients; c++) {
                            RestClient node = nodes.get(c % nodes.size());
                            outcomes.add(clientsPool.submit(() -> {
                                go.await();
                                // A worker that was still at the gate when another could not be submitted does not start its query.
                                return abandoned.get() == false && loadAllBrokeTheCircuit(node, index, docs);
                            }));
                        }
                        allSubmitted = true;
                    } finally {
                        // Whatever stopped the submission, also an Error, the workers at the gate are released only to give up.
                        abandoned.set(allSubmitted == false);
                        go.countDown();
                    }
                    for (Future<Boolean> outcome : outcomes) {
                        try {
                            broke |= outcome.get();
                        } catch (ExecutionException e) {
                            if (failure == null) {
                                failure = e.getCause();
                            }
                        }
                    }
                } finally {
                    // Wait for every query that started to finish. shutdownNow() would not cancel their requests, and their reservations
                    // would still be held when this test starts to tear down, with failures of their own that hide the first one.
                    clientsPool.shutdown();
                    assertTrue("the queries did not finish", clientsPool.awaitTermination(10, TimeUnit.MINUTES));
                }
                if (failure != null) {
                    throw new AssertionError("a concurrent query failed", failure);
                }
            }
            assertTrue("no query circuit broke, with 24 of them at once", broke);
            for (RestClient node : nodes) {
                assertLoadAllSingleColumn(node, index, docs);
            }
        }
    }

    /**
     * The clients of a test, closed together by try-with-resources: a client that cannot be closed adds to the failure of the test as a
     * suppressed exception, and does not replace it.
     */
    private static final class NodeClients implements Closeable {
        private final List<RestClient> clients = new ArrayList<>();

        @Override
        public void close() throws IOException {
            IOUtils.close(clients);
        }
    }

    /**
     * Index:
     * <ul>
     *     <li>The 5,000 documents with 1,000 unmapped leaves each of {@link #testManyDocsWithManyDistinctFields}: a query over all of
     *     them passes alone</li>
     * </ul>
     * Query:
     * <ul>
     *     <li>{@code LOAD_ALL}, then a {@code FORK} of 8 branches that each return every document</li>
     * </ul>
     * Expected: circuit break, and nothing left reserved afterwards
     * <p>
     * Eight branches that each return every document do not break at the default limit: the rows are not all held at once. So the
     * request breaker is lowered to a fixed size, see {@link #LOWERED_REQUEST_BREAKER}, until a read of this index trips it. The check
     * on what is left is the one that every test of this suite ends with, that the request breaker of each node is back at zero.
     * <p>
     * This does not show that a sibling branch that holds pages is cancelled and releases them: the test accepts any breaker response,
     * and a sibling may not hold pages yet when another branch fails, or may have finished. Holding a sibling at the moment that another
     * fails needs a hook in the compute engine.
     */
    public void testForkOfWideReadsCircuitBreaksAndLeavesNothingReserved() throws IOException {
        assumeTrue("requires LOAD_ALL with FORK", EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL_FORK.isEnabled());
        String index = "load_all_fork";
        int docs = 5_000;
        indexManyDocsWithManyDistinctFields(index, docs, 1_000);
        assertLoadAllSingleColumn(index, docs);

        setRequestBreakerLimit(LOWERED_REQUEST_BREAKER);
        assertCircuitBreaks(attempt -> {
            StringBuilder query = startQuery();
            query.append("SET unmapped_fields=\\\"LOAD_ALL\\\";\n");
            query.append("FROM ").append(index).append("\n| FORK");
            for (int branch = 0; branch < 8; branch++) {
                query.append(" (LIMIT ").append(docs).append(')');
            }
            query.append("\"}");
            return responseAsMap(query(query.toString(), "columns"));
        });
    }

    /** Runs {@code LOAD_ALL} over {@code index}: whether it circuit broke, or false if it succeeded. Fails on any other outcome. */
    private boolean loadAllBrokeTheCircuit(RestClient restClient, String index, int limit) throws IOException {
        StringBuilder query = startQuery();
        query.append("SET unmapped_fields=\\\"LOAD_ALL\\\";\n");
        query.append("FROM ").append(index).append("\n| LIMIT ").append(limit).append("\"}");
        try {
            query(restClient, query.toString(), "columns");
            return false;
        } catch (ResponseException e) {
            assertMap(
                responseAsMap(e.getResponse()),
                matchesMap().entry("status", 429).entry("error", matchesMap().extraOk().entry("type", "circuit_breaking_exception"))
            );
            return true;
        }
    }

    /** One document with the array {@code a} of {@code elements} one-character strings, or two arrays that are the field {@code a.b}. */
    private void indexDocumentWithArrays(String index, int elements, boolean spelledInTwoWays) throws IOException {
        createMostlyUnmappedIndex(index);
        try (XContentBuilder doc = XContentFactory.cborBuilder()) {
            doc.startObject().field(MAPPED_FIELD, "0");
            if (spelledInTwoWays) {
                arrayOfShortStrings(doc, "a.b", elements);
                doc.startObject("a");
                arrayOfShortStrings(doc, "b", elements);
                doc.endObject();
            } else {
                arrayOfShortStrings(doc, "a", elements);
            }
            doc.endObject();
            indexCborDocument(index, doc);
        }
        // Everything is indexed already; this only force-merges and refreshes.
        initIndex(index, "");
    }

    private Map<String, Object> loadAllOneRow(String index) throws IOException {
        StringBuilder query = startQuery();
        query.append("SET unmapped_fields=\\\"LOAD_ALL\\\";\n");
        query.append("FROM ").append(index).append("\n| LIMIT 1\"}");
        return responseAsMap(query(query.toString(), "columns"));
    }

    private void createMostlyUnmappedIndex(String index) throws IOException {
        CreateIndexResponse response = createIndex(index, Settings.EMPTY, """
            {
              "dynamic": false,
              "properties": {
                "%s": { "type": "keyword" }
              }
            }""".formatted(MAPPED_FIELD));
        assertTrue(response.isAcknowledged());
    }

    /**
     * Runs {@code LOAD_ALL} over all of {@code index} and asserts the response is exactly {@link #MAPPED_FIELD} and the single
     * {@code _unmapped_fields} column, with no warning about dropped fields. Only the columns are fetched: {@code filter_path} drops
     * the values on the server, after the response has been built, so every row is fully loaded without shipping every
     * row's JSON object to the test.
     */
    private void assertLoadAllSingleColumn(String index, int limit) throws IOException {
        assertLoadAllSingleColumn(client(), index, limit);
    }

    private void assertLoadAllSingleColumn(RestClient restClient, String index, int limit) throws IOException {
        StringBuilder query = startQuery();
        query.append("SET unmapped_fields=\\\"LOAD_ALL\\\";\n");
        query.append("FROM ").append(index).append("\n");
        query.append("| LIMIT ").append(limit).append("\"}");
        Response response = query(restClient, query.toString(), "columns");

        Map<String, Object> map = responseAsMap(response);
        List<?> columns = (List<?>) map.get("columns");
        List<String> names = new ArrayList<>(columns.size());
        for (Object column : columns) {
            names.add((String) ((Map<?, ?>) column).get("name"));
        }
        assertThat(names, containsInAnyOrder(MAPPED_FIELD, "_unmapped_fields"));
        assertThat(response.getWarnings(), empty());
    }

    private static String manyDocsFieldName(int i) {
        return String.format(Locale.ROOT, "f%08d", i);
    }

    private static String hugeDocsObjectName(int doc, int object) {
        return String.format(Locale.ROOT, "d%d_g%03d", doc, object);
    }

    private static String hugeDocsLeafName(int leaf) {
        return String.format(Locale.ROOT, "f%03d", leaf);
    }
}
