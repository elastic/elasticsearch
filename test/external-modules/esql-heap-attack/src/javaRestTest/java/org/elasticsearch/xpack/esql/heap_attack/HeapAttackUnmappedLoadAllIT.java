/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
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
        int fieldsPerDoc = 1_000;
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

        assertLoadAllSingleColumn(index, docs);
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
        StringBuilder query = startQuery();
        query.append("SET unmapped_fields=\\\"LOAD_ALL\\\";\n");
        query.append("FROM ").append(index).append("\n");
        query.append("| LIMIT ").append(limit).append("\"}");
        Response response = query(query.toString(), "columns");

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
