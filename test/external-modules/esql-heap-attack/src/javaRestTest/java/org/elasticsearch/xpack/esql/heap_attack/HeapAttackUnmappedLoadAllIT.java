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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

/**
 * Heap-attack coverage for {@code SET unmapped_fields="LOAD_ALL"}, which turns every distinct {@code _source} leaf of the result
 * rows into an output column. The coordinator must cap the number of such columns rather than collect an unbounded set of field
 * names: each test indexes millions of distinct unmapped leaves - far more names than fit in the heap - and expects the query to
 * return only the alphabetically first {@link #MAX_EXPANDED_FIELDS} of them, with a warning.
 * <p>
 * The two tests differ in where the distinct names come from: spread thinly across many moderately sized documents, or packed
 * into a handful of huge ones.
 * <p>
 * Each index maps a single {@link #MAPPED_FIELD}, so the queries exercise the expansion rather than whatever an index with no
 * mapped fields at all would plan to.
 */
public class HeapAttackUnmappedLoadAllIT extends HeapAttackTestCase {
    private static final int MAX_EXPANDED_FIELDS = 1000;
    private static final String MAPPED_FIELD = "id";

    @Before
    public void requireLoadAll() {
        assumeTrue("requires the LOAD_ALL field cap", EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL_MAX_FIELDS.isEnabled());
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
     * Expected: the first 1,000 names - all of document 0's leaves - and a warning
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

        List<String> expected = new ArrayList<>(MAX_EXPANDED_FIELDS);
        for (int i = 0; i < MAX_EXPANDED_FIELDS; i++) {
            expected.add(manyDocsFieldName(i));
        }
        assertLoadAllCapped(index, docs, expected);
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
     * Expected: the first 1,000 names - the leaves of document 0's first objects - and a warning
     * <p>
     * Serverless has a bigger heap but 6 shards, so documents are read concurrently; the block loader reserves a multiple of a
     * document's {@code _source} while reading it, and 9MB documents then trip the request breaker long before the field names are an
     * issue. There the same 5M names are spread over ten times as many, ten times smaller documents: still 100k fields in each.
     */
    public void testFewDocsWithHugeNumberOfDistinctFields() throws IOException {
        String index = "load_all_huge_docs";
        boolean serverless = isServerless();
        int docs = serverless ? 50 : 5;
        int objectsPerDoc = 1_000;
        int leavesPerObject = serverless ? 100 : 1_000;
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

        // Dotted names sort object by object, then by leaf, and document 0's names sort before every other document's.
        List<String> expected = new ArrayList<>(MAX_EXPANDED_FIELDS);
        for (int o = 0; expected.size() < MAX_EXPANDED_FIELDS; o++) {
            for (int l = 0; l < leavesPerObject && expected.size() < MAX_EXPANDED_FIELDS; l++) {
                expected.add(hugeDocsObjectName(0, o) + "." + hugeDocsLeafName(l));
            }
        }
        assertLoadAllCapped(index, docs, expected);
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
     * Runs {@code LOAD_ALL} over all of {@code index} and asserts it returns {@link #MAPPED_FIELD} plus exactly the discovered
     * {@code expectedDiscovered}, in that order among themselves. Where the mapped field lands relative to them is the ordering
     * replay's business, not this test's, so it is only checked for presence. Only the columns are
     * fetched: {@code filter_path} drops the values on the server, after the response has been built, so the expansion is fully
     * exercised without shipping a {@code rows x 1000} table of mostly nulls to the test.
     */
    private void assertLoadAllCapped(String index, int limit, List<String> expectedDiscovered) throws IOException {
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
        assertThat(names, hasItem(MAPPED_FIELD));
        names.remove(MAPPED_FIELD);
        assertThat(names, equalTo(expectedDiscovered));
        assertThat(response.getWarnings(), hasItem(containsString("only the first [" + MAX_EXPANDED_FIELDS + "]")));
    }

    /** Zero-padded so the alphabetical order the cap keeps is the numeric order. */
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
