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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;

/**
 * Heap-attack coverage for {@code SET unmapped_fields="LOAD_ALL"} over an index with synthetic {@code _source}, where the unmapped
 * fields are rebuilt from {@code _ignored_source}. {@link HeapAttackUnmappedLoadAllIT} attacks the same reads over stored
 * {@code _source}.
 * <p>
 * A document that is one array of a million tiny objects is not attacked here: it can be indexed, but a data node of this suite, which
 * runs with assertions, then compares two copies of it in the translog writer, parses each into maps, and runs out of memory
 * before any query. That is the assertion, not the read.
 */
public class HeapAttackUnmappedLoadAllSyntheticSourceIT extends HeapAttackTestCase {
    private static final String MAPPED_FIELD = "id";

    @Before
    public void requireLoadAll() {
        assumeTrue("requires the LOAD_ALL single column", EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL_SINGLE_SOURCE_COLUMN.isEnabled());
    }

    /**
     * Index:
     * <ul>
     *     <li>Synthetic {@code _source}, {@code dynamic: false}</li>
     *     <li>One document with an array of 7M one-character strings, indexed as CBOR: 14MB of {@code _source}</li>
     * </ul>
     * Query:
     * <ul>
     *     <li>{@code LOAD_ALL} over every document</li>
     * </ul>
     * Expected: circuit break while the values of the array are held, as it does over stored {@code _source}
     */
    public void testArrayOfShortStringsCircuitBreaksWhileItsValuesAreHeld() throws IOException {
        String index = "load_all_synthetic_short_strings";
        createSyntheticIndex(index);
        try (XContentBuilder doc = XContentFactory.cborBuilder()) {
            doc.startObject().field(MAPPED_FIELD, "0");
            arrayOfShortStrings(doc, "a", scaleToHeap(7_000_000));
            doc.endObject();
            indexCborDocument(index, doc);
        }
        // Everything is indexed already; this only force-merges and refreshes.
        initIndex(index, "");

        assertCircuitBreaks(
            attempt -> loadAllOneRow(index),
            matchesMap().entry("status", 429)
                .entry(
                    "error",
                    matchesMap().extraOk()
                        .entry("type", "circuit_breaking_exception")
                        .entry("reason", containsString("[unmapped fields held values]"))
                )
        );
    }

    /**
     * Index:
     * <ul>
     *     <li>Synthetic {@code _source}, {@code dynamic: false}</li>
     *     <li>50 documents with 100k unmapped leaves each, nested 100 to an object: {@code {"d0_g000": {"f000": 1, ...}, ...}}</li>
     *     <li>every leaf name is distinct across the index: 5M names</li>
     * </ul>
     * Query:
     * <ul>
     *     <li>{@code LOAD_ALL} over every document</li>
     * </ul>
     * Expected: the columns {@code id} and {@code _unmapped_fields}, and no warning
     * <p>
     * Each object is its own entry in {@code _ignored_source}, so this reads a thousand entries and merges them into the one JSON
     * object of a row, which stored {@code _source} does not. A sampled row must hold all of its 100,000 leaves.
     */
    public void testFewDocsWithHugeNumberOfDistinctFields() throws IOException {
        String index = "load_all_synthetic_huge_docs";
        int docs = 50;
        int objectsPerDoc = 1_000;
        int leavesPerObject = 100;
        createSyntheticIndex(index);
        for (int d = 0; d < docs; d++) {
            StringBuilder bulk = new StringBuilder();
            bulk.append("{\"create\":{}}\n{\"").append(MAPPED_FIELD).append("\":\"").append(d).append('"');
            for (int o = 0; o < objectsPerDoc; o++) {
                bulk.append(",\"").append(String.format(Locale.ROOT, "d%d_g%03d", d, o)).append("\":{");
                for (int l = 0; l < leavesPerObject; l++) {
                    if (l > 0) {
                        bulk.append(',');
                    }
                    bulk.append('"').append(String.format(Locale.ROOT, "f%03d", l)).append("\":1");
                }
                bulk.append('}');
            }
            bulk.append("}\n");
            // One document per bulk keeps each request well under the indexing pressure limit.
            bulk(index, bulk.toString());
        }
        // Everything is indexed already; this only force-merges and refreshes.
        initIndex(index, "");

        StringBuilder query = startQuery();
        query.append("SET unmapped_fields=\\\"LOAD_ALL\\\";\n");
        query.append("FROM ").append(index).append("\n");
        query.append("| LIMIT ").append(docs).append("\"}");
        Response response = query(query.toString(), "columns");

        Map<String, Object> map = responseAsMap(response);
        List<?> columns = (List<?>) map.get("columns");
        List<String> names = new ArrayList<>(columns.size());
        for (Object column : columns) {
            names.add((String) ((Map<?, ?>) column).get("name"));
        }
        assertThat(names, containsInAnyOrder(MAPPED_FIELD, "_unmapped_fields"));
        assertThat(response.getWarnings(), empty());

        // The query above checks the shape of the response, and would pass if only one of the thousand entries of a row were merged.
        // So read the values of one document, and check that its JSON holds every leaf, with some from the first, the middle and the last.
        int sampled = 7;
        StringBuilder sample = startQuery();
        sample.append("SET unmapped_fields=\\\"LOAD_ALL\\\";\n");
        sample.append("FROM ").append(index).append("\n| WHERE ").append(MAPPED_FIELD).append(" == \\\"").append(sampled).append("\\\"\n");
        sample.append("| LIMIT 1\"}");
        Map<String, Object> sampleMap = responseAsMap(query(sample.toString(), "columns,values"));
        int column = -1;
        List<?> sampleColumns = (List<?>) sampleMap.get("columns");
        for (int c = 0; c < sampleColumns.size(); c++) {
            if ("_unmapped_fields".equals(((Map<?, ?>) sampleColumns.get(c)).get("name"))) {
                column = c;
            }
        }
        assertThat(column, greaterThanOrEqualTo(0));
        Map<?, ?> json = (Map<?, ?>) ((List<?>) ((List<?>) sampleMap.get("values")).get(0)).get(column);
        assertThat(json.size(), equalTo(objectsPerDoc * leavesPerObject));
        for (int o : new int[] { 0, objectsPerDoc / 2, objectsPerDoc - 1 }) {
            for (int l : new int[] { 0, leavesPerObject / 2, leavesPerObject - 1 }) {
                assertThat(json, hasKey(String.format(Locale.ROOT, "d%d_g%03d.f%03d", sampled, o, l)));
            }
        }
    }

    private Map<String, Object> loadAllOneRow(String index) throws IOException {
        StringBuilder query = startQuery();
        query.append("SET unmapped_fields=\\\"LOAD_ALL\\\";\n");
        query.append("FROM ").append(index).append("\n| LIMIT 1\"}");
        return responseAsMap(query(query.toString(), "columns"));
    }

    private void createSyntheticIndex(String index) throws IOException {
        CreateIndexResponse response = createIndex(index, Settings.builder().put("index.mapping.source.mode", "synthetic").build(), """
            {
              "dynamic": false,
              "properties": {
                "%s": { "type": "keyword" }
              }
            }""".formatted(MAPPED_FIELD));
        assertTrue(response.isAcknowledged());
    }
}
