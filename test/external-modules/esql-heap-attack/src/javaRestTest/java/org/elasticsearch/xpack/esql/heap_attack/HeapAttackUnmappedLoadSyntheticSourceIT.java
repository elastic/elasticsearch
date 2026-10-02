/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

/**
 * Heap-attack coverage for {@code SET unmapped_fields="load"} when values come
 * from synthetic {@code _source}.
 */
public class HeapAttackUnmappedLoadSyntheticSourceIT extends HeapAttackTestCase {
    private static final String MANY_SYNTHETIC_SOURCE_ONLY_FIELDS_INDEX = "unmapped_load_many_synthetic_source_fields";
    private static final String MANY_DOC_VALUES_IGNORED_SOURCE_FIELDS_INDEX = "unmapped_load_many_doc_values_ignored_source_fields";

    /**
     * Index:
     * <ul>
     *     <li>Synthetic source</li>
     *     <li>Mapped sort key</li>
     *     <li>Many small source-only fields</li>
     * </ul>
     * Query:
     * <ul>
     *     <li>Keep all source-only fields as unmapped LOAD columns</li>
     * </ul>
     * Expected: Circuit break
     */
    public void testFetchTooManySyntheticSourceOnlyUnmappedFields() throws IOException {
        int fields = 1000;
        initManySyntheticSourceOnlyFieldsIndex(MANY_SYNTHETIC_SOURCE_ONLY_FIELDS_INDEX, false, 500, fields);

        try {
            setRequestBreakerLimit("40%");
            assertCircuitBreaks(
                attempt -> fetchManySyntheticSourceOnlyFields(MANY_SYNTHETIC_SOURCE_ONLY_FIELDS_INDEX, fields, attempt * 100)
            );
        } finally {
            setRequestBreakerLimit(null);
        }
    }

    /**
     * Same as {@link #testFetchTooManySyntheticSourceOnlyUnmappedFields} but {@code _ignored_source} is stored as binary doc values
     * (the TSDB doc values format) instead of a stored field, which is what the reproduction in
     * <a href="https://github.com/elastic/elasticsearch/issues/159349">#159349</a> used.
     * <p>
     * Index:
     * <ul>
     *     <li>Synthetic source</li>
     *     <li>TSDB doc values format, so {@code _ignored_source} is binary doc values</li>
     *     <li>Mapped sort key</li>
     *     <li>Many small source-only fields</li>
     * </ul>
     * Query:
     * <ul>
     *     <li>Keep all source-only fields as unmapped LOAD columns</li>
     * </ul>
     * Expected: Circuit break
     */
    public void testFetchTooManyDocValuesIgnoredSourceUnmappedFields() throws IOException {
        int fields = 1000;
        initManySyntheticSourceOnlyFieldsIndex(MANY_DOC_VALUES_IGNORED_SOURCE_FIELDS_INDEX, true, 500, fields);

        try {
            setRequestBreakerLimit("40%");
            assertCircuitBreaks(
                attempt -> fetchManySyntheticSourceOnlyFields(MANY_DOC_VALUES_IGNORED_SOURCE_FIELDS_INDEX, fields, attempt * 100)
            );
        } finally {
            setRequestBreakerLimit(null);
        }
    }

    /**
     * Single-segment index:
     * <ul>
     *     <li>Synthetic source</li>
     *     <li>Mapped sort key</li>
     *     <li>Many small source-only fields</li>
     * </ul>
     *
     * @param docValuesIgnoredSource whether to store {@code _ignored_source} as binary doc values rather than a stored field
     */
    private void initManySyntheticSourceOnlyFieldsIndex(String index, boolean docValuesIgnoredSource, int docs, int fields)
        throws IOException {
        logger.info("loading {} documents with {} 1KB synthetic source-only fields", docs, fields);
        Settings.Builder settings = Settings.builder().put("index.mapping.source.mode", "synthetic");
        if (docValuesIgnoredSource) {
            settings.put("index.use_time_series_doc_values_format", true);
        }
        CreateIndexResponse response = createIndex(index, settings.build(), """
            {
              "dynamic": false,
              "properties": {
                "sort_key": {
                  "type": "long"
                }
              }
            }""");
        assertTrue(response.isAcknowledged());

        int docsPerBulk = 5;
        int fieldSize = Math.toIntExact(ByteSizeValue.ofKb(1).getBytes());
        StringBuilder bulk = new StringBuilder();
        for (int d = 0; d < docs; d++) {
            bulk.append("{\"create\":{}}\n");
            bulk.append("{\"sort_key\":").append(d);
            for (int f = 0; f < fields; f++) {
                bulk.append(",\"").append(fieldName(f)).append("\":\"");
                bulk.append(Integer.toString(f % 10).repeat(fieldSize));
                bulk.append('"');
            }
            bulk.append("}\n");
            if (d % docsPerBulk == docsPerBulk - 1 && d != docs - 1) {
                bulk(index, bulk.toString());
                bulk.setLength(0);
            }
        }
        initIndex(index, bulk.toString());
    }

    private Map<String, Object> fetchManySyntheticSourceOnlyFields(String index, int fields, int limit) throws IOException {
        StringBuilder query = startQuery();
        query.append("SET unmapped_fields=\\\"load\\\";\n");
        query.append("FROM ").append(index).append("\n");
        query.append("| SORT sort_key\n");
        query.append("| KEEP ");
        for (int f = 0; f < fields; f++) {
            if (f > 0) {
                query.append(", ");
            }
            query.append(fieldName(f));
        }
        query.append("\n| LIMIT ").append(limit).append("\"}");
        return responseAsMap(query(query.toString(), "columns"));
    }

    private static String fieldName(int field) {
        return "f" + String.format(Locale.ROOT, "%03d", field);
    }
}
