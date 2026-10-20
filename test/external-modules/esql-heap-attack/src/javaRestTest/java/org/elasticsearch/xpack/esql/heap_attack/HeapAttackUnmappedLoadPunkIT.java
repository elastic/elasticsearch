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
import java.util.Map;

/**
 * Heap-attack coverage for PUNK (partially unmapped non-keyword) fields that are made
 * legal by explicit casts under {@code SET unmapped_fields="load"}.
 */
public class HeapAttackUnmappedLoadPunkIT extends HeapAttackTestCase {
    private static final String MAPPED_LONG_INDEX = "a_punk_mapped_long";
    private static final String SOURCE_ONLY_INDEX = "z_punk_unmapped_source";

    /**
     * Index:
     * <ul>
     *     <li>One index maps {@code v} as long</li>
     *     <li>One index has large source-only {@code v} strings</li>
     * </ul>
     * Query:
     * <ul>
     *     <li>Cast PUNK field {@code v} to keyword</li>
     * </ul>
     * Expected: Circuit break in source loading
     */
    public void testPunkKeywordConversionCircuitBreaks() throws IOException {
        initPunkIndices(100, 2);

        try {
            setRequestBreakerLimit("40%");
            assertCircuitBreaks(attempt -> fetchPunkKeywordConversion(attempt * 25));
        } finally {
            setRequestBreakerLimit(null);
        }
    }

    /**
     * Two-index setup:
     * <ul>
     *     <li>Mapped long {@code v}</li>
     *     <li>Large source-only string {@code v}</li>
     * </ul>
     */
    private void initPunkIndices(int sourceOnlyDocs, int sourceFieldSizeMb) throws IOException {
        CreateIndexResponse mappedResponse = createIndex(MAPPED_LONG_INDEX, Settings.EMPTY, """
            {
              "properties": {
                "v": {
                  "type": "long"
                }
              }
            }""");
        assertTrue(mappedResponse.isAcknowledged());

        StringBuilder mappedBulk = new StringBuilder();
        for (int d = 0; d < 10; d++) {
            mappedBulk.append("{\"create\":{}}\n");
            mappedBulk.append("{\"v\":").append(d).append("}\n");
        }
        initIndex(MAPPED_LONG_INDEX, mappedBulk.toString());

        CreateIndexResponse sourceOnlyResponse = createIndex(SOURCE_ONLY_INDEX, Settings.EMPTY, """
            {
              "dynamic": false,
              "properties": {}
            }""");
        assertTrue(sourceOnlyResponse.isAcknowledged());

        int docsPerBulk = 5;
        int fieldSize = Math.toIntExact(ByteSizeValue.ofMb(sourceFieldSizeMb).getBytes());
        String largeValue = "x".repeat(fieldSize);
        StringBuilder sourceOnlyBulk = new StringBuilder();
        for (int d = 0; d < sourceOnlyDocs; d++) {
            sourceOnlyBulk.append("{\"create\":{}}\n");
            sourceOnlyBulk.append("{\"v\":\"").append(largeValue).append("\"}\n");
            if (d % docsPerBulk == docsPerBulk - 1 && d != sourceOnlyDocs - 1) {
                bulk(SOURCE_ONLY_INDEX, sourceOnlyBulk.toString());
                sourceOnlyBulk.setLength(0);
            }
        }
        initIndex(SOURCE_ONLY_INDEX, sourceOnlyBulk.toString());
    }

    private Map<String, Object> fetchPunkKeywordConversion(int limit) throws IOException {
        StringBuilder query = startQuery();
        query.append("SET unmapped_fields=\\\"load\\\";\n");
        query.append("FROM ").append(MAPPED_LONG_INDEX).append(", ").append(SOURCE_ONLY_INDEX).append(" METADATA _index\n");
        query.append("| SORT _index DESC\n");
        query.append("| EVAL v = v::keyword\n");
        query.append("| KEEP _index, v\n");
        query.append("| LIMIT ").append(limit).append("\"}");
        return responseAsMap(query(query.toString(), "columns,values"));
    }
}
