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
 * Heap-attack coverage for mapped fields that have neither doc values nor stored fields, so they are loaded from
 * {@code _ignored_source} by the fallback synthetic source block loader. See
 * <a href="https://github.com/elastic/elasticsearch/issues/159349">#159349</a>.
 */
public class HeapAttackFallbackSyntheticSourceIT extends HeapAttackTestCase {
    private static final String MANY_FALLBACK_FIELDS_INDEX = "many_fallback_synthetic_source_fields";

    /**
     * Index:
     * <ul>
     *     <li>Synthetic source</li>
     *     <li>TSDB doc values format, so {@code _ignored_source} is binary doc values</li>
     *     <li>Mapped sort key</li>
     *     <li>Many small keyword fields without doc values or index</li>
     * </ul>
     * Query:
     * <ul>
     *     <li>Keep all the keyword fields</li>
     * </ul>
     * Expected: Circuit break
     */
    public void testFetchTooManyFallbackSyntheticSourceFields() throws IOException {
        int fields = 1000;
        initManyFallbackFieldsIndex(500, fields);

        try {
            setRequestBreakerLimit("40%");
            assertCircuitBreaks(attempt -> fetchManyFallbackFields(fields, attempt * 100));
        } finally {
            setRequestBreakerLimit(null);
        }
    }

    private void initManyFallbackFieldsIndex(int docs, int fields) throws IOException {
        logger.info("loading {} documents with {} 1KB fallback synthetic source fields", docs, fields);
        StringBuilder mapping = new StringBuilder();
        mapping.append("{\"properties\": {\"sort_key\": {\"type\": \"long\"}");
        for (int f = 0; f < fields; f++) {
            mapping.append(",\"").append(fieldName(f)).append("\": {\"type\": \"keyword\", \"doc_values\": false, \"index\": false}");
        }
        mapping.append("}}");
        CreateIndexResponse response = createIndex(
            MANY_FALLBACK_FIELDS_INDEX,
            Settings.builder()
                .put("index.mapping.source.mode", "synthetic")
                .put("index.use_time_series_doc_values_format", true)
                .put("index.mapping.total_fields.limit", fields + 100)
                .build(),
            mapping.toString()
        );
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
                bulk(MANY_FALLBACK_FIELDS_INDEX, bulk.toString());
                bulk.setLength(0);
            }
        }
        initIndex(MANY_FALLBACK_FIELDS_INDEX, bulk.toString());
    }

    private Map<String, Object> fetchManyFallbackFields(int fields, int limit) throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM ").append(MANY_FALLBACK_FIELDS_INDEX).append("\n");
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
