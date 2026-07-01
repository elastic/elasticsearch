/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Heap-attack coverage for {@code SET unmapped_fields="load"} paths that read
 * values from stored {@code _source}.
 */
@TimeoutSuite(millis = 10 * TimeUnits.MINUTE)
public class HeapAttackUnmappedLoadSourceIT extends HeapAttackTestCase {
    private static final String HUGE_SOURCE_INDEX = "unmapped_load_huge_source";
    private static final String MANY_SOURCE_ONLY_FIELDS_INDEX = "unmapped_load_many_source_fields";
    private static final String MULTI_SEGMENT_SOURCE_INDEX = "unmapped_load_multi_segment_source";
    private static final String METADATA_SOURCE_INDEX = "unmapped_load_metadata_source";
    private static final int HUGE_SOURCE_DOCS = 8;
    private static final int HUGE_SOURCE_SIZE_MB = 16;

    /**
     * Loads a tiny unmapped field from very large source documents after sorting by a mapped field.
     * The descending sort reverses the single-segment doc order, exercising the reordered
     * {@code ValuesFromSingleReader} path. This should remain cheap because the loader asks for
     * only the tiny source-only field, not the unrelated large source payload.
     */
    public void testHugeSourceTinyUnmappedFieldAfterSort() throws IOException {
        initHugeSourceTinyUnmappedIndex(HUGE_SOURCE_DOCS, HUGE_SOURCE_SIZE_MB);

        try {
            setRequestBreakerLimit("10%");
            Map<String, Object> response = fetchTinyUnmappedAfterSort(HUGE_SOURCE_DOCS);
            assertThat(response.get("columns"), equalTo(List.of(Map.of("name", "small_unmapped", "type", "keyword"))));

            List<List<String>> values = new ArrayList<>(HUGE_SOURCE_DOCS);
            for (int d = HUGE_SOURCE_DOCS - 1; d >= 0; d--) {
                values.add(List.of("value-" + d));
            }
            assertThat(response.get("values"), equalTo(values));
        } finally {
            setRequestBreakerLimit(null);
        }
    }

    public void testFetchTooManySourceOnlyUnmappedFields() throws IOException {
        int fields = 1000;
        initManySourceOnlyFieldsIndex(500, fields);
        assertCircuitBreaks(attempt -> fetchManySourceOnlyFields(fields, attempt * 100));
    }

    public void testLargeSourceOnlyUnmappedFieldAcrossSegmentsCircuitBreaks() throws IOException {
        initMultiSegmentLargeSourceOnlyFieldIndex(100, 2);

        try {
            setRequestBreakerLimit("25%");
            assertCircuitBreaks(attempt -> fetchMultiSegmentLargeSourceOnlyField(attempt * 25));
        } finally {
            setRequestBreakerLimit(null);
        }
    }

    public void testMetadataSourceWithUnmappedFieldCircuitBreaks() throws IOException {
        initHugeSourceTinyUnmappedIndex(METADATA_SOURCE_INDEX, HUGE_SOURCE_DOCS, HUGE_SOURCE_SIZE_MB);

        try {
            setRequestBreakerLimit("10%");
            assertCircuitBreaks(attempt -> fetchMetadataSourceWithUnmappedField(attempt * 2));
        } finally {
            setRequestBreakerLimit(null);
        }
    }

    private void initHugeSourceTinyUnmappedIndex(int docs, int sourceSizeMb) throws IOException {
        initHugeSourceTinyUnmappedIndex(HUGE_SOURCE_INDEX, docs, sourceSizeMb);
    }

    private void initHugeSourceTinyUnmappedIndex(String index, int docs, int sourceSizeMb) throws IOException {
        logger.info("loading {} documents with one {}MB source-only payload", docs, sourceSizeMb);
        CreateIndexResponse response = createIndex(
            index,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build(),
            """
                {
                  "dynamic": false,
                  "properties": {
                    "sort_key": {
                      "type": "long"
                    }
                  }
                }"""
        );
        assertTrue(response.isAcknowledged());

        int sourceSize = Math.toIntExact(ByteSizeValue.ofMb(sourceSizeMb).getBytes());
        String hugePayload = "x".repeat(sourceSize);
        for (int d = 0; d < docs; d++) {
            /*
             * Keep setup pressure separate from the query under test: each huge source
             * document is sent as its own flushed bulk request by HeapAttackTestCase#bulk.
             */
            String bulk = String.format(
                Locale.ROOT,
                """
                    {"create":{}}
                    {"sort_key":%d,"small_unmapped":"value-%d","huge_payload":"%s"}
                    """,
                d,
                d,
                hugePayload
            );
            bulk(index, bulk);
        }
        initIndex(index, "");
    }

    private void initManySourceOnlyFieldsIndex(int docs, int fields) throws IOException {
        logger.info("loading {} documents with {} 1KB source-only fields", docs, fields);
        CreateIndexResponse response = createIndex(
            MANY_SOURCE_ONLY_FIELDS_INDEX,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build(),
            """
                {
                  "dynamic": false,
                  "properties": {
                    "sort_key": {
                      "type": "long"
                    }
                  }
                }"""
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
                bulk(MANY_SOURCE_ONLY_FIELDS_INDEX, bulk.toString());
                bulk.setLength(0);
            }
        }
        initIndex(MANY_SOURCE_ONLY_FIELDS_INDEX, bulk.toString());
    }

    private void initMultiSegmentLargeSourceOnlyFieldIndex(int docs, int sourceSizeMb) throws IOException {
        logger.info("loading {} documents with one {}MB source-only field across multiple segments", docs, sourceSizeMb);
        CreateIndexResponse response = createIndex(
            MULTI_SEGMENT_SOURCE_INDEX,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build(),
            """
                {
                  "dynamic": false,
                  "properties": {}
                }"""
        );
        assertTrue(response.isAcknowledged());

        int docsPerBulk = 5;
        int sourceSize = Math.toIntExact(ByteSizeValue.ofMb(sourceSizeMb).getBytes());
        String largeValue = "x".repeat(sourceSize);
        StringBuilder bulk = new StringBuilder();
        for (int d = 0; d < docs; d++) {
            bulk.append("{\"create\":{}}\n");
            bulk.append("{\"big_unmapped\":\"").append(largeValue).append("\"}\n");
            if (d % docsPerBulk == docsPerBulk - 1 && d != docs - 1) {
                bulk(MULTI_SEGMENT_SOURCE_INDEX, bulk.toString());
                bulk.setLength(0);
            }
        }
        if (bulk.isEmpty() == false) {
            bulk(MULTI_SEGMENT_SOURCE_INDEX, bulk.toString());
        }
        refreshIndex(MULTI_SEGMENT_SOURCE_INDEX);
    }

    private Map<String, Object> fetchTinyUnmappedAfterSort(int limit) throws IOException {
        StringBuilder query = startQuery();
        query.append("SET unmapped_fields=\\\"load\\\";\n");
        query.append("FROM ").append(HUGE_SOURCE_INDEX).append("\n");
        query.append("| SORT sort_key DESC\n");
        query.append("| KEEP small_unmapped\n");
        query.append("| LIMIT ").append(limit).append("\"}");
        return responseAsMap(query(query.toString(), "columns,values"));
    }

    private Map<String, Object> fetchMetadataSourceWithUnmappedField(int limit) throws IOException {
        StringBuilder query = startQuery();
        query.append("SET unmapped_fields=\\\"load\\\";\n");
        query.append("FROM ").append(METADATA_SOURCE_INDEX).append(" METADATA _source\n");
        query.append("| SORT sort_key DESC\n");
        query.append("| KEEP _source, small_unmapped\n");
        query.append("| LIMIT ").append(limit).append("\"}");
        return responseAsMap(query(query.toString(), "columns,values"));
    }

    private Map<String, Object> fetchManySourceOnlyFields(int fields, int limit) throws IOException {
        StringBuilder query = startQuery();
        query.append("SET unmapped_fields=\\\"load\\\";\n");
        query.append("FROM ").append(MANY_SOURCE_ONLY_FIELDS_INDEX).append("\n");
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

    private Map<String, Object> fetchMultiSegmentLargeSourceOnlyField(int limit) throws IOException {
        StringBuilder query = startQuery();
        query.append("SET unmapped_fields=\\\"load\\\";\n");
        query.append("FROM ").append(MULTI_SEGMENT_SOURCE_INDEX).append("\n");
        query.append("| KEEP big_unmapped\n");
        query.append("| LIMIT ").append(limit).append("\"}");
        return responseAsMap(query(query.toString(), "columns,values"));
    }

    private static String fieldName(int field) {
        return "f" + String.format(Locale.ROOT, "%03d", field);
    }

    private void refreshIndex(String index) throws IOException {
        Request request = new Request("POST", "/" + index + "/_refresh");
        Response response = client().performRequest(request);
        assertWriteResponse(response);
    }
}
