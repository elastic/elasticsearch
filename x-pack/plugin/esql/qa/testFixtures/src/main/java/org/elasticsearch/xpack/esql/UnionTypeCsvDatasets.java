/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.List;

/**
 * One CSV index per indexable ES|QL {@link DataType}, named {@code union_<typeName>}.
 * Each index has a {@code field} of that type and a {@code type} keyword holding {@link DataType#typeName()}.
 * Generative tests load the whole catalog, so pairing {@code FROM union_a, union_b} covers that union.
 * <p>
 * Types that cannot be stored as a mapped field (meta types, runtime-only geo grids, periods) are omitted.
 * The switch is exhaustive: adding a {@link DataType} fails compilation until it is classified here.
 * Counters need a time-series index, so those datasets also carry {@code @timestamp}.
 */
final class UnionTypeCsvDatasets {

    static final String INDEX_PREFIX = "union_";

    private static final String COUNTER_SETTINGS = """
        {
          "index": {
            "mode": "time_series",
            "routing_path": ["type"],
            "time_series": {
              "start_time": "2024-05-10T00:00:00Z",
              "end_time": "2024-05-20T00:00:00Z"
            }
          }
        }
        """;

    private UnionTypeCsvDatasets() {}

    static List<CsvTestsDataLoader.TestDataset> datasets() {
        List<CsvTestsDataLoader.TestDataset> datasets = new ArrayList<>();
        for (DataType type : DataType.values()) {
            CsvTestsDataLoader.TestDataset dataset = datasetFor(type);
            if (dataset != null) {
                datasets.add(dataset);
            }
        }
        return datasets;
    }

    private static CsvTestsDataLoader.TestDataset datasetFor(DataType type) {
        return switch (type) {
            case UNSUPPORTED, NULL, SOURCE, DATE_PERIOD, TIME_DURATION, DOC_DATA_TYPE, TSID_DATA_TYPE, PARTIAL_AGG, GEOHASH, GEOTILE,
                GEOHEX -> null;
            case BOOLEAN -> simple(type, "\"type\": \"boolean\"", "boolean", "true");
            case LONG -> simple(type, "\"type\": \"long\"", "long", "1");
            case INTEGER -> simple(type, "\"type\": \"integer\"", "integer", "1");
            case UNSIGNED_LONG -> simple(type, "\"type\": \"unsigned_long\"", "unsigned_long", "1");
            case SHORT -> simple(type, "\"type\": \"short\"", "short", "1");
            case BYTE -> simple(type, "\"type\": \"byte\"", "byte", "1");
            case DOUBLE -> simple(type, "\"type\": \"double\"", "double", "1.1");
            case FLOAT -> simple(type, "\"type\": \"float\"", "float", "1.1");
            case HALF_FLOAT -> simple(type, "\"type\": \"half_float\"", "half_float", "1.1");
            case SCALED_FLOAT -> simple(type, "\"type\": \"scaled_float\", \"scaling_factor\": 1", "scaled_float", "1.1");
            case KEYWORD -> simple(type, "\"type\": \"keyword\"", "keyword", "foo");
            case TEXT -> simple(type, "\"type\": \"text\"", "text", "foo");
            case DATETIME -> simple(type, "\"type\": \"date\"", "date", "2025-01-01T01:00:00Z");
            case DATE_NANOS -> simple(type, "\"type\": \"date_nanos\"", "date_nanos", "2025-01-01T01:00:00.000000001Z");
            case IP -> simple(type, "\"type\": \"ip\"", "ip", "127.0.0.1");
            case VERSION -> simple(type, "\"type\": \"version\"", "version", "1.0.0");
            case GEO_POINT -> simple(type, "\"type\": \"geo_point\"", "geo_point", "POINT (1.0 2.0)");
            case GEO_SHAPE -> simple(type, "\"type\": \"geo_shape\"", "geo_shape", "POINT (1.0 2.0)");
            case CARTESIAN_POINT -> simple(type, "\"type\": \"point\"", "point", "POINT (1.0 2.0)");
            case CARTESIAN_SHAPE -> simple(type, "\"type\": \"shape\"", "shape", "POINT (1.0 2.0)");
            case OBJECT -> simple(type, "\"type\": \"object\"", "object", "{\"inner\":\"x\"}");
            case DATE_RANGE -> capped(
                type,
                "\"type\": \"date_range\"",
                "date_range",
                "1989-01-01..2025-01-01",
                EsqlCapabilities.Cap.DATE_RANGE_FIELD_TYPE_V6
            );
            case DOUBLE_RANGE -> capped(
                type,
                "\"type\": \"double_range\"",
                "double_range",
                "1.0..2.0",
                EsqlCapabilities.Cap.DOUBLE_RANGE_TECH_PREVIEW
            );
            case AGGREGATE_METRIC_DOUBLE -> simple(
                type,
                "\"type\": \"aggregate_metric_double\", \"metrics\": [\"min\", \"max\", \"sum\", \"value_count\"], "
                    + "\"default_metric\": \"max\"",
                "aggregate_metric_double",
                "{\"min\":-302.5\\,\"max\":702.3\\,\"sum\":200.0\\,\"value_count\":25}"
            );
            case DENSE_VECTOR -> capped(
                type,
                "\"type\": \"dense_vector\"",
                "dense_vector",
                "[0.5, 10, 6]",
                EsqlCapabilities.Cap.DENSE_VECTOR_FIELD_TYPE_RELEASED
            );
            case FLATTENED -> capped(
                type,
                "\"type\": \"flattened\"",
                "flattened",
                "{\"d\":\"baz\"\\,\"b\":\"foo\"}",
                EsqlCapabilities.Cap.FLATTENED_DATATYPE
            );
            case HISTOGRAM -> capped(
                type,
                "\"type\": \"histogram\"",
                "histogram",
                "{\"values\":[0.1\\,0.2\\,0.3]\\,\"counts\":[3\\,7\\,23]}",
                EsqlCapabilities.Cap.HISTOGRAM_RELEASE_VERSION
            );
            case TDIGEST -> capped(
                type,
                "\"type\": \"tdigest\"",
                "tdigest",
                "{\"min\":0.1\\,\"max\":0.3\\,\"sum\":15.5\\,\"centroids\":[0.1\\,0.2\\,0.3]\\,\"counts\":[3\\,7\\,23]}",
                EsqlCapabilities.Cap.TDIGEST_TECH_PREVIEW
            );
            case EXPONENTIAL_HISTOGRAM -> capped(
                type,
                "\"type\": \"exponential_histogram\"",
                "exponential_histogram",
                "{\"scale\":0}",
                EsqlCapabilities.Cap.EXPONENTIAL_HISTOGRAM_TECH_PREVIEW
            );
            case COUNTER_LONG -> counter(type, "long", "1");
            case COUNTER_INTEGER -> counter(type, "integer", "1");
            case COUNTER_DOUBLE -> counter(type, "double", "1.1");
        };
    }

    private static CsvTestsDataLoader.TestDataset simple(DataType type, String fieldMapping, String headerType, String value) {
        return build(type, standardMapping(fieldMapping), row(type, headerType, value), null, List.of());
    }

    private static CsvTestsDataLoader.TestDataset capped(
        DataType type,
        String fieldMapping,
        String headerType,
        String value,
        EsqlCapabilities.Cap capability
    ) {
        return build(type, standardMapping(fieldMapping), row(type, headerType, value), null, List.of(capability));
    }

    private static CsvTestsDataLoader.TestDataset counter(DataType type, String numericType, String value) {
        String mapping = """
            {
              "properties": {
                "type": { "type": "keyword", "time_series_dimension": true },
                "@timestamp": { "type": "date" },
                "field": { "type": "%s", "time_series_metric": "counter" }
              }
            }
            """.formatted(numericType);
        String csv = "type:keyword,@timestamp:date,field:" + numericType + "\n" + type.typeName() + ",2024-05-11T00:00:00Z," + value + "\n";
        return build(type, mapping, csv, COUNTER_SETTINGS, List.of());
    }

    private static CsvTestsDataLoader.TestDataset build(
        DataType type,
        String mapping,
        String csv,
        String settings,
        List<EsqlCapabilities.Cap> capabilities
    ) {
        return CsvTestsDataLoader.TestDataset.inline(INDEX_PREFIX + type.typeName(), mapping, csv, settings, capabilities);
    }

    private static String standardMapping(String fieldMapping) {
        return """
            {
              "properties": {
                "type": { "type": "keyword" },
                "field": { %s }
              }
            }
            """.formatted(fieldMapping);
    }

    private static String row(DataType type, String headerType, String value) {
        return "type:keyword,field:" + headerType + "\n" + type.typeName() + "," + value + "\n";
    }
}
