/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;

import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.Matchers.hasEntry;

/**
 * Generates the Kibana definition file describing the implicit time-series
 * conversions applied inside {@code TS ... | STATS ...}: a bare field is
 * wrapped in {@code last_over_time} (or {@code histogram_merge_over_time} for
 * histograms) before the outer aggregate sees it. Kibana uses this file, in a
 * {@code TS} context only, to type-check aggregates such as
 * {@code AVG(counter_double)} against the converted type instead of the raw
 * field type.
 * <p>
 * Run with:
 * <pre>
 * ./gradlew :x-pack:plugin:esql:test --tests "TimeSeriesAggregateKibanaDefinitionTests"
 * </pre>
 */
public class TimeSeriesAggregateKibanaDefinitionTests extends ESTestCase {

    record Conversion(String function, DataType returnType) {}

    // TODO(sidosera): think of a better way to encode the rules of InsertDefaultInnerTimeSeriesAggregate,
    // so this switch does not have to mirror them by hand.
    /** The conversions the {@code InsertDefaultInnerTimeSeriesAggregate} rule applies, by field type. */
    static Map<String, Conversion> implicitConversions() {
        Map<String, Conversion> conversions = new TreeMap<>();
        for (DataType fieldType : DataType.values()) {
            switch (fieldType) {
                case COUNTER_DOUBLE -> conversions.put(fieldType.esNameIfPossible(), new Conversion("last_over_time", DataType.DOUBLE));
                case COUNTER_INTEGER -> conversions.put(fieldType.esNameIfPossible(), new Conversion("last_over_time", DataType.INTEGER));
                case COUNTER_LONG -> conversions.put(fieldType.esNameIfPossible(), new Conversion("last_over_time", DataType.LONG));
                case DOUBLE -> conversions.put(fieldType.esNameIfPossible(), new Conversion("last_over_time", DataType.DOUBLE));
                case INTEGER -> conversions.put(fieldType.esNameIfPossible(), new Conversion("last_over_time", DataType.INTEGER));
                case LONG -> conversions.put(fieldType.esNameIfPossible(), new Conversion("last_over_time", DataType.LONG));
                case TSID_DATA_TYPE -> conversions.put(
                    fieldType.esNameIfPossible(),
                    new Conversion("last_over_time", DataType.TSID_DATA_TYPE)
                );
                case DATETIME -> conversions.put(fieldType.esNameIfPossible(), new Conversion("last_over_time", DataType.DATETIME));
                case DATE_NANOS -> conversions.put(fieldType.esNameIfPossible(), new Conversion("last_over_time", DataType.DATE_NANOS));
                case FLATTENED -> conversions.put(fieldType.esNameIfPossible(), new Conversion("last_over_time", DataType.FLATTENED));
                case IP -> conversions.put(fieldType.esNameIfPossible(), new Conversion("last_over_time", DataType.IP));
                case KEYWORD -> conversions.put(fieldType.esNameIfPossible(), new Conversion("last_over_time", DataType.KEYWORD));
                case TEXT -> conversions.put(fieldType.esNameIfPossible(), new Conversion("last_over_time", DataType.KEYWORD));
                case EXPONENTIAL_HISTOGRAM -> conversions.put(
                    fieldType.esNameIfPossible(),
                    new Conversion("histogram_merge_over_time", DataType.EXPONENTIAL_HISTOGRAM)
                );
                case TDIGEST -> conversions.put(
                    fieldType.esNameIfPossible(),
                    new Conversion("histogram_merge_over_time", DataType.TDIGEST)
                );
                default -> {
                }
            }
        }
        return conversions;
    }

    public void testImplicitConversions() {
        Map<String, Conversion> conversions = implicitConversions();
        // the conversions from the issue: a counter aggregates as its plain numeric type,
        // and a histogram merges instead of taking the last value
        assertThat(conversions, hasEntry("counter_double", new Conversion("last_over_time", DataType.DOUBLE)));
        assertThat(
            conversions,
            hasEntry("exponential_histogram", new Conversion("histogram_merge_over_time", DataType.EXPONENTIAL_HISTOGRAM))
        );
        assertThat(conversions, hasEntry("text", new Conversion("last_over_time", DataType.KEYWORD)));
        // types the rewrite cannot convert get no entry
        for (String unsupported : new String[] { "boolean", "unsigned_long", "aggregate_metric_double", "histogram" }) {
            assertFalse("unexpected conversion for [" + unsupported + "]", conversions.containsKey(unsupported));
        }
    }

    public void testGenerateKibanaDefinition() throws Exception {
        String rendered;
        try (XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint().lfAtEnd().startObject()) {
            builder.field(
                "comment",
                "This is generated by ESQL's TimeSeriesAggregateKibanaDefinitionTests. "
                    + "Do not edit it. See docs/reference/query-languages/esql/README.md for how to regenerate it."
            );
            builder.startObject("types");
            for (Map.Entry<String, Conversion> entry : implicitConversions().entrySet()) {
                builder.startObject(entry.getKey());
                builder.field("function", entry.getValue().function());
                builder.field("returnType", entry.getValue().returnType().esNameIfPossible());
                builder.endObject();
            }
            builder.endObject();
            rendered = Strings.toString(builder.endObject());
        }
        DocsV3Support.Callbacks callbacks = DocsV3Support.callbacksFromSystemProperty();
        Path dir = PathUtils.get(System.getProperty("java.io.tmpdir")).resolve("esql").resolve("kibana").resolve("definition");
        callbacks.write(dir, "time_series_aggregates", "json", rendered, true);
    }
}
