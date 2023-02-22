/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.pivot;

import org.elasticsearch.aggregations.AggregationsPlugin;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TransformAggregationsTests extends ESTestCase {
    public void testResolveTargetMapping() {

        // avg
        assertEquals("double", TransformAggregations.resolveTargetMapping("avg", "int"));
        assertEquals("double", TransformAggregations.resolveTargetMapping("avg", "double"));

        // median_absolute_deviation
        assertEquals("double", TransformAggregations.resolveTargetMapping("median_absolute_deviation", "int"));
        assertEquals("double", TransformAggregations.resolveTargetMapping("median_absolute_deviation", "double"));

        // cardinality
        assertEquals("long", TransformAggregations.resolveTargetMapping("cardinality", "int"));
        assertEquals("long", TransformAggregations.resolveTargetMapping("cardinality", "double"));

        // value_count
        assertEquals("long", TransformAggregations.resolveTargetMapping("value_count", "int"));
        assertEquals("long", TransformAggregations.resolveTargetMapping("value_count", "double"));

        // max
        assertEquals("int", TransformAggregations.resolveTargetMapping("max", "int"));
        assertEquals("double", TransformAggregations.resolveTargetMapping("max", "double"));
        assertEquals("double", TransformAggregations.resolveTargetMapping("max", "aggregate_metric_double"));
        assertEquals("half_float", TransformAggregations.resolveTargetMapping("max", "half_float"));
        assertEquals("float", TransformAggregations.resolveTargetMapping("max", "scaled_float"));

        // min
        assertEquals("int", TransformAggregations.resolveTargetMapping("min", "int"));
        assertEquals("double", TransformAggregations.resolveTargetMapping("min", "double"));
        assertEquals("double", TransformAggregations.resolveTargetMapping("min", "aggregate_metric_double"));
        assertEquals("half_float", TransformAggregations.resolveTargetMapping("min", "half_float"));
        assertEquals("float", TransformAggregations.resolveTargetMapping("min", "scaled_float"));

        // sum
        assertEquals("double", TransformAggregations.resolveTargetMapping("sum", "double"));
        assertEquals("double", TransformAggregations.resolveTargetMapping("sum", "aggregate_metric_double"));
        assertEquals("double", TransformAggregations.resolveTargetMapping("sum", "half_float"));
        assertEquals("double", TransformAggregations.resolveTargetMapping("sum", null));

        // range
        assertEquals("long", TransformAggregations.resolveTargetMapping("range", "int"));
        assertEquals("long", TransformAggregations.resolveTargetMapping("range", "double"));
        assertEquals("long", TransformAggregations.resolveTargetMapping("range", "half_float"));
        assertEquals("long", TransformAggregations.resolveTargetMapping("range", "scaled_float"));

        // geo_centroid
        assertEquals("geo_point", TransformAggregations.resolveTargetMapping("geo_centroid", "geo_point"));
        assertEquals("geo_point", TransformAggregations.resolveTargetMapping("geo_centroid", null));

        // geo_bounds
        assertEquals("geo_shape", TransformAggregations.resolveTargetMapping("geo_bounds", "geo_shape"));
        assertEquals("geo_shape", TransformAggregations.resolveTargetMapping("geo_bounds", null));

        // geo_line
        assertEquals("geo_shape", TransformAggregations.resolveTargetMapping("geo_line", "geo_shape"));
        assertEquals("geo_shape", TransformAggregations.resolveTargetMapping("geo_line", null));

        // scripted_metric
        assertEquals("_dynamic", TransformAggregations.resolveTargetMapping("scripted_metric", null));
        assertEquals("_dynamic", TransformAggregations.resolveTargetMapping("scripted_metric", "int"));

        // bucket_script
        assertEquals("_dynamic", TransformAggregations.resolveTargetMapping("bucket_script", null));
        assertEquals("_dynamic", TransformAggregations.resolveTargetMapping("bucket_script", "int"));

        // bucket_selector
        assertEquals("_dynamic", TransformAggregations.resolveTargetMapping("bucket_selector", null));
        assertEquals("_dynamic", TransformAggregations.resolveTargetMapping("bucket_selector", "int"));

        // weighted_avg
        assertEquals("double", TransformAggregations.resolveTargetMapping("weighted_avg", null));
        assertEquals("double", TransformAggregations.resolveTargetMapping("weighted_avg", "double"));
        assertEquals("double", TransformAggregations.resolveTargetMapping("weighted_avg", "int"));

        // percentile
        assertEquals("double", TransformAggregations.resolveTargetMapping("percentiles", null));
        assertEquals("double", TransformAggregations.resolveTargetMapping("percentiles", "int"));

        // filter
        assertEquals("long", TransformAggregations.resolveTargetMapping("filter", null));
        assertEquals("long", TransformAggregations.resolveTargetMapping("filter", "long"));
        assertEquals("long", TransformAggregations.resolveTargetMapping("filter", "double"));

        // terms
        assertEquals("flattened", TransformAggregations.resolveTargetMapping("terms", null));
        assertEquals("flattened", TransformAggregations.resolveTargetMapping("terms", "keyword"));
        assertEquals("flattened", TransformAggregations.resolveTargetMapping("terms", "text"));

        // rare_terms
        assertEquals("flattened", TransformAggregations.resolveTargetMapping("rare_terms", null));
        assertEquals("flattened", TransformAggregations.resolveTargetMapping("rare_terms", "text"));
        assertEquals("flattened", TransformAggregations.resolveTargetMapping("rare_terms", "keyword"));

        // top_metrics
        assertEquals("int", TransformAggregations.resolveTargetMapping("top_metrics", "int"));
        assertEquals("double", TransformAggregations.resolveTargetMapping("top_metrics", "double"));
        assertEquals("ip", TransformAggregations.resolveTargetMapping("top_metrics", "ip"));
        assertEquals("keyword", TransformAggregations.resolveTargetMapping("top_metrics", "keyword"));

        // stats
        assertEquals("double", TransformAggregations.resolveTargetMapping("stats", null));
        assertEquals("double", TransformAggregations.resolveTargetMapping("stats", "int"));

        // corner case: source type null
        assertEquals(null, TransformAggregations.resolveTargetMapping("min", null));
    }

    public void testAggregationsVsTransforms() {
        // Note: if a new plugin is added, it must be added here
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Arrays.asList((new AnalyticsPlugin()), new AggregationsPlugin()));
        List<NamedWriteableRegistry.Entry> namedWriteables = searchModule.getNamedWriteables();

        List<String> aggregationNames = namedWriteables.stream()
            .filter(namedWritable -> namedWritable.categoryClass.equals(AggregationBuilder.class))
            .map(namedWritable -> namedWritable.name)
            .collect(Collectors.toList());

        for (String aggregationName : aggregationNames) {
            String message = Strings.format("""
                The following aggregation is unknown to transform: [%s]. If this is a newly added aggregation, \
                please open an issue to add transform support for it. Afterwards add "%s" to the list in %s. \
                Thanks!\
                """, aggregationName, aggregationName, TransformAggregations.class.getName());
            assertTrue(
                message,
                TransformAggregations.isSupportedByTransform(aggregationName)
                    || TransformAggregations.isUnSupportedByTransform(aggregationName)
            );
        }
    }

    public void testGetAggregationOutputTypesPercentiles() {
        AggregationBuilder percentialAggregationBuilder = new PercentilesAggregationBuilder("percentiles", new double[] { 1, 5, 10 }, null);

        Tuple<Map<String, String>, Map<String, String>> inputAndOutputTypes = TransformAggregations.getAggregationInputAndOutputTypes(
            percentialAggregationBuilder
        );
        assertTrue(inputAndOutputTypes.v1().isEmpty());
        Map<String, String> outputTypes = inputAndOutputTypes.v2();
        assertEquals(3, outputTypes.size());
        assertEquals("percentiles", outputTypes.get("percentiles.1"));
        assertEquals("percentiles", outputTypes.get("percentiles.5"));
        assertEquals("percentiles", outputTypes.get("percentiles.10"));

        // note: using the constructor, omits validation, in reality this test might fail
        percentialAggregationBuilder = new PercentilesAggregationBuilder("percentiles", new double[] { 1, 5, 5, 10 }, null);

        inputAndOutputTypes = TransformAggregations.getAggregationInputAndOutputTypes(percentialAggregationBuilder);
        assertTrue(inputAndOutputTypes.v1().isEmpty());
        outputTypes = inputAndOutputTypes.v2();

        assertEquals(3, outputTypes.size());
        assertEquals("percentiles", outputTypes.get("percentiles.1"));
        assertEquals("percentiles", outputTypes.get("percentiles.5"));
        assertEquals("percentiles", outputTypes.get("percentiles.10"));

        percentialAggregationBuilder = new PercentilesAggregationBuilder("percentiles", new double[] { 1.2, 5.5, 10.7 }, null);

        inputAndOutputTypes = TransformAggregations.getAggregationInputAndOutputTypes(percentialAggregationBuilder);
        assertTrue(inputAndOutputTypes.v1().isEmpty());
        outputTypes = inputAndOutputTypes.v2();

        assertEquals(3, outputTypes.size());
        assertEquals("percentiles", outputTypes.get("percentiles.1_2"));
        assertEquals("percentiles", outputTypes.get("percentiles.5_5"));
        assertEquals("percentiles", outputTypes.get("percentiles.10_7"));
    }

    public void testGetAggregationOutputTypesStats() {
        AggregationBuilder statsAggregationBuilder = new StatsAggregationBuilder("stats");

        Tuple<Map<String, String>, Map<String, String>> inputAndOutputTypes = TransformAggregations.getAggregationInputAndOutputTypes(
            statsAggregationBuilder
        );
        Map<String, String> outputTypes = inputAndOutputTypes.v2();
        assertEquals(5, outputTypes.size());
        assertEquals("stats", outputTypes.get("stats.max"));
        assertEquals("stats", outputTypes.get("stats.min"));
        assertEquals("stats", outputTypes.get("stats.avg"));
        assertEquals("stats", outputTypes.get("stats.count"));
        assertEquals("stats", outputTypes.get("stats.sum"));
    }

    public void testGetAggregationOutputTypesRange() {
        {
            AggregationBuilder rangeAggregationBuilder = new RangeAggregationBuilder("range_agg_name").addUnboundedTo(100)
                .addRange(100, 200)
                .addUnboundedFrom(200);
            var inputAndOutputTypes = TransformAggregations.getAggregationInputAndOutputTypes(rangeAggregationBuilder);
            assertThat(
                inputAndOutputTypes,
                is(
                    equalTo(
                        Tuple.tuple(
                            Map.of(),
                            Map.of("range_agg_name.*-100", "range", "range_agg_name.100-200", "range", "range_agg_name.200-*", "range")
                        )
                    )
                )
            );
        }

        {
            AggregationBuilder rangeAggregationBuilder = new RangeAggregationBuilder("range_agg_name").addUnboundedTo(100.5)
                .addRange(100.5, 200.7)
                .addUnboundedFrom(200.7);
            var inputAndOutputTypes = TransformAggregations.getAggregationInputAndOutputTypes(rangeAggregationBuilder);
            assertThat(
                inputAndOutputTypes,
                is(
                    equalTo(
                        Tuple.tuple(
                            Map.of(),
                            Map.of(
                                "range_agg_name.*-100_5",
                                "range",
                                "range_agg_name.100_5-200_7",
                                "range",
                                "range_agg_name.200_7-*",
                                "range"
                            )
                        )
                    )
                )
            );
        }

        {
            AggregationBuilder rangeAggregationBuilder = new RangeAggregationBuilder("range_agg_name").addUnboundedTo(100.5)
                .addRange(100.5, 200.7)
                .addUnboundedFrom(200.7)
                .subAggregation(AggregationBuilders.avg("my-avg").field("my-field"));
            var inputAndOutputTypes = TransformAggregations.getAggregationInputAndOutputTypes(rangeAggregationBuilder);
            assertThat(
                inputAndOutputTypes,
                is(
                    equalTo(
                        Tuple.tuple(
                            Map.of(
                                "range_agg_name.*-100_5.my-avg",
                                "my-field",
                                "range_agg_name.100_5-200_7.my-avg",
                                "my-field",
                                "range_agg_name.200_7-*.my-avg",
                                "my-field"
                            ),
                            Map.of(
                                "range_agg_name.*-100_5.my-avg",
                                "avg",
                                "range_agg_name.100_5-200_7.my-avg",
                                "avg",
                                "range_agg_name.200_7-*.my-avg",
                                "avg"
                            )
                        )
                    )
                )
            );
        }
    }

    public void testGetAggregationOutputTypesSubAggregations() {

        AggregationBuilder filterAggregationBuilder = new FilterAggregationBuilder("filter_1", new TermQueryBuilder("type", "cat"));
        Tuple<Map<String, String>, Map<String, String>> inputAndOutputTypes = TransformAggregations.getAggregationInputAndOutputTypes(
            filterAggregationBuilder
        );
        assertTrue(inputAndOutputTypes.v1().isEmpty());

        Map<String, String> outputTypes = inputAndOutputTypes.v2();
        assertEquals(1, outputTypes.size());
        assertEquals("filter", outputTypes.get("filter_1"));

        AggregationBuilder subFilterAggregationBuilder = new FilterAggregationBuilder("filter_2", new TermQueryBuilder("subtype", "siam"));
        filterAggregationBuilder.subAggregation(subFilterAggregationBuilder);
        inputAndOutputTypes = TransformAggregations.getAggregationInputAndOutputTypes(filterAggregationBuilder);
        assertTrue(inputAndOutputTypes.v1().isEmpty());

        outputTypes = inputAndOutputTypes.v2();
        assertEquals(1, outputTypes.size());
        assertEquals("filter", outputTypes.get("filter_1.filter_2"));

        filterAggregationBuilder.subAggregation(new MaxAggregationBuilder("max_2").field("max_field"));
        inputAndOutputTypes = TransformAggregations.getAggregationInputAndOutputTypes(filterAggregationBuilder);
        assertEquals(1, inputAndOutputTypes.v1().size());
        Map<String, String> inputTypes = inputAndOutputTypes.v1();
        assertEquals("max_field", inputTypes.get("filter_1.max_2"));

        outputTypes = inputAndOutputTypes.v2();
        assertEquals(2, outputTypes.size());
        assertEquals("filter", outputTypes.get("filter_1.filter_2"));
        assertEquals("max", outputTypes.get("filter_1.max_2"));

        subFilterAggregationBuilder.subAggregation(new FilterAggregationBuilder("filter_3", new TermQueryBuilder("color", "white")));
        inputAndOutputTypes = TransformAggregations.getAggregationInputAndOutputTypes(filterAggregationBuilder);
        assertEquals(1, inputAndOutputTypes.v1().size());

        outputTypes = inputAndOutputTypes.v2();
        assertEquals(2, outputTypes.size());
        assertEquals("filter", outputTypes.get("filter_1.filter_2.filter_3"));
        assertEquals("max", outputTypes.get("filter_1.max_2"));

        subFilterAggregationBuilder.subAggregation(new MinAggregationBuilder("min_3").field("min_field"));
        inputAndOutputTypes = TransformAggregations.getAggregationInputAndOutputTypes(filterAggregationBuilder);
        assertEquals(2, inputAndOutputTypes.v1().size());
        inputTypes = inputAndOutputTypes.v1();
        assertEquals("max_field", inputTypes.get("filter_1.max_2"));
        assertEquals("min_field", inputTypes.get("filter_1.filter_2.min_3"));

        outputTypes = inputAndOutputTypes.v2();
        assertEquals(3, outputTypes.size());
        assertEquals("filter", outputTypes.get("filter_1.filter_2.filter_3"));
        assertEquals("max", outputTypes.get("filter_1.max_2"));
        assertEquals("min", outputTypes.get("filter_1.filter_2.min_3"));

        subFilterAggregationBuilder.subAggregation(
            new PercentilesAggregationBuilder("percentiles", new double[] { 33.3, 44.4, 88.8, 99.5 }, null)
        );
        inputAndOutputTypes = TransformAggregations.getAggregationInputAndOutputTypes(filterAggregationBuilder);
        assertEquals(2, inputAndOutputTypes.v1().size());

        outputTypes = inputAndOutputTypes.v2();
        assertEquals(7, outputTypes.size());
        assertEquals("filter", outputTypes.get("filter_1.filter_2.filter_3"));
        assertEquals("max", outputTypes.get("filter_1.max_2"));
        assertEquals("min", outputTypes.get("filter_1.filter_2.min_3"));
        assertEquals("percentiles", outputTypes.get("filter_1.filter_2.percentiles.33_3"));
        assertEquals("percentiles", outputTypes.get("filter_1.filter_2.percentiles.44_4"));
        assertEquals("percentiles", outputTypes.get("filter_1.filter_2.percentiles.88_8"));
        assertEquals("percentiles", outputTypes.get("filter_1.filter_2.percentiles.99_5"));
    }

    public void testGenerateKeyForRange() {
        assertThat(TransformAggregations.generateKeyForRange(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY), is(equalTo("*-*")));
        assertThat(TransformAggregations.generateKeyForRange(Double.NEGATIVE_INFINITY, 0.0), is(equalTo("*-0")));
        assertThat(TransformAggregations.generateKeyForRange(0.0, 0.0), is(equalTo("0-0")));
        assertThat(TransformAggregations.generateKeyForRange(10.0, 10.0), is(equalTo("10-10")));
        assertThat(TransformAggregations.generateKeyForRange(10.5, 10.5), is(equalTo("10_5-10_5")));
        assertThat(TransformAggregations.generateKeyForRange(10.5, 19.5), is(equalTo("10_5-19_5")));
        assertThat(TransformAggregations.generateKeyForRange(19.5, 20), is(equalTo("19_5-20")));
        assertThat(TransformAggregations.generateKeyForRange(20, Double.POSITIVE_INFINITY), is(equalTo("20-*")));
    }
}
