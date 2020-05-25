/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms.pivot;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.matrix.MatrixAggregationPlugin;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AggregationsTests extends ESTestCase {
    public void testResolveTargetMapping() {

        // avg
        assertEquals("double", Aggregations.resolveTargetMapping("avg", "int"));
        assertEquals("double", Aggregations.resolveTargetMapping("avg", "double"));

        // cardinality
        assertEquals("long", Aggregations.resolveTargetMapping("cardinality", "int"));
        assertEquals("long", Aggregations.resolveTargetMapping("cardinality", "double"));

        // value_count
        assertEquals("long", Aggregations.resolveTargetMapping("value_count", "int"));
        assertEquals("long", Aggregations.resolveTargetMapping("value_count", "double"));

        // max
        assertEquals("int", Aggregations.resolveTargetMapping("max", "int"));
        assertEquals("double", Aggregations.resolveTargetMapping("max", "double"));
        assertEquals("half_float", Aggregations.resolveTargetMapping("max", "half_float"));
        assertEquals("float", Aggregations.resolveTargetMapping("max", "scaled_float"));

        // min
        assertEquals("int", Aggregations.resolveTargetMapping("min", "int"));
        assertEquals("double", Aggregations.resolveTargetMapping("min", "double"));
        assertEquals("half_float", Aggregations.resolveTargetMapping("min", "half_float"));
        assertEquals("float", Aggregations.resolveTargetMapping("min", "scaled_float"));

        // sum
        assertEquals("double", Aggregations.resolveTargetMapping("sum", "double"));
        assertEquals("double", Aggregations.resolveTargetMapping("sum", "half_float"));
        assertEquals("double", Aggregations.resolveTargetMapping("sum", null));

        // geo_centroid
        assertEquals("geo_point", Aggregations.resolveTargetMapping("geo_centroid", "geo_point"));
        assertEquals("geo_point", Aggregations.resolveTargetMapping("geo_centroid", null));

        // geo_bounds
        assertEquals("geo_shape", Aggregations.resolveTargetMapping("geo_bounds", "geo_shape"));
        assertEquals("geo_shape", Aggregations.resolveTargetMapping("geo_bounds", null));

        // scripted_metric
        assertEquals("_dynamic", Aggregations.resolveTargetMapping("scripted_metric", null));
        assertEquals("_dynamic", Aggregations.resolveTargetMapping("scripted_metric", "int"));

        // bucket_script
        assertEquals("_dynamic", Aggregations.resolveTargetMapping("bucket_script", null));
        assertEquals("_dynamic", Aggregations.resolveTargetMapping("bucket_script", "int"));

        // bucket_selector
        assertEquals("_dynamic", Aggregations.resolveTargetMapping("bucket_selector", null));
        assertEquals("_dynamic", Aggregations.resolveTargetMapping("bucket_selector", "int"));

        // weighted_avg
        assertEquals("_dynamic", Aggregations.resolveTargetMapping("weighted_avg", null));
        assertEquals("_dynamic", Aggregations.resolveTargetMapping("weighted_avg", "double"));

        // percentile
        assertEquals("double", Aggregations.resolveTargetMapping("percentiles", null));
        assertEquals("double", Aggregations.resolveTargetMapping("percentiles", "int"));

        // filter
        assertEquals("long", Aggregations.resolveTargetMapping("filter", null));
        assertEquals("long", Aggregations.resolveTargetMapping("filter", "long"));
        assertEquals("long", Aggregations.resolveTargetMapping("filter", "double"));

        // terms
        assertEquals("flattened", Aggregations.resolveTargetMapping("terms", null));
        assertEquals("flattened", Aggregations.resolveTargetMapping("terms", "keyword"));
        assertEquals("flattened", Aggregations.resolveTargetMapping("terms", "text"));

        // rare_terms
        assertEquals("flattened", Aggregations.resolveTargetMapping("rare_terms", null));
        assertEquals("flattened", Aggregations.resolveTargetMapping("rare_terms", "text"));
        assertEquals("flattened", Aggregations.resolveTargetMapping("rare_terms", "keyword"));

        // corner case: source type null
        assertEquals(null, Aggregations.resolveTargetMapping("min", null));
    }

    public void testAggregationsVsTransforms() {
        // Note: if a new plugin is added, it must be added here
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Arrays.asList((new AnalyticsPlugin()), new MatrixAggregationPlugin()));
        List<NamedWriteableRegistry.Entry> namedWriteables = searchModule.getNamedWriteables();

        List<String> aggregationNames = namedWriteables.stream()
            .filter(namedWritable -> namedWritable.categoryClass.equals(AggregationBuilder.class))
            .map(namedWritable -> namedWritable.name)
            .collect(Collectors.toList());

        for (String aggregationName : aggregationNames) {
            assertTrue(
                "The following aggregation is unknown to transform: ["
                    + aggregationName
                    + "]. If this is a newly added aggregation, "
                    + "please open an issue to add transform support for it. Afterwards add \""
                    + aggregationName
                    + "\" to the list in "
                    + Aggregations.class.getName()
                    + ". Thanks!",

                Aggregations.isSupportedByTransform(aggregationName) || Aggregations.isUnSupportedByTransform(aggregationName)
            );
        }
    }

    public void testGetAggregationOutputTypesPercentiles() {
        AggregationBuilder percentialAggregationBuilder = new PercentilesAggregationBuilder("percentiles", new double[] { 1, 5, 10 }, null);

        Tuple<Map<String, String>, Map<String, String>> inputAndOutputTypes = Aggregations.getAggregationInputAndOutputTypes(
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

        inputAndOutputTypes = Aggregations.getAggregationInputAndOutputTypes(percentialAggregationBuilder);
        assertTrue(inputAndOutputTypes.v1().isEmpty());
        outputTypes = inputAndOutputTypes.v2();

        assertEquals(3, outputTypes.size());
        assertEquals("percentiles", outputTypes.get("percentiles.1"));
        assertEquals("percentiles", outputTypes.get("percentiles.5"));
        assertEquals("percentiles", outputTypes.get("percentiles.10"));
    }

    public void testGetAggregationOutputTypesSubAggregations() {

        AggregationBuilder filterAggregationBuilder = new FilterAggregationBuilder("filter_1", new TermQueryBuilder("type", "cat"));
        Tuple<Map<String, String>, Map<String, String>> inputAndOutputTypes = Aggregations.getAggregationInputAndOutputTypes(
            filterAggregationBuilder
        );
        assertTrue(inputAndOutputTypes.v1().isEmpty());

        Map<String, String> outputTypes = inputAndOutputTypes.v2();
        assertEquals(1, outputTypes.size());
        assertEquals("filter", outputTypes.get("filter_1"));

        AggregationBuilder subFilterAggregationBuilder = new FilterAggregationBuilder("filter_2", new TermQueryBuilder("subtype", "siam"));
        filterAggregationBuilder.subAggregation(subFilterAggregationBuilder);
        inputAndOutputTypes = Aggregations.getAggregationInputAndOutputTypes(filterAggregationBuilder);
        assertTrue(inputAndOutputTypes.v1().isEmpty());

        outputTypes = inputAndOutputTypes.v2();
        assertEquals(1, outputTypes.size());
        assertEquals("filter", outputTypes.get("filter_1.filter_2"));

        filterAggregationBuilder.subAggregation(new MaxAggregationBuilder("max_2").field("max_field"));
        inputAndOutputTypes = Aggregations.getAggregationInputAndOutputTypes(filterAggregationBuilder);
        assertEquals(1, inputAndOutputTypes.v1().size());
        Map<String, String> inputTypes = inputAndOutputTypes.v1();
        assertEquals("max_field", inputTypes.get("filter_1.max_2"));

        outputTypes = inputAndOutputTypes.v2();
        assertEquals(2, outputTypes.size());
        assertEquals("filter", outputTypes.get("filter_1.filter_2"));
        assertEquals("max", outputTypes.get("filter_1.max_2"));

        subFilterAggregationBuilder.subAggregation(new FilterAggregationBuilder("filter_3", new TermQueryBuilder("color", "white")));
        inputAndOutputTypes = Aggregations.getAggregationInputAndOutputTypes(filterAggregationBuilder);
        assertEquals(1, inputAndOutputTypes.v1().size());

        outputTypes = inputAndOutputTypes.v2();
        assertEquals(2, outputTypes.size());
        assertEquals("filter", outputTypes.get("filter_1.filter_2.filter_3"));
        assertEquals("max", outputTypes.get("filter_1.max_2"));

        subFilterAggregationBuilder.subAggregation(new MinAggregationBuilder("min_3").field("min_field"));
        inputAndOutputTypes = Aggregations.getAggregationInputAndOutputTypes(filterAggregationBuilder);
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
        inputAndOutputTypes = Aggregations.getAggregationInputAndOutputTypes(filterAggregationBuilder);
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
}
