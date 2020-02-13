/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms.pivot;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.matrix.MatrixAggregationPlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;

import java.util.Arrays;
import java.util.List;
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
    }

    public void testAggregationsVsTransforms() {
        // Note: if a new plugin is added, it must be added here
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Arrays.asList(new AnalyticsPlugin(Settings.EMPTY),
            new MatrixAggregationPlugin()));
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
}
