/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms.pivot;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.dataframe.transforms.pivot.Aggregations;

import java.util.HashMap;
import java.util.Map;

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

        // min
        assertEquals("int", Aggregations.resolveTargetMapping("min", "int"));
        assertEquals("double", Aggregations.resolveTargetMapping("min", "double"));
        assertEquals("half_float", Aggregations.resolveTargetMapping("min", "half_float"));

        // sum
        assertEquals("double", Aggregations.resolveTargetMapping("sum", "double"));
        assertEquals("double", Aggregations.resolveTargetMapping("sum", "half_float"));
        assertEquals("double", Aggregations.resolveTargetMapping("sum", null));

        // geo_centroid
        assertEquals("geo_point", Aggregations.resolveTargetMapping("geo_centroid", "geo_point"));
        assertEquals("geo_point", Aggregations.resolveTargetMapping("geo_centroid", null));

        // scripted_metric
        assertEquals("_dynamic", Aggregations.resolveTargetMapping("scripted_metric", null));
        assertEquals("_dynamic", Aggregations.resolveTargetMapping("scripted_metric", "int"));

        // bucket_script
        assertEquals("_dynamic", Aggregations.resolveTargetMapping("bucket_script", null));
        assertEquals("_dynamic", Aggregations.resolveTargetMapping("bucket_script", "int"));

        // weighted_avg
        assertEquals("_dynamic", Aggregations.resolveTargetMapping("weighted_avg", null));
        assertEquals("_dynamic", Aggregations.resolveTargetMapping("weighted_avg", "double"));

        // count
        assertEquals("long", Aggregations.resolveTargetMapping("count", null));
    }

    public void testFilterSpecialAggregations() {
        Map<String, Object> input = asMap(
                "bucket_count",
                    asMap("count",
                            asMap()
                    ));

        Map<String, Object> output = Aggregations.filterSpecialAggregations(input);
        assertEquals(0, output.size());

        input = asMap(
                "bucket_count",
                    asMap("count",
                        asMap()
                            ),
                "avg_price",
                    asMap("avg",
                        asMap("field",
                              "price")
                    ));

        output = Aggregations.filterSpecialAggregations(input);
        assertEquals(1, output.size());

        input = asMap(
                "max_price",
                    asMap("max",
                        asMap("field",
                              "price")
                        ),
                "bucket_count",
                    asMap("count",
                        asMap()
                            ),
                "avg_price",
                    asMap("avg",
                        asMap("field",
                              "price")
                    ));

        output = Aggregations.filterSpecialAggregations(input);
        assertEquals(2, output.size());

    }

    static Map<String, Object> asMap(Object... fields) {
        assert fields.length % 2 == 0;
        final Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < fields.length; i += 2) {
            String field = (String) fields[i];
            map.put(field, fields[i + 1]);
        }
        return map;
    }
}
