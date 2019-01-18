/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms.pivot;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.dataframe.transforms.pivot.Aggregations;

public class AggregationsTests extends ESTestCase {
    public void testResolveTargetMapping() {

        // avg
        assertEquals("double", Aggregations.resolveTargetMapping("avg", "int"));
        assertEquals("double", Aggregations.resolveTargetMapping("avg", "double"));

        // max
        assertEquals("int", Aggregations.resolveTargetMapping("max", "int"));
        assertEquals("double", Aggregations.resolveTargetMapping("max", "double"));
        assertEquals("half_float", Aggregations.resolveTargetMapping("max", "half_float"));
    }
}
