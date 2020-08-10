/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics.datasketches.hll;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.Union;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;

public class InternalHllSketchAggregationTests extends ESTestCase {
    public void testConstruct() {
        final HllSketch hllSketch = new HllSketch(15);
        hllSketch.update(1);
        final String internalName = "test_construct";
        final HashMap<String, Object> meta = new HashMap<String, Object>();
        final InternalHllSketchAggregation internalHllSketchAggregation = new InternalHllSketchAggregation(internalName, hllSketch, meta);
        assertSame(Math.round(hllSketch.getEstimate()), internalHllSketchAggregation.getValue());
    }

    public void testGetValueHllSketchNull() {
        final String internalName = "test_get_value_hll_sketch_null";
        final HashMap<String, Object> meta = new HashMap<String, Object>();
        final InternalHllSketchAggregation internalHllSketchAggregation = new InternalHllSketchAggregation(internalName, null, meta);
        assertSame(0L, internalHllSketchAggregation.getValue());
    }

    public void testReduce() {
        final HllSketch hllSketch1 = new HllSketch(15);
        hllSketch1.update(1);
        final HllSketch hllSketch2 = new HllSketch(15);
        hllSketch2.update(8);
        final HllSketch hllSketch3 = new HllSketch(15);
        hllSketch3.update(1);
        final Union hllUnion = new Union(15);
        hllUnion.update(hllSketch1);
        hllUnion.update(hllSketch2);
        hllUnion.update(hllSketch3);

        final String internalName = "test_reduce";
        final HashMap<String, Object> meta = new HashMap<String, Object>();

        final InternalHllSketchAggregation internalHllSketchAggregation1 = new InternalHllSketchAggregation(internalName, hllSketch1, meta);
        final InternalHllSketchAggregation internalHllSketchAggregation2 = new InternalHllSketchAggregation(internalName, hllSketch2, meta);
        final InternalHllSketchAggregation internalHllSketchAggregation3 = new InternalHllSketchAggregation(internalName, hllSketch3, meta);

        final ArrayList<InternalAggregation> aggregations = new ArrayList<>();
        aggregations.add(internalHllSketchAggregation2);
        aggregations.add(internalHllSketchAggregation3);

        final InternalHllSketchAggregation result = (InternalHllSketchAggregation) internalHllSketchAggregation1.reduce(aggregations, null);

        assertSame(Math.round(hllUnion.getEstimate()), result.getValue());
    }

    public void testEquals() {
        final HllSketch hllSketch = new HllSketch(15);
        hllSketch.update(1);
        final HllSketch hllSketchDiff = new HllSketch(15);
        hllSketchDiff.update(1);

        final String internalName = "test_equals";
        final HashMap<String, Object> meta = new HashMap<String, Object>();

        final InternalHllSketchAggregation internalHllSketchAggregation1 = new InternalHllSketchAggregation(internalName, hllSketch, meta);
        final InternalHllSketchAggregation internalHllSketchAggregation2 = new InternalHllSketchAggregation(internalName, hllSketch, meta);
        final InternalHllSketchAggregation internalHllSketchAggregationDiff = new InternalHllSketchAggregation(internalName, hllSketchDiff, meta);

        assertTrue(internalHllSketchAggregation1.equals(internalHllSketchAggregation1));
        assertTrue(internalHllSketchAggregation1.equals(internalHllSketchAggregation2));
        assertFalse(internalHllSketchAggregation1.equals(null));
        assertFalse(internalHllSketchAggregation1.equals(Integer.MAX_VALUE));
        assertFalse(internalHllSketchAggregation1.equals(internalHllSketchAggregationDiff));
    }

}
