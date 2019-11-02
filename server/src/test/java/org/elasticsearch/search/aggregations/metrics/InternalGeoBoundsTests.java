/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.geo.GeoTestUtil;
import org.elasticsearch.common.geo.GeoExtent;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.closeTo;

public class InternalGeoBoundsTests extends InternalAggregationTestCase<InternalGeoBounds> {
    static final double GEOHASH_TOLERANCE = 1E-5D;

    @Override
    protected InternalGeoBounds createTestInstance(String name, List<PipelineAggregator> pipelineAggregators,
                                                   Map<String, Object> metaData) {
        return new InternalGeoBounds(name, randomExtent(), randomBoolean(), pipelineAggregators, Collections.emptyMap());
    }

    private GeoExtent randomExtent() {
        GeoExtent extent = new GeoExtent();
        // we occasionally want to test empty extents since this triggers empty xContent object
        if (frequently()) {
            int numPoints = randomIntBetween(2, 100);
            for (int i = 0; i < numPoints; i++) {
                extent.addPoint(GeoTestUtil.nextLatitude(), GeoTestUtil.nextLongitude());
            }
        }
        return extent;
    }

    @Override
    protected void assertReduced(InternalGeoBounds reduced, List<InternalGeoBounds> inputs) {
        GeoExtent extent = new GeoExtent();
        for (InternalGeoBounds bounds : inputs) {
          extent.addExtent(bounds.extent);
        }
        assertValueClose(reduced.extent.maxLat(), extent.maxLat());
        assertValueClose(reduced.extent.minLat(), extent.minLat());
        assertValueClose(reduced.extent.minLon(false), extent.minLon(false));
        assertValueClose(reduced.extent.minLon(true), extent.minLon(true));
        assertValueClose(reduced.extent.maxLon(true), extent.maxLon(true));
        assertValueClose(reduced.extent.maxLon(true), extent.maxLon(true));
    }

    private static void assertValueClose(double expected, double actual) {
        if (Double.isInfinite(expected) == false) {
            assertThat(expected, closeTo(actual, GEOHASH_TOLERANCE));
        } else {
            assertTrue(Double.isInfinite(actual));
        }
    }

    @Override
    protected void assertFromXContent(InternalGeoBounds aggregation, ParsedAggregation parsedAggregation) {
        assertTrue(parsedAggregation instanceof ParsedGeoBounds);
        ParsedGeoBounds parsed = (ParsedGeoBounds) parsedAggregation;

        assertEquals(aggregation.topLeft(), parsed.topLeft());
        assertEquals(aggregation.bottomRight(), parsed.bottomRight());
    }

    @Override
    protected Writeable.Reader<InternalGeoBounds> instanceReader() {
        return InternalGeoBounds::new;
    }

    @Override
    protected InternalGeoBounds mutateInstance(InternalGeoBounds instance) {
        String name = instance.getName();
        GeoExtent extent = new GeoExtent();
        extent.addExtent(instance.extent);
        boolean wrapLongitude = instance.wrapLongitude;
        List<PipelineAggregator> pipelineAggregators = instance.pipelineAggregators();
        Map<String, Object> metaData = instance.getMetaData();
        switch (between(0, 3)) {
        case 0:
            name += randomAlphaOfLength(5);
            break;
        case 1:
            // we reset the extent to make sure we get a new extent
            extent = new GeoExtent();
            do {
                extent.addPoint(GeoTestUtil.nextLatitude(), GeoTestUtil.nextLongitude());
            } while (extent.equals(instance.extent));
            break;
        case 2:
            wrapLongitude = wrapLongitude == false;
            break;
        case 3:
            if (metaData == null) {
                metaData = new HashMap<>(1);
            } else {
                metaData = new HashMap<>(instance.getMetaData());
            }
            metaData.put(randomAlphaOfLength(15), randomInt());
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
       }
        return new InternalGeoBounds(name, extent, wrapLongitude, pipelineAggregators, metaData);
    }
}
