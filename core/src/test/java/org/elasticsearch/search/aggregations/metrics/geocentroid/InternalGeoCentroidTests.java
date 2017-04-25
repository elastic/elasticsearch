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
package org.elasticsearch.search.aggregations.metrics.geocentroid;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.geo.RandomGeoGenerator;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class InternalGeoCentroidTests extends InternalAggregationTestCase<InternalGeoCentroid> {

    @Override
    protected InternalGeoCentroid createTestInstance(String name, List<PipelineAggregator> pipelineAggregators,
                                                     Map<String, Object> metaData) {
        GeoPoint centroid = RandomGeoGenerator.randomPoint(random());

        // Re-encode lat/longs to avoid rounding issue when testing InternalGeoCentroid#hashCode() and
        // InternalGeoCentroid#equals()
        int encodedLon = GeoEncodingUtils.encodeLongitude(centroid.lon());
        centroid.resetLon(GeoEncodingUtils.decodeLongitude(encodedLon));
        int encodedLat = GeoEncodingUtils.encodeLatitude(centroid.lat());
        centroid.resetLat(GeoEncodingUtils.decodeLatitude(encodedLat));

        return new InternalGeoCentroid("_name", centroid, 1, Collections.emptyList(), Collections.emptyMap());
    }

    @Override
    protected Writeable.Reader<InternalGeoCentroid> instanceReader() {
        return InternalGeoCentroid::new;
    }

    @Override
    protected void assertReduced(InternalGeoCentroid reduced, List<InternalGeoCentroid> inputs) {
        GeoPoint expected = new GeoPoint(0, 0);
        int i = 0;
        for (InternalGeoCentroid input : inputs) {
            expected.reset(expected.lat() + (input.centroid().lat() - expected.lat()) / (i+1),
                    expected.lon() + (input.centroid().lon() - expected.lon()) / (i+1));
            i++;
        }
        assertEquals(expected.getLat(), reduced.centroid().getLat(), 1E-5D);
        assertEquals(expected.getLon(), reduced.centroid().getLon(), 1E-5D);
    }
}
