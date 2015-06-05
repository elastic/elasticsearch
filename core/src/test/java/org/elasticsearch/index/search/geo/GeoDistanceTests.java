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

package org.elasticsearch.index.search.geo;

import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 */
public class GeoDistanceTests extends ElasticsearchTestCase {

    @Test
    public void testDistanceCheck() {
        // Note, is within is an approximation, so, even though 0.52 is outside 50mi, we still get "true"
        GeoDistance.DistanceBoundingCheck check = GeoDistance.distanceBoundingCheck(0, 0, 50, DistanceUnit.MILES);
        assertThat(check.isWithin(0.5, 0.5), equalTo(true));
        assertThat(check.isWithin(0.52, 0.52), equalTo(true));
        assertThat(check.isWithin(1, 1), equalTo(false));

        check = GeoDistance.distanceBoundingCheck(0, 179, 200, DistanceUnit.MILES);
        assertThat(check.isWithin(0, -179), equalTo(true));
        assertThat(check.isWithin(0, -178), equalTo(false));
    }

    @Test
    public void testArcDistanceVsPlaneInEllipsis() {
        GeoPoint centre = new GeoPoint(48.8534100, 2.3488000);
        GeoPoint northernPoint = new GeoPoint(48.8801108681, 2.35152032666);
        GeoPoint westernPoint = new GeoPoint(48.85265, 2.308896);

        // With GeoDistance.ARC both the northern and western points are within the 4km range
        assertThat(GeoDistance.ARC.calculate(centre.lat(), centre.lon(), northernPoint.lat(),
                northernPoint.lon(), DistanceUnit.KILOMETERS), lessThan(4D));
        assertThat(GeoDistance.ARC.calculate(centre.lat(), centre.lon(), westernPoint.lat(),
                westernPoint.lon(), DistanceUnit.KILOMETERS), lessThan(4D));

        // With GeoDistance.PLANE, only the northern point is within the 4km range,
        // the western point is outside of the range due to the simple math it employs,
        // meaning results will appear elliptical
        assertThat(GeoDistance.PLANE.calculate(centre.lat(), centre.lon(), northernPoint.lat(),
                northernPoint.lon(), DistanceUnit.KILOMETERS), lessThan(4D));
        assertThat(GeoDistance.PLANE.calculate(centre.lat(), centre.lon(), westernPoint.lat(),
                westernPoint.lon(), DistanceUnit.KILOMETERS), greaterThan(4D));
    }
}
