/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.unit.index.search.geo;

import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.search.geo.GeoDistance;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 */
@Test
public class GeoDistanceTests {

    @Test
    public void testDistanceCheck() {
        // Note, is within is an approximation, so, even though 0.52 is outside 50mi, we still get "true"
        GeoDistance.DistanceBoundingCheck check = GeoDistance.distanceBoundingCheck(0, 0, 50, DistanceUnit.MILES);
        //System.out.println("Dist: " + GeoDistance.ARC.calculate(0, 0, 0.5, 0.5, DistanceUnit.MILES));
        assertThat(check.isWithin(0.5, 0.5), equalTo(true));
        //System.out.println("Dist: " + GeoDistance.ARC.calculate(0, 0, 0.52, 0.52, DistanceUnit.MILES));
        assertThat(check.isWithin(0.52, 0.52), equalTo(true));
        //System.out.println("Dist: " + GeoDistance.ARC.calculate(0, 0, 1, 1, DistanceUnit.MILES));
        assertThat(check.isWithin(1, 1), equalTo(false));


        check = GeoDistance.distanceBoundingCheck(0, 179, 200, DistanceUnit.MILES);
        //System.out.println("Dist: " + GeoDistance.ARC.calculate(0, 179, 0, -179, DistanceUnit.MILES));
        assertThat(check.isWithin(0, -179), equalTo(true));
        //System.out.println("Dist: " + GeoDistance.ARC.calculate(0, 179, 0, -178, DistanceUnit.MILES));
        assertThat(check.isWithin(0, -178), equalTo(false));
    }
}
