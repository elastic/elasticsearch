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

package org.elasticsearch.common.unit;

import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class DistanceUnitTests extends ElasticsearchTestCase {

    @Test
    public void testSimpleDistanceUnit() {
        assertThat(DistanceUnit.MILES.toKilometers(10), closeTo(16.09344, 0.001));
        assertThat(DistanceUnit.MILES.toMiles(10), closeTo(10, 0.001));
        assertThat(DistanceUnit.KILOMETERS.toMiles(10), closeTo(6.21371192, 0.001));
        assertThat(DistanceUnit.KILOMETERS.toKilometers(10), closeTo(10, 0.001));
        assertThat(DistanceUnit.METERS.toKilometers(10), closeTo(0.01, 0.00001));
        assertThat(DistanceUnit.METERS.toKilometers(1000), closeTo(1, 0.001));
        assertThat(DistanceUnit.KILOMETERS.toMeters(1), closeTo(1000, 0.001));
    }
    
    @Test
    public void testDistanceUnitParsing() {
        assertThat(DistanceUnit.Distance.parseDistance("50km", null).unit, equalTo(DistanceUnit.KILOMETERS));
        assertThat(DistanceUnit.Distance.parseDistance("500m", null).unit, equalTo(DistanceUnit.METERS));
        assertThat(DistanceUnit.Distance.parseDistance("51mi", null).unit, equalTo(DistanceUnit.MILES));
        assertThat(DistanceUnit.Distance.parseDistance("52yd", null).unit, equalTo(DistanceUnit.YARD));
        assertThat(DistanceUnit.Distance.parseDistance("12in", null).unit, equalTo(DistanceUnit.INCH));
        assertThat(DistanceUnit.Distance.parseDistance("23mm", null).unit, equalTo(DistanceUnit.MILLIMETERS));
        assertThat(DistanceUnit.Distance.parseDistance("23cm", null).unit, equalTo(DistanceUnit.CENTIMETERS));
        
        double testValue = 12345.678;
        for (DistanceUnit unit : DistanceUnit.values()) {
            assertThat("Unit can be parsed from '" + unit.toString() + "'", DistanceUnit.fromString(unit.toString()), equalTo(unit));
            assertThat("Unit can be parsed from '" + testValue + unit.toString() + "'", DistanceUnit.fromString(unit.toString()), equalTo(unit));
            assertThat("Value can be parsed from '" + testValue + unit.toString() + "'", DistanceUnit.Distance.parseDistance(unit.toString(testValue), null).value, equalTo(testValue));
        }
    }

}
