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

package org.elasticsearch.common.unit;

import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class DistanceUnitTests extends ElasticsearchTestCase {

    @Test
    public void testSimpleDistanceUnit() {
        assertThat(DistanceUnit.KILOMETERS.convert(10, DistanceUnit.MILES), closeTo(16.09344, 0.001));
        assertThat(DistanceUnit.MILES.convert(10, DistanceUnit.MILES), closeTo(10, 0.001));
        assertThat(DistanceUnit.MILES.convert(10, DistanceUnit.KILOMETERS), closeTo(6.21371192, 0.001));
        assertThat(DistanceUnit.NAUTICALMILES.convert(10, DistanceUnit.MILES), closeTo(8.689762, 0.001));
        assertThat(DistanceUnit.KILOMETERS.convert(10, DistanceUnit.KILOMETERS), closeTo(10, 0.001));
        assertThat(DistanceUnit.KILOMETERS.convert(10, DistanceUnit.METERS), closeTo(0.01, 0.00001));
        assertThat(DistanceUnit.KILOMETERS.convert(1000,DistanceUnit.METERS), closeTo(1, 0.001));
        assertThat(DistanceUnit.METERS.convert(1, DistanceUnit.KILOMETERS), closeTo(1000, 0.001));
    }
    
    @Test
    public void testDistanceUnitParsing() {
        assertThat(DistanceUnit.Distance.parseDistance("50km").unit, equalTo(DistanceUnit.KILOMETERS));
        assertThat(DistanceUnit.Distance.parseDistance("500m").unit, equalTo(DistanceUnit.METERS));
        assertThat(DistanceUnit.Distance.parseDistance("51mi").unit, equalTo(DistanceUnit.MILES));
        assertThat(DistanceUnit.Distance.parseDistance("53nmi").unit, equalTo(DistanceUnit.NAUTICALMILES));
        assertThat(DistanceUnit.Distance.parseDistance("53NM").unit, equalTo(DistanceUnit.NAUTICALMILES));
        assertThat(DistanceUnit.Distance.parseDistance("52yd").unit, equalTo(DistanceUnit.YARD));
        assertThat(DistanceUnit.Distance.parseDistance("12in").unit, equalTo(DistanceUnit.INCH));
        assertThat(DistanceUnit.Distance.parseDistance("23mm").unit, equalTo(DistanceUnit.MILLIMETERS));
        assertThat(DistanceUnit.Distance.parseDistance("23cm").unit, equalTo(DistanceUnit.CENTIMETERS));
        
        double testValue = 12345.678;
        for (DistanceUnit unit : DistanceUnit.values()) {
            assertThat("Unit can be parsed from '" + unit.toString() + "'", DistanceUnit.fromString(unit.toString()), equalTo(unit));
            assertThat("Unit can be parsed from '" + testValue + unit.toString() + "'", DistanceUnit.fromString(unit.toString()), equalTo(unit));
            assertThat("Value can be parsed from '" + testValue + unit.toString() + "'", DistanceUnit.Distance.parseDistance(unit.toString(testValue)).value, equalTo(testValue));
        }
    }

}
