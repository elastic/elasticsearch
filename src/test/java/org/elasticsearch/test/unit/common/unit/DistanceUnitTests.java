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

package org.elasticsearch.test.unit.common.unit;

import org.elasticsearch.common.unit.DistanceUnit;
import org.hamcrest.MatcherAssert;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;

/**
 *
 */
@Test
public class DistanceUnitTests {

    @Test
    void testSimpleDistanceUnit() {
        MatcherAssert.assertThat(DistanceUnit.MILES.toKilometers(10), closeTo(16.09344, 0.001));
        assertThat(DistanceUnit.MILES.toMiles(10), closeTo(10, 0.001));
        assertThat(DistanceUnit.KILOMETERS.toMiles(10), closeTo(6.21371192, 0.001));
        assertThat(DistanceUnit.KILOMETERS.toKilometers(10), closeTo(10, 0.001));
    }
}
