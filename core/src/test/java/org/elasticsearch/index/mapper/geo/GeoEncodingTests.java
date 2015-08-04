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

package org.elasticsearch.index.mapper.geo;

import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.DistanceUnit.Distance;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

import static org.hamcrest.Matchers.lessThanOrEqualTo;


public class GeoEncodingTests extends ESTestCase {

    public void test() {
        for (int i = 0; i < 10000; ++i) {
            final double lat = randomDouble() * 180 - 90;
            final double lon = randomDouble() * 360 - 180;
            final Distance precision = new Distance(1+(randomDouble() * 9), randomFrom(Arrays.asList(DistanceUnit.MILLIMETERS, DistanceUnit.METERS, DistanceUnit.KILOMETERS)));
            final GeoPointFieldMapper.Encoding encoding = GeoPointFieldMapper.Encoding.of(precision);
            assertThat(encoding.precision().convert(DistanceUnit.METERS).value, lessThanOrEqualTo(precision.convert(DistanceUnit.METERS).value));
            final GeoPoint geoPoint = encoding.decode(encoding.encodeCoordinate(lat), encoding.encodeCoordinate(lon), new GeoPoint());
            final double error = GeoDistance.PLANE.calculate(lat, lon, geoPoint.lat(), geoPoint.lon(), DistanceUnit.METERS);
            assertThat(error, lessThanOrEqualTo(precision.convert(DistanceUnit.METERS).value));
        }
    }

}
