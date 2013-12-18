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

package org.apache.lucene.util;

import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.number.IsCloseTo.closeTo;

public class SloppyMathTests extends ElasticsearchTestCase {    

    @Test
    public void testAccuracy() {
        for (double lat1 = -89; lat1 <= 89; lat1+=1) {
            final double lon1 = randomLongitude();

            for (double i = -180; i <= 180; i+=1) {
                final double lon2 = i;
                final double lat2 = randomLatitude();

                assertAccurate(lat1, lon1, lat2, lon2);
            }
        }
    }

    @Test
    public void testSloppyMath() {
        assertThat(GeoDistance.SLOPPY_ARC.calculate(-46.645, -171.057, -46.644, -171.058, DistanceUnit.METERS), closeTo(134.87709, maxError(134.87709)));
        assertThat(GeoDistance.SLOPPY_ARC.calculate(-77.912, -81.173, -77.912, -81.171, DistanceUnit.METERS), closeTo(46.57161, maxError(46.57161)));
        assertThat(GeoDistance.SLOPPY_ARC.calculate(65.75, -20.708, 65.75, -20.709, DistanceUnit.METERS), closeTo(45.66996, maxError(45.66996)));
        assertThat(GeoDistance.SLOPPY_ARC.calculate(-86.9, 53.738, -86.9, 53.741, DistanceUnit.METERS), closeTo(18.03998, maxError(18.03998)));
        assertThat(GeoDistance.SLOPPY_ARC.calculate(89.041, 115.93, 89.04, 115.946, DistanceUnit.METERS), closeTo(115.11711, maxError(115.11711)));

        testSloppyMath(DistanceUnit.METERS, 0.01, 5, 45, 90);
        testSloppyMath(DistanceUnit.KILOMETERS, 0.01, 5, 45, 90);
        testSloppyMath(DistanceUnit.INCH, 0.01, 5, 45, 90);
        testSloppyMath(DistanceUnit.MILES, 0.01, 5, 45, 90);
    }

    private static double maxError(double distance) {
        return distance / 1000.0;
    }
    
    private void testSloppyMath(DistanceUnit unit, double...deltaDeg) {
        final double lat1 = randomLatitude();
        final double lon1 = randomLongitude();
        logger.info("testing SloppyMath with {} at \"{}, {}\"", unit, lat1, lon1);

        for (int test = 0; test < deltaDeg.length; test++) {
            for (int i = 0; i < 100; i++) {
                // crop pole areas, sine we now there the function
                // is not accurate around lat(89°, 90°) and lat(-90°, -89°)
                final double lat2 = Math.max(-89.0, Math.min(+89.0, lat1 + (randomDouble() - 0.5) * 2 * deltaDeg[test]));
                final double lon2 = lon1 + (randomDouble() - 0.5) * 2 * deltaDeg[test];

                final double accurate = GeoDistance.ARC.calculate(lat1, lon1, lat2, lon2, unit);
                final double dist = GeoDistance.SLOPPY_ARC.calculate(lat1, lon1, lat2, lon2, unit);
    
                assertThat("distance between("+lat1+", "+lon1+") and ("+lat2+", "+lon2+"))", dist, closeTo(accurate, maxError(accurate)));
            }
        }
    }
        
    private static void assertAccurate(double lat1, double lon1, double lat2, double lon2) {
        double accurate = GeoDistance.ARC.calculate(lat1, lon1, lat2, lon2, DistanceUnit.METERS);
        double sloppy = GeoDistance.SLOPPY_ARC.calculate(lat1, lon1, lat2, lon2, DistanceUnit.METERS);
        assertThat("distance between("+lat1+", "+lon1+") and ("+lat2+", "+lon2+"))", sloppy, closeTo(accurate, maxError(accurate)));
    }

    private static final double randomLatitude() {
        // crop pole areas, sine we now there the function
        // is not accurate around lat(89°, 90°) and lat(-90°, -89°)
        return (getRandom().nextDouble() - 0.5) * 178.0;
    }

    private static final double randomLongitude() {
        return (getRandom().nextDouble() - 0.5) * 360.0;
    }
}
