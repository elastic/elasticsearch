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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.elasticsearch.index.search.geo.GeoUtils;
import org.elasticsearch.index.search.geo.Point;
import org.elasticsearch.index.search.geo.Range;
import org.testng.annotations.Test;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 *
 */
@Test
public class GeoUtilsTests {

    /**
     * Test special values like inf, NaN and -0.0.
     */
    @Test
    public void testSpecials() {
        assertThat(GeoUtils.normalizeLon(Double.POSITIVE_INFINITY), equalTo(Double.NaN));
        assertThat(GeoUtils.normalizeLat(Double.POSITIVE_INFINITY), equalTo(Double.NaN));
        assertThat(GeoUtils.normalizeLon(Double.NEGATIVE_INFINITY), equalTo(Double.NaN));
        assertThat(GeoUtils.normalizeLat(Double.NEGATIVE_INFINITY), equalTo(Double.NaN));
        assertThat(GeoUtils.normalizeLon(Double.NaN), equalTo(Double.NaN));
        assertThat(GeoUtils.normalizeLat(Double.NaN), equalTo(Double.NaN));
        assertThat(0.0, not(equalTo(-0.0)));
        assertThat(GeoUtils.normalizeLon(-0.0), equalTo(0.0));
        assertThat(GeoUtils.normalizeLat(-0.0), equalTo(0.0));
        assertThat(GeoUtils.normalizeLon(0.0), equalTo(0.0));
        assertThat(GeoUtils.normalizeLat(0.0), equalTo(0.0));
    }

    /**
     * Test bounding values.
     */
    @Test
    public void testBounds() {
        assertThat(GeoUtils.normalizeLon(-360.0), equalTo(0.0));
        assertThat(GeoUtils.normalizeLat(-180.0), equalTo(0.0));
        assertThat(GeoUtils.normalizeLon(360.0), equalTo(0.0));
        assertThat(GeoUtils.normalizeLat(180.0), equalTo(0.0));
        // and halves
        assertThat(GeoUtils.normalizeLon(-180.0), equalTo(180.0));
        assertThat(GeoUtils.normalizeLat(-90.0), equalTo(-90.0));
        assertThat(GeoUtils.normalizeLon(180.0), equalTo(180.0));
        assertThat(GeoUtils.normalizeLat(90.0), equalTo(90.0));
    }

    /**
     * Test normal values.
     */
    @Test
    public void testNormal() {
        // Near bounds
        assertThat(GeoUtils.normalizeLon(-360.5), equalTo(-0.5));
        assertThat(GeoUtils.normalizeLat(-180.5), equalTo(0.5));
        assertThat(GeoUtils.normalizeLon(360.5), equalTo(0.5));
        assertThat(GeoUtils.normalizeLat(180.5), equalTo(-0.5));
        // and near halves
        assertThat(GeoUtils.normalizeLon(-180.5), equalTo(179.5));
        assertThat(GeoUtils.normalizeLat(-90.5), equalTo(-89.5));
        assertThat(GeoUtils.normalizeLon(180.5), equalTo(-179.5));
        assertThat(GeoUtils.normalizeLat(90.5), equalTo(89.5));
        // Now with points, to check for longitude shifting with latitude normalization
        assertNormalizedPoint(new Point(0, 0), new Point(0, 0));
        assertNormalizedPoint(new Point(0, -180), new Point(0, 180));
        assertNormalizedPoint(new Point(0, 180), new Point(0, 180));
        assertNormalizedPoint(new Point(-90, 0), new Point(-90, 0));
        assertNormalizedPoint(new Point(90, 0), new Point(90, 0));
        // Every 10-units, multiple full turns
        for (int shift = -20; shift <= 20; ++shift) {
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 0.0), equalTo(0.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 10.0), equalTo(10.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 20.0), equalTo(20.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 30.0), equalTo(30.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 40.0), equalTo(40.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 50.0), equalTo(50.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 60.0), equalTo(60.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 70.0), equalTo(70.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 80.0), equalTo(80.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 90.0), equalTo(90.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 100.0), equalTo(100.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 110.0), equalTo(110.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 120.0), equalTo(120.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 130.0), equalTo(130.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 140.0), equalTo(140.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 150.0), equalTo(150.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 160.0), equalTo(160.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 170.0), equalTo(170.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 180.0), equalTo(180.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 190.0), equalTo(-170.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 200.0), equalTo(-160.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 210.0), equalTo(-150.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 220.0), equalTo(-140.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 230.0), equalTo(-130.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 240.0), equalTo(-120.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 250.0), equalTo(-110.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 260.0), equalTo(-100.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 270.0), equalTo(-90.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 280.0), equalTo(-80.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 290.0), equalTo(-70.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 300.0), equalTo(-60.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 310.0), equalTo(-50.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 320.0), equalTo(-40.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 330.0), equalTo(-30.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 340.0), equalTo(-20.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 350.0), equalTo(-10.0));
            assertThat(GeoUtils.normalizeLon(shift * 360.0 + 360.0), equalTo(0.0));
        }
        for (int shift = -20; shift <= 20; ++shift) {
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 0.0), equalTo(0.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 10.0), equalTo(10.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 20.0), equalTo(20.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 30.0), equalTo(30.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 40.0), equalTo(40.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 50.0), equalTo(50.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 60.0), equalTo(60.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 70.0), equalTo(70.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 80.0), equalTo(80.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 90.0), equalTo(90.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 100.0), equalTo(80.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 110.0), equalTo(70.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 120.0), equalTo(60.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 130.0), equalTo(50.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 140.0), equalTo(40.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 150.0), equalTo(30.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 160.0), equalTo(20.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 170.0), equalTo(10.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 180.0), equalTo(0.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 190.0), equalTo(-10.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 200.0), equalTo(-20.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 210.0), equalTo(-30.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 220.0), equalTo(-40.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 230.0), equalTo(-50.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 240.0), equalTo(-60.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 250.0), equalTo(-70.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 260.0), equalTo(-80.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 270.0), equalTo(-90.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 280.0), equalTo(-80.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 290.0), equalTo(-70.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 300.0), equalTo(-60.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 310.0), equalTo(-50.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 320.0), equalTo(-40.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 330.0), equalTo(-30.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 340.0), equalTo(-20.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 350.0), equalTo(-10.0));
            assertThat(GeoUtils.normalizeLat(shift * 360.0 + 360.0), equalTo(0.0));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 0.0), equalTo(false));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 10.0), equalTo(false));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 20.0), equalTo(false));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 30.0), equalTo(false));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 40.0), equalTo(false));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 50.0), equalTo(false));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 60.0), equalTo(false));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 70.0), equalTo(false));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 80.0), equalTo(false));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 90.0), equalTo(false));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 100.0), equalTo(true));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 110.0), equalTo(true));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 120.0), equalTo(true));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 130.0), equalTo(true));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 140.0), equalTo(true));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 150.0), equalTo(true));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 160.0), equalTo(true));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 170.0), equalTo(true));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 180.0), equalTo(true));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 190.0), equalTo(true));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 200.0), equalTo(true));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 210.0), equalTo(true));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 220.0), equalTo(true));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 230.0), equalTo(true));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 240.0), equalTo(true));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 250.0), equalTo(true));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 260.0), equalTo(true));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 270.0), equalTo(false));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 280.0), equalTo(false));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 290.0), equalTo(false));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 300.0), equalTo(false));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 310.0), equalTo(false));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 320.0), equalTo(false));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 330.0), equalTo(false));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 340.0), equalTo(false));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 350.0), equalTo(false));
            assertThat(GeoUtils.normalizingLatImpliesShift(shift * 360.0 + 360.0), equalTo(false));
        }
    }

    /**
     * Test huge values.
     */
    @Test
    public void testHuge() {
        assertThat(GeoUtils.normalizeLon(-36000000000181.0), equalTo(GeoUtils.normalizeLon(-181.0)));
        assertThat(GeoUtils.normalizeLon(-36000000000180.0), equalTo(GeoUtils.normalizeLon(-180.0)));
        assertThat(GeoUtils.normalizeLon(-36000000000179.0), equalTo(GeoUtils.normalizeLon(-179.0)));
        assertThat(GeoUtils.normalizeLon(-36000000000178.0), equalTo(GeoUtils.normalizeLon(-178.0)));
        assertThat(GeoUtils.normalizeLon(-36000000000001.0), equalTo(GeoUtils.normalizeLon(-001.0)));
        assertThat(GeoUtils.normalizeLon(+36000000000000.0), equalTo(GeoUtils.normalizeLon(+000.0)));
        assertThat(GeoUtils.normalizeLon(+36000000000001.0), equalTo(GeoUtils.normalizeLon(+001.0)));
        assertThat(GeoUtils.normalizeLon(+36000000000002.0), equalTo(GeoUtils.normalizeLon(+002.0)));
        assertThat(GeoUtils.normalizeLon(+36000000000178.0), equalTo(GeoUtils.normalizeLon(+178.0)));
        assertThat(GeoUtils.normalizeLon(+36000000000179.0), equalTo(GeoUtils.normalizeLon(+179.0)));
        assertThat(GeoUtils.normalizeLon(+36000000000180.0), equalTo(GeoUtils.normalizeLon(+180.0)));
        assertThat(GeoUtils.normalizeLon(+36000000000181.0), equalTo(GeoUtils.normalizeLon(+181.0)));
        assertThat(GeoUtils.normalizeLat(-18000000000091.0), equalTo(GeoUtils.normalizeLat(-091.0)));
        assertThat(GeoUtils.normalizeLat(-18000000000090.0), equalTo(GeoUtils.normalizeLat(-090.0)));
        assertThat(GeoUtils.normalizeLat(-18000000000089.0), equalTo(GeoUtils.normalizeLat(-089.0)));
        assertThat(GeoUtils.normalizeLat(-18000000000088.0), equalTo(GeoUtils.normalizeLat(-088.0)));
        assertThat(GeoUtils.normalizeLat(-18000000000001.0), equalTo(GeoUtils.normalizeLat(-001.0)));
        assertThat(GeoUtils.normalizeLat(+18000000000000.0), equalTo(GeoUtils.normalizeLat(+000.0)));
        assertThat(GeoUtils.normalizeLat(+18000000000001.0), equalTo(GeoUtils.normalizeLat(+001.0)));
        assertThat(GeoUtils.normalizeLat(+18000000000002.0), equalTo(GeoUtils.normalizeLat(+002.0)));
        assertThat(GeoUtils.normalizeLat(+18000000000088.0), equalTo(GeoUtils.normalizeLat(+088.0)));
        assertThat(GeoUtils.normalizeLat(+18000000000089.0), equalTo(GeoUtils.normalizeLat(+089.0)));
        assertThat(GeoUtils.normalizeLat(+18000000000090.0), equalTo(GeoUtils.normalizeLat(+090.0)));
        assertThat(GeoUtils.normalizeLat(+18000000000091.0), equalTo(GeoUtils.normalizeLat(+091.0)));
    }

    @Test
    public void testRanges() {
        assertNormalizedRange("valid singleton", new Range(10, 20, 10, 20),  new Range(10, 20, 10, 20));
        assertNormalizedRange("valid range", new Range(89, -179, -89, 179),  new Range(89, -179, -89, 179));

        assertNormalizedRange("whole world", new Range(90, -180, -90, 180),  new Range(90, -180, -90, 180));
        assertNormalizedRange("whole latitude", new Range(90, -10, -90, 10),  new Range(90, -10, -90, 10));
        assertNormalizedRange("whole longitude", new Range(10, -180, -10, 180),  new Range(10, -180, -10, 180));

        assertNormalizedRange("more than whole world", new Range(90+10, -180-20, -90-30, 180+40),  new Range(90, -180, -90, 180));
        assertNormalizedRange("more than whole latitude", new Range(90+10, -20, -90-30, 40),  new Range(90, -20, -90, 40));
        assertNormalizedRange("more than whole longitude", new Range(10, -180-20, -30, 180+40),  new Range(10, -180, -30, 180));

        assertNormalizedRange("longitude wrap", new Range(10, -180-20, -30, -180+40),  new Range(10, -180, -30, -180+40), new Range(10, 180-20, -30, 180));
        assertNormalizedRange("latitude wrap left", new Range(90+10, -140, 90-30, -120),  new Range(90, -140, 90-30, -120), new Range(90, -140+180, 90-10, -120+180));
        assertNormalizedRange("latitude wrap middle", new Range(90+10, -20, 90-30, 40),  new Range(90, -20, 90-30, 40), new Range(90, -20+180, 90-10, 180), new Range(90, -180, 90-10, 40-180));
        assertNormalizedRange("latitude wrap right", new Range(90+10, 120, 90-30, 140),  new Range(90, 120, 90-30, 140), new Range(90, 120-180, 90-10, 140-180));
        assertNormalizedRange("latitude and longitude wrap", new Range(-90+10, 180-20, -90-30, 180+40), new Range(-90+10, 180-20, -90, 180), new Range(-90+10, -180, -90, -180+40), new Range(-90+30, -20, -90, 40));
    }

    private static void assertNormalizedPoint(Point input, Point expected) {
        normalizePoint(input);
        assertThat(input, equalTo(expected));
    }

    private static void assertNormalizedRange(String msg, Range input, Range... expectedOutput) {
        Set<Range> expected = new TreeSet<Range>(Arrays.asList(expectedOutput));
        assertThat(msg + " (duplicates in expected output)", expected.size(), equalTo(expectedOutput.length));
        List<Range> output = GeoUtils.normalizeRange(input);
        Set<Range> outputSet = new TreeSet(output);
        assertThat(msg + " (no output duplicates)", outputSet.size(), equalTo(output.size()));
        assertThat(msg + " (expected output length)", outputSet.size(), equalTo(expectedOutput.length));
        assertThat(msg, (Collection<Range>)outputSet, containsInAnyOrder(expectedOutput));

        // Now shift a little along both axis
        Range shifted = new Range();
        for (int i = -2 ; i <= 2 ; ++i) {
            for (int j = -2 ; j <= 2 ; ++j) {
                shifted.topLeft.lat = input.topLeft.lat + i * 180;
                shifted.topLeft.lon = input.topLeft.lon + j * 360;
                shifted.bottomRight.lat = input.bottomRight.lat + i * 180;
                shifted.bottomRight.lon = input.bottomRight.lon + j * 360;

                output = GeoUtils.normalizeRange(input);
                assertThat(msg + ", shifted by " + (i*180) + " ; " + (j*360) + " (expected output length)", output.size(), equalTo(expectedOutput.length));
                assertThat(msg + ", shifted by " + (i*180) + " ; " + (j*360), output, containsInAnyOrder(expectedOutput));
            }
        }
    }

}
