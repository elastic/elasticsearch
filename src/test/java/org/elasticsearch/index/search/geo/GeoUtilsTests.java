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

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class GeoUtilsTests extends ElasticsearchTestCase {

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
        // We've gone past the north pole and down the other side, the longitude will
        // be shifted by 180
        assertNormalizedPoint(new GeoPoint(90.5, 10), new GeoPoint(89.5, -170));

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
    public void testPrefixTreeCellSizes() {
        assertThat(GeoUtils.EARTH_SEMI_MAJOR_AXIS, equalTo(DistanceUtils.EARTH_EQUATORIAL_RADIUS_KM * 1000));
        assertThat(GeoUtils.quadTreeCellWidth(0), lessThanOrEqualTo(GeoUtils.EARTH_EQUATOR));

        SpatialContext spatialContext = new SpatialContext(true);

        GeohashPrefixTree geohashPrefixTree = new GeohashPrefixTree(spatialContext, GeohashPrefixTree.getMaxLevelsPossible()/2);
        Cell gNode = geohashPrefixTree.getWorldCell();

        for(int i = 0; i<geohashPrefixTree.getMaxLevels(); i++) {
            double width = GeoUtils.geoHashCellWidth(i);
            double height = GeoUtils.geoHashCellHeight(i);
            double size = GeoUtils.geoHashCellSize(i);
            double degrees = 360.0 * width / GeoUtils.EARTH_EQUATOR;
            int level = GeoUtils.quadTreeLevelsForPrecision(size);

            assertThat(GeoUtils.quadTreeCellWidth(level), lessThanOrEqualTo(width));
            assertThat(GeoUtils.quadTreeCellHeight(level), lessThanOrEqualTo(height));
            assertThat(GeoUtils.geoHashLevelsForPrecision(size), equalTo(geohashPrefixTree.getLevelForDistance(degrees)));

            assertThat("width at level "+i, gNode.getShape().getBoundingBox().getWidth(), equalTo(360.d * width / GeoUtils.EARTH_EQUATOR));
            assertThat("height at level "+i, gNode.getShape().getBoundingBox().getHeight(), equalTo(180.d * height / GeoUtils.EARTH_POLAR_DISTANCE));

            gNode = gNode.getSubCells(null).iterator().next();
        }

        QuadPrefixTree quadPrefixTree = new QuadPrefixTree(spatialContext);
        Cell qNode = quadPrefixTree.getWorldCell();
        for (int i = 0; i < QuadPrefixTree.DEFAULT_MAX_LEVELS; i++) {

            double degrees = 360.0/(1L<<i);
            double width = GeoUtils.quadTreeCellWidth(i);
            double height = GeoUtils.quadTreeCellHeight(i);
            double size = GeoUtils.quadTreeCellSize(i);
            int level = GeoUtils.quadTreeLevelsForPrecision(size);

            assertThat(GeoUtils.quadTreeCellWidth(level), lessThanOrEqualTo(width));
            assertThat(GeoUtils.quadTreeCellHeight(level), lessThanOrEqualTo(height));
            assertThat(GeoUtils.quadTreeLevelsForPrecision(size), equalTo(quadPrefixTree.getLevelForDistance(degrees)));

            assertThat("width at level "+i, qNode.getShape().getBoundingBox().getWidth(), equalTo(360.d * width / GeoUtils.EARTH_EQUATOR));
            assertThat("height at level "+i, qNode.getShape().getBoundingBox().getHeight(), equalTo(180.d * height / GeoUtils.EARTH_POLAR_DISTANCE));

            qNode = qNode.getSubCells(null).iterator().next();
        }
    }

    @Test
    public void testTryParsingLatLonFromString() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().field("lat", "52").field("lon", "4").endObject();
        XContentParser parser = XContentHelper.createParser(builder.bytes());
        parser.nextToken();
        GeoPoint geoPoint = GeoUtils.parseGeoPoint(parser);
        assertThat(geoPoint.lat(), is(52.0));
        assertThat(geoPoint.lon(), is(4.0));
    }


    private static void assertNormalizedPoint(GeoPoint input, GeoPoint expected) {
        GeoUtils.normalizePoint(input);
        assertThat(input, equalTo(expected));
    }
}
