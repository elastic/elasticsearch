/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.legacygeo.search;

import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.test.ESTestCase;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceUtils;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class LegacyGeoUtilsTests extends ESTestCase {

    public void testPrefixTreeCellSizes() {
        assertThat(GeoUtils.EARTH_SEMI_MAJOR_AXIS, equalTo(DistanceUtils.EARTH_EQUATORIAL_RADIUS_KM * 1000));
        assertThat(GeoUtils.quadTreeCellWidth(0), lessThanOrEqualTo(GeoUtils.EARTH_EQUATOR));

        SpatialContext spatialContext = new SpatialContext(true);

        GeohashPrefixTree geohashPrefixTree = new GeohashPrefixTree(spatialContext, GeohashPrefixTree.getMaxLevelsPossible() / 2);
        Cell gNode = geohashPrefixTree.getWorldCell();

        for (int i = 0; i < geohashPrefixTree.getMaxLevels(); i++) {
            double width = GeoUtils.geoHashCellWidth(i);
            double height = GeoUtils.geoHashCellHeight(i);
            double size = GeoUtils.geoHashCellSize(i);
            double degrees = 360.0 * width / GeoUtils.EARTH_EQUATOR;
            int level = GeoUtils.quadTreeLevelsForPrecision(size);

            assertThat(GeoUtils.quadTreeCellWidth(level), lessThanOrEqualTo(width));
            assertThat(GeoUtils.quadTreeCellHeight(level), lessThanOrEqualTo(height));
            assertThat(GeoUtils.geoHashLevelsForPrecision(size), equalTo(geohashPrefixTree.getLevelForDistance(degrees)));

            assertThat(
                "width at level " + i,
                gNode.getShape().getBoundingBox().getWidth(),
                equalTo(360.d * width / GeoUtils.EARTH_EQUATOR)
            );
            assertThat(
                "height at level " + i,
                gNode.getShape().getBoundingBox().getHeight(),
                equalTo(180.d * height / GeoUtils.EARTH_POLAR_DISTANCE)
            );

            gNode = gNode.getNextLevelCells(null).next();
        }

        QuadPrefixTree quadPrefixTree = new QuadPrefixTree(spatialContext);
        Cell qNode = quadPrefixTree.getWorldCell();
        for (int i = 0; i < quadPrefixTree.getMaxLevels(); i++) {

            double degrees = 360.0 / (1L << i);
            double width = GeoUtils.quadTreeCellWidth(i);
            double height = GeoUtils.quadTreeCellHeight(i);
            double size = GeoUtils.quadTreeCellSize(i);
            int level = GeoUtils.quadTreeLevelsForPrecision(size);

            assertThat(GeoUtils.quadTreeCellWidth(level), lessThanOrEqualTo(width));
            assertThat(GeoUtils.quadTreeCellHeight(level), lessThanOrEqualTo(height));
            assertThat(GeoUtils.quadTreeLevelsForPrecision(size), equalTo(quadPrefixTree.getLevelForDistance(degrees)));

            assertThat(
                "width at level " + i,
                qNode.getShape().getBoundingBox().getWidth(),
                equalTo(360.d * width / GeoUtils.EARTH_EQUATOR)
            );
            assertThat(
                "height at level " + i,
                qNode.getShape().getBoundingBox().getHeight(),
                equalTo(180.d * height / GeoUtils.EARTH_POLAR_DISTANCE)
            );

            qNode = qNode.getNextLevelCells(null).next();
        }
    }
}
