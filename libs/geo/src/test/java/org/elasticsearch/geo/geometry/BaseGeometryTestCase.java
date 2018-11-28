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

package org.elasticsearch.geo.geometry;

import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.util.LuceneTestCase;

abstract class BaseGeometryTestCase<T extends GeoShape> extends LuceneTestCase {
    abstract public T getShape();

    public void testArea() {
        expectThrows(UnsupportedOperationException.class, () -> getShape().getArea());
    }

    /**
     * tests bounding box of shape
     */
    abstract public void testBoundingBox();

    /**
     * tests WITHIN relation
     */
    abstract public void testWithin();

    /**
     * tests CONTAINS relation
     */
    abstract public void testContains();

    /**
     * tests DISJOINT relation
     */
    abstract public void testDisjoint();

    /**
     * tests INTERSECTS relation
     */
    abstract public void testIntersects();

    /**
     * tests center of shape
     */
    public void testCenter() {
        GeoShape shape = getShape();
        Rectangle bbox = shape.getBoundingBox();
        double centerLat = StrictMath.abs(bbox.maxLat() - bbox.minLat()) * 0.5 + bbox.minLat();
        double centerLon;
        if (bbox.crossesDateline()) {
            centerLon = GeoUtils.MAX_LON_INCL - bbox.minLon() + bbox.maxLon() - GeoUtils.MIN_LON_INCL;
            centerLon = GeoUtils.normalizeLonDegrees(centerLon * 0.5 + bbox.minLon());
        } else {
            centerLon = StrictMath.abs(bbox.maxLon() - bbox.minLon()) * 0.5 + bbox.minLon();
        }
        assertEquals(shape.getCenter(), new Point(centerLat, centerLon));
    }

    /**
     * helper method for semi-random relation testing
     */
    protected void relationTest(GeoShape points, GeoShape.Relation r) {
        Rectangle bbox = points.getBoundingBox();
        double minLat = bbox.minLat();
        double maxLat = bbox.maxLat();
        double minLon = bbox.minLon();
        double maxLon = bbox.maxLon();

        if (r == GeoShape.Relation.WITHIN) {
            return;
        } else if (r == GeoShape.Relation.DISJOINT) {
            // shrink test box
            minLat -= 20D;
            maxLat = minLat - 1D;
            minLon -= 20D;
            maxLon = minLon - 1D;
        } else if (r == GeoShape.Relation.INTERSECTS) {
            // intersects (note: MultiPoint does not support CONTAINS)
            minLat -= 10D;
            maxLat = minLat + 10D;
        }

        assertEquals(r, points.relate(minLat, maxLat, minLon, maxLon));
    }
}
