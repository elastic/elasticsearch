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

import java.util.function.Function;

import org.elasticsearch.geo.GeoUtils;
import org.elasticsearch.geo.geometry.GeoShape.Relation;
import org.apache.lucene.util.SloppyMath;

import static org.elasticsearch.geo.GeoEncodingUtils.decodeLatitude;
import static org.elasticsearch.geo.GeoEncodingUtils.decodeLongitude;
import static org.elasticsearch.geo.GeoEncodingUtils.encodeLatitude;
import static org.elasticsearch.geo.GeoEncodingUtils.encodeLatitudeCeil;
import static org.elasticsearch.geo.GeoEncodingUtils.encodeLongitude;
import static org.elasticsearch.geo.GeoEncodingUtils.encodeLongitudeCeil;

/**
 * Used to speed up point-in-polygon and point-distance computations
 */
abstract class Predicate {
    static final int ARITY = 64;

    final int latShift, lonShift;
    final int latBase, lonBase;
    final int maxLatDelta, maxLonDelta;
    final byte[] relations;

    protected Predicate(Rectangle boundingBox, Function<Rectangle, Relation> boxToRelation) {
        final int minLat = encodeLatitudeCeil(boundingBox.minLat);
        final int maxLat = encodeLatitude(boundingBox.maxLat);
        final int minLon = encodeLongitudeCeil(boundingBox.minLon);
        final int maxLon = encodeLongitude(boundingBox.maxLon);

        int latShift = 1;
        int lonShift = 1;
        int latBase = 0;
        int lonBase = 0;
        int maxLatDelta = 0;
        int maxLonDelta = 0;
        byte[] relations;

        if (maxLat < minLat || (boundingBox.crossesDateline() == false && maxLon < minLon)) {
            // the box cannot match any quantized point
            relations = new byte[0];
        } else {
            {
                long minLat2 = (long) minLat - Integer.MIN_VALUE;
                long maxLat2 = (long) maxLat - Integer.MIN_VALUE;
                latShift = computeShift(minLat2, maxLat2);
                latBase = (int) (minLat2 >>> latShift);
                maxLatDelta = (int) (maxLat2 >>> latShift) - latBase + 1;
                assert maxLatDelta > 0;
            }
            {
                long minLon2 = (long) minLon - Integer.MIN_VALUE;
                long maxLon2 = (long) maxLon - Integer.MIN_VALUE;
                if (boundingBox.crossesDateline()) {
                    maxLon2 += 1L << 32; // wrap
                }
                lonShift = computeShift(minLon2, maxLon2);
                lonBase = (int) (minLon2 >>> lonShift);
                maxLonDelta = (int) (maxLon2 >>> lonShift) - lonBase + 1;
                assert maxLonDelta > 0;
            }

            relations = new byte[maxLatDelta * maxLonDelta];
            for (int i = 0; i < maxLatDelta; ++i) {
                for (int j = 0; j < maxLonDelta; ++j) {
                    final int boxMinLat = ((latBase + i) << latShift) + Integer.MIN_VALUE;
                    final int boxMinLon = ((lonBase + j) << lonShift) + Integer.MIN_VALUE;
                    final int boxMaxLat = boxMinLat + (1 << latShift) - 1;
                    final int boxMaxLon = boxMinLon + (1 << lonShift) - 1;

                    relations[i * maxLonDelta + j] = (byte) boxToRelation.apply(new Rectangle(
                        decodeLatitude(boxMinLat), decodeLatitude(boxMaxLat),
                        decodeLongitude(boxMinLon), decodeLongitude(boxMaxLon))).ordinal();
                }
            }
        }
        this.latShift = latShift;
        this.lonShift = lonShift;
        this.latBase = latBase;
        this.lonBase = lonBase;
        this.maxLatDelta = maxLatDelta;
        this.maxLonDelta = maxLonDelta;
        this.relations = relations;
    }

    /**
     * A predicate that checks whether a given point is within a distance of another point.
     */
    final static class DistancePredicate extends Predicate {

        private final double lat, lon;
        private final double distanceKey;
        private final double axisLat;

        private DistancePredicate(double lat, double lon, double distanceKey, double axisLat, Rectangle boundingBox,
                                  Function<Rectangle, Relation> boxToRelation) {
            super(boundingBox, boxToRelation);
            this.lat = lat;
            this.lon = lon;
            this.distanceKey = distanceKey;
            this.axisLat = axisLat;
        }

        /**
         * Create a predicate that checks whether points are within a distance of a given point.
         * It works by computing the bounding box around the circle that is defined
         * by the given points/distance and splitting it into between 1024 and 4096
         * smaller boxes (4096*0.75^2=2304 on average). Then for each sub box, it
         * computes the relation between this box and the distance query. Finally at
         * search time, it first computes the sub box that the point belongs to,
         * most of the time, no distance computation will need to be performed since
         * all points from the sub box will either be in or out of the circle.
         *
         * @lucene.internal
         */
        static DistancePredicate create(double lat, double lon, double radiusMeters) {
            final Rectangle boundingBox = Rectangle.fromPointDistance(lat, lon, radiusMeters);
            final double axisLat = Rectangle.axisLat(lat, radiusMeters);
            final double distanceSortKey = GeoUtils.distanceQuerySortKey(radiusMeters);
            final Function<Rectangle, Relation> boxToRelation = box -> GeoUtils.relate(
                box.minLat, box.maxLat, box.minLon, box.maxLon, lat, lon, distanceSortKey, axisLat);
            return new DistancePredicate(lat, lon, distanceSortKey, axisLat, boundingBox, boxToRelation);
        }

        public Relation relate(double minLat, double maxLat, double minLon, double maxLon) {
            return GeoUtils.relate(minLat, maxLat, minLon, maxLon, lat, lon, distanceKey, axisLat);
        }

        /**
         * Check whether the given point is within a distance of another point.
         * NOTE: this operates directly on the encoded representation of points.
         */
        public boolean test(int lat, int lon) {
            final int lat2 = ((lat - Integer.MIN_VALUE) >>> latShift);
            if (lat2 < latBase || lat2 >= latBase + maxLatDelta) {
                return false;
            }
            int lon2 = ((lon - Integer.MIN_VALUE) >>> lonShift);
            if (lon2 < lonBase) { // wrap
                lon2 += 1 << (32 - lonShift);
            }
            assert Integer.toUnsignedLong(lon2) >= lonBase;
            assert lon2 - lonBase >= 0;
            if (lon2 - lonBase >= maxLonDelta) {
                return false;
            }

            final int relation = relations[(lat2 - latBase) * maxLonDelta + (lon2 - lonBase)];
            if (relation == Relation.CROSSES.ordinal()) {
                return SloppyMath.haversinSortKey(
                    decodeLatitude(lat), decodeLongitude(lon),
                    this.lat, this.lon) <= distanceKey;
            } else {
                return relation == Relation.WITHIN.ordinal();
            }
        }
    }

    /**
     * A predicate that checks whether a given point is within a polygon.
     */
    final static class PolygonPredicate extends Predicate {

        final EdgeTree tree;

        private PolygonPredicate(EdgeTree tree, Rectangle boundingBox, Function<Rectangle, Relation> boxToRelation) {
            super(boundingBox, boxToRelation);
            this.tree = tree;
        }

        /**
         * Create a predicate that checks whether points are within a polygon.
         * It works the same way as {@code DistancePredicate.create}.
         *
         * @lucene.internal
         */
        public static PolygonPredicate create(Rectangle boundingBox, EdgeTree tree) {
            final Function<Rectangle, Relation> boxToRelation = box -> tree.relate(
                box.minLat, box.maxLat, box.minLon, box.maxLon);
            return new PolygonPredicate(tree, boundingBox, boxToRelation);
        }


        public Relation relate(final double minLat, final double maxLat, final double minLon, final double maxLon) {
            return tree.relate(minLat, maxLat, minLon, maxLon);
        }

        /**
         * Check whether the given point is within the considered polygon.
         * NOTE: this operates directly on the encoded representation of points.
         */
        public boolean test(int lat, int lon) {
            final int lat2 = ((lat - Integer.MIN_VALUE) >>> latShift);
            if (lat2 < latBase || lat2 >= latBase + maxLatDelta) {
                return false;
            }
            int lon2 = ((lon - Integer.MIN_VALUE) >>> lonShift);
            if (lon2 < lonBase) { // wrap
                lon2 += 1 << (32 - lonShift);
            }
            assert Integer.toUnsignedLong(lon2) >= lonBase;
            assert lon2 - lonBase >= 0;
            if (lon2 - lonBase >= maxLonDelta) {
                return false;
            }

            final int relation = relations[(lat2 - latBase) * maxLonDelta + (lon2 - lonBase)];
            if (relation == Relation.CROSSES.ordinal()) {
                return tree.contains(decodeLatitude(lat), decodeLongitude(lon));
            } else {
                return relation == Relation.WITHIN.ordinal();
            }
        }
    }

    /**
     * Compute the minimum shift value so that
     * {@code (b>>>shift)-(a>>>shift)} is less that {@code ARITY}.
     */
    private static int computeShift(long a, long b) {
        assert a <= b;
        // We enforce a shift of at least 1 so that when we work with unsigned ints
        // by doing (lat - MIN_VALUE), the result of the shift (lat - MIN_VALUE) >>> shift
        // can be used for comparisons without particular care: the sign bit has
        // been cleared so comparisons work the same for signed and unsigned ints
        for (int shift = 1; ; ++shift) {
            final long delta = (b >>> shift) - (a >>> shift);
            if (delta >= 0 && delta < ARITY) {
                return shift;
            }
        }
    }
}
