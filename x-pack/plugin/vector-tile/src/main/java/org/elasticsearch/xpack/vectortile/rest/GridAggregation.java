/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectortile.rest;

import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.h3.H3;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.xpack.spatial.common.H3CartesianUtil;
import org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid.GeoHexGridAggregationBuilder;
import org.elasticsearch.xpack.vectortile.feature.FeatureFactory;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

/**
 * Enum containing the basic operations for different GeoGridAggregations.
 */
enum GridAggregation {
    GEOTILE {
        @Override
        public GeoGridAggregationBuilder newAgg(String aggName) {
            return new GeoTileGridAggregationBuilder(aggName);
        }

        @Override
        public Rectangle bufferTile(Rectangle tile, int z, int gridPrecision) {
            // No buffering needed as GeoTile bins aligns with the tile
            return tile;
        }

        @Override
        public int gridPrecisionToAggPrecision(int z, int gridPrecision) {
            return Math.min(GeoTileUtils.MAX_ZOOM, z + gridPrecision);
        }

        @Override
        public byte[] toGrid(String bucketKey, FeatureFactory featureFactory) throws IOException {
            final Rectangle r = toRectangle(bucketKey);
            return featureFactory.box(r.getMinLon(), r.getMaxLon(), r.getMinLat(), r.getMaxLat());
        }

        @Override
        public Rectangle toRectangle(String bucketKey) {
            return GeoTileUtils.toBoundingBox(bucketKey);
        }

        @Override
        public boolean needsBounding(int z, int gridPrecision) {
            // we always bound it as it is pretty efficient anyway.
            return true;
        }
    },
    GEOHEX {

        // Because hex bins do not fit perfectly on a tile, we need to buffer our queries in order to collect
        // all points inside the bin. For levels 0 and 1 we will consider all data, values for level 2 have
        // been computed manually and approximated. The amount that the buffer decreases by level has been
        // approximated to 2.5 (brute force computation suggest ~2.6 in the first 9 levels so 2.5 should be safe).
        private static final double[] LAT_BUFFER_SIZE = new double[16];
        private static final double[] LON_BUFFER_SIZE = new double[16];
        static {
            LAT_BUFFER_SIZE[0] = LAT_BUFFER_SIZE[1] = Double.NaN;
            LON_BUFFER_SIZE[0] = LON_BUFFER_SIZE[1] = Double.NaN;
            LAT_BUFFER_SIZE[2] = 3.7;
            LON_BUFFER_SIZE[2] = 51.2;
            for (int i = 3; i < LON_BUFFER_SIZE.length; i++) {
                LAT_BUFFER_SIZE[i] = LAT_BUFFER_SIZE[i - 1] / 2.5;
                LON_BUFFER_SIZE[i] = LON_BUFFER_SIZE[i - 1] / 2.5;
            }
        }
        // Mapping between a vector tile zoom and a H3 resolution. The mapping tries to keep the density of hexes similar
        // to the density of tile bins but trying not to be bigger.
        // Level unique tiles H3 resolution unique hexes ratio
        // 1 4 0 122 30.5
        // 2 16 0 122 7.625
        // 3 64 1 842 13.15625
        // 4 256 1 842 3.2890625
        // 5 1024 2 5882 5.744140625
        // 6 4096 2 5882 1.436035156
        // 7 16384 3 41162 2.512329102
        // 8 65536 3 41162 0.6280822754
        // 9 262144 4 288122 1.099098206
        // 10 1048576 4 288122 0.2747745514
        // 11 4194304 5 2016842 0.4808526039
        // 12 16777216 6 14117882 0.8414913416
        // 13 67108864 6 14117882 0.2103728354
        // 14 268435456 7 98825162 0.3681524172
        // 15 1073741824 8 691776122 0.644266719
        // 16 4294967296 8 691776122 0.1610666797
        // 17 17179869184 9 4842432842 0.2818666889
        // 18 68719476736 10 33897029882 0.4932667053
        // 19 274877906944 11 237279209162 0.8632167343
        // 20 1099511627776 11 237279209162 0.2158041836
        // 21 4398046511104 12 1660954464122 0.3776573213
        // 22 17592186044416 13 11626681248842 0.6609003122
        // 23 70368744177664 13 11626681248842 0.165225078
        // 24 281474976710656 14 81386768741882 0.2891438866
        // 25 1125899906842620 15 569707381193162 0.5060018015
        // 26 4503599627370500 15 569707381193162 0.1265004504
        // 27 18014398509482000 15 569707381193162 0.03162511259
        // 28 72057594037927900 15 569707381193162 0.007906278149
        // 29 288230376151712000 15 569707381193162 0.001976569537
        private static final int[] ZOOM2RESOLUTION = new int[] {
            0,
            0,
            0,
            1,
            1,
            2,
            2,
            3,
            3,
            3,
            4,
            4,
            5,
            6,
            6,
            7,
            8,
            9,
            9,
            10,
            11,
            11,
            12,
            13,
            14,
            14,
            15,
            15,
            15,
            15 };

        @Override
        public GeoGridAggregationBuilder newAgg(String aggName) {
            return new GeoHexGridAggregationBuilder(aggName);
        }

        @Override
        public Rectangle bufferTile(Rectangle tile, int z, int gridPrecision) {
            if (z == 0 || gridPrecision == 0) {
                // no need to buffer at level 0 as we are looking to all data.
                return tile;
            }
            final int aggPrecision = gridPrecisionToAggPrecision(z, gridPrecision);
            if (aggPrecision < 2) {
                // we need to consider all data
                return new Rectangle(-180, 180, GeoTileUtils.LATITUDE_MASK, -GeoTileUtils.LATITUDE_MASK);
            }
            return new Rectangle(
                GeoUtils.normalizeLon(tile.getMinX() - LON_BUFFER_SIZE[aggPrecision]),
                GeoUtils.normalizeLon(tile.getMaxX() + LON_BUFFER_SIZE[aggPrecision]),
                Math.min(GeoTileUtils.LATITUDE_MASK, tile.getMaxY() + LAT_BUFFER_SIZE[aggPrecision]),
                Math.max(-GeoTileUtils.LATITUDE_MASK, tile.getMinY() - LAT_BUFFER_SIZE[aggPrecision])
            );
        }

        @Override
        public int gridPrecisionToAggPrecision(int z, int gridPrecision) {
            return ZOOM2RESOLUTION[GEOTILE.gridPrecisionToAggPrecision(z, gridPrecision)];
        }

        @Override
        public byte[] toGrid(String bucketKey, FeatureFactory featureFactory) {
            final List<byte[]> x = featureFactory.getFeatures(H3CartesianUtil.getNormalizeGeometry(H3.stringToH3(bucketKey)));
            return x.size() > 0 ? x.get(0) : null;
        }

        @Override
        public Rectangle toRectangle(String bucketKey) {
            return H3CartesianUtil.toBoundingBox(H3.stringToH3(bucketKey));
        }

        @Override
        public boolean needsBounding(int z, int gridPrecision) {
            /*
              Bounded geohex aggregation can be expensive, in particular where there is lots of data outside the bounding
              box. Because we are buffering our queries, this is magnified for low precision tiles. Because the total number
              of buckets up to precision 3 is lower than the default max buckets, we better not bound those aggregations
              which results in much better performance.
             */
            return gridPrecisionToAggPrecision(z, gridPrecision) > 3;
        }
    };

    /**
     * New {@link GeoGridAggregationBuilder} instance.
     */
    public abstract GeoGridAggregationBuilder newAgg(String aggName);

    /**
     * Buffer the query bounding box so the bins of an aggregation see
     * all data that is inside them.
     */
    public abstract Rectangle bufferTile(Rectangle tile, int z, int gridPrecision);

    /**
     * Transform the provided grid precision at the given zoom to the
     * agg precision.
     */
    public abstract int gridPrecisionToAggPrecision(int z, int gridPrecision);

    /**
     * transforms the geometry of a given bin into the vector tile feature.
     */
    public abstract byte[] toGrid(String bucketKey, FeatureFactory featureFactory) throws IOException;

    /**
     * Returns the bounding box of the bin.
     */
    public abstract Rectangle toRectangle(String bucketKey);

    /**
     * If false, the aggregation at the given zoom and grid precision is not bound.
     */
    public abstract boolean needsBounding(int z, int gridPrecision);

    public static GridAggregation fromString(String type) {
        return switch (type.toLowerCase(Locale.ROOT)) {
            case "geotile" -> GEOTILE;
            case "geohex" -> GEOHEX;
            default -> throw new IllegalArgumentException("Invalid agg type [" + type + "]");
        };
    }
}
