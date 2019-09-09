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
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.index.fielddata.AbstractSortingNumericDocValues;
import org.elasticsearch.index.fielddata.MultiGeoValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.locationtech.spatial4j.io.GeohashUtils;

import java.io.IOException;

/**
 * Wrapper class to help convert {@link MultiGeoValues}
 * to numeric long values for bucketing.
 */
class CellIdSource extends ValuesSource.Numeric {
    private final ValuesSource.Geo valuesSource;
    private final int precision;
    private final GeoGridTiler encoder;

    CellIdSource(Geo valuesSource, int precision, GeoGridTiler encoder) {
        this.valuesSource = valuesSource;
        //different GeoPoints could map to the same or different hashing cells.
        this.precision = precision;
        this.encoder = encoder;
    }

    public int precision() {
        return precision;
    }

    @Override
    public boolean isFloatingPoint() {
        return false;
    }

    @Override
    public SortedNumericDocValues longValues(LeafReaderContext ctx) {
        return new CellValues(valuesSource.geoValues(ctx), precision, encoder);
    }

    @Override
    public SortedNumericDoubleValues doubleValues(LeafReaderContext ctx) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedBinaryDocValues bytesValues(LeafReaderContext ctx) {
        throw new UnsupportedOperationException();
    }

    /**
     * The tiler to use to convert a geopoint's (lon, lat, precision) into
     * a long-encoded bucket key for aggregating.
     */
    public interface GeoGridTiler {
        long encode(double lon, double lat, int precision);
        int getCandidateTileCount(MultiGeoValues.GeoValue geoValue, int precision);
        int setValues(long[] docValues, MultiGeoValues.GeoValue geoValue, int precision);
    }

    public static class GeoHashGridTiler implements GeoGridTiler {
        public static final GeoHashGridTiler INSTANCE = new GeoHashGridTiler();

        @Override
        public long encode(double lon, double lat, int precision) {
            return Geohash.longEncode(lon, lat, precision);
        }

        @Override
        public int getCandidateTileCount(MultiGeoValues.GeoValue geoValue, int precision) {
            MultiGeoValues.BoundingBox bounds = geoValue.boundingBox();
            String rr = Geohash.stringEncode(bounds.minX(), bounds.minY(), precision);
            Rectangle rrr = Geohash.toBoundingBox(rr);
            double[] degStep = GeohashUtils.lookupDegreesSizeForHashLen(precision);
            int numLonCells = (int) Math.ceil((bounds.maxX() - rrr.getMinX()) / degStep[1]);
            int numLatCells = (int) Math.ceil((bounds.maxY() - rrr.getMinY()) / degStep[0]);
            return numLonCells * numLatCells;
        }

        @Override
        public int setValues(long[] values, MultiGeoValues.GeoValue geoValue, int precision) {
            MultiGeoValues.BoundingBox bounds = geoValue.boundingBox();
            double[] degStep = GeohashUtils.lookupDegreesSizeForHashLen(precision);
            int idx = 0;
            String rr = Geohash.stringEncode(bounds.minX(), bounds.minY(), precision);
            Rectangle rrr = Geohash.toBoundingBox(rr);
            for (double i = rrr.getMinX(); i < bounds.maxX(); i+= degStep[1]) {
                for (double j = rrr.getMinY(); j < bounds.maxY(); j += degStep[0]) {
                    Rectangle rectangle = Geohash.toBoundingBox(Geohash.stringEncode(i, j, precision));
                    if (geoValue.intersects(rectangle)) {
                        values[idx++] = encode(i, j, precision);
                    }
                }
            }

            return idx + 1;
        }
    }

    public static class GeoTileGridTiler implements GeoGridTiler {
        public static final GeoTileGridTiler INSTANCE = new GeoTileGridTiler();

        @Override
        public long encode(double lon, double lat, int precision) {
            return GeoTileUtils.longEncode(lon, lat, precision);
        }

        @Override
        public int getCandidateTileCount(MultiGeoValues.GeoValue geoValue, int precision) {
            MultiGeoValues.BoundingBox bounds = geoValue.boundingBox();
            final double tiles = 1 << precision;
            int minXTile = GeoTileUtils.getXTile(bounds.minX(), (long) tiles);
            int minYTile = GeoTileUtils.getYTile(bounds.maxY(), (long) tiles);
            int maxXTile = GeoTileUtils.getXTile(bounds.maxX(), (long) tiles);
            int maxYTile = GeoTileUtils.getYTile(bounds.minY(), (long) tiles);
            return (maxXTile - minXTile) * (maxYTile - minYTile);
        }

        @Override
        public int setValues(long[] values, MultiGeoValues.GeoValue geoValue, int precision) {
            MultiGeoValues.BoundingBox bounds = geoValue.boundingBox();

            final double tiles = 1 << precision;
            int minXTile = GeoTileUtils.getXTile(bounds.minX(), (long) tiles);
            int minYTile = GeoTileUtils.getYTile(bounds.maxY(), (long) tiles);
            int maxXTile = GeoTileUtils.getXTile(bounds.maxX(), (long) tiles);
            int maxYTile = GeoTileUtils.getYTile(bounds.minY(), (long) tiles);
            int idx = 0;
            for (int i = minXTile; i <= maxXTile; i++) {
                for (int j = minYTile; j <= maxYTile; j++) {
                    Rectangle rectangle = GeoTileUtils.toBoundingBox(i, j, precision);
                    if (geoValue.intersects(rectangle)) {
                        values[idx++] = GeoTileUtils.longEncodeTiles(precision, i, j);
                    }
                }
            }

            return idx + 1;
        }
    }

    private static class CellValues extends AbstractSortingNumericDocValues {
        private MultiGeoValues geoValues;
        private int precision;
        private GeoGridTiler tiler;

        protected CellValues(MultiGeoValues geoValues, int precision, GeoGridTiler tiler) {
            this.geoValues = geoValues;
            this.precision = precision;
            this.tiler = tiler;
        }

        @Override
        public boolean advanceExact(int docId) throws IOException {
            if (geoValues.advanceExact(docId)) {
                switch (geoValues.valuesSourceType()) {
                    case GEOPOINT:
                        resize(geoValues.docValueCount());
                        for (int i = 0; i < docValueCount(); ++i) {
                            MultiGeoValues.GeoValue target = geoValues.nextValue();
                            values[i] = tiler.encode(target.lon(), target.lat(), precision);
                        }
                        break;
                    case GEOSHAPE:
                        MultiGeoValues.GeoValue target = geoValues.nextValue();
                        resize(tiler.getCandidateTileCount(target, precision));
                        tiler.setValues(values, target, precision);
                        break;
                    default:
                        throw new IllegalArgumentException("unsupported geo type");
                }
                sort();
                return true;
            } else {
                return false;
            }
        }
    }
}
