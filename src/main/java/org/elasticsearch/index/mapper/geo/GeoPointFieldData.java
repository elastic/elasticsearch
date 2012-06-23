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

package org.elasticsearch.index.mapper.geo;

import gnu.trove.list.array.TDoubleArrayList;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.util.concurrent.ThreadLocals;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.support.FieldDataLoader;
import org.elasticsearch.index.search.geo.GeoDistance;
import org.elasticsearch.index.search.geo.GeoHashUtils;

import java.io.IOException;

/**
 *
 */
public abstract class GeoPointFieldData extends FieldData<GeoPointDocFieldData> {

    static ThreadLocal<ThreadLocals.CleanableValue<GeoPoint>> valuesCache = new ThreadLocal<ThreadLocals.CleanableValue<GeoPoint>>() {
        @Override
        protected ThreadLocals.CleanableValue<GeoPoint> initialValue() {
            return new ThreadLocals.CleanableValue<GeoPoint>(new GeoPoint());
        }
    };

    static class GeoPointHash {
        public double lat;
        public double lon;
        public String geoHash = "";
    }

    static ThreadLocal<ThreadLocals.CleanableValue<GeoPointHash>> geoHashCache = new ThreadLocal<ThreadLocals.CleanableValue<GeoPointHash>>() {
        @Override
        protected ThreadLocals.CleanableValue<GeoPointHash> initialValue() {
            return new ThreadLocals.CleanableValue<GeoPointHash>(new GeoPointHash());
        }
    };

    public static final GeoPoint[] EMPTY_ARRAY = new GeoPoint[0];

    protected final double[] lat;
    protected final double[] lon;

    protected GeoPointFieldData(String fieldName, double[] lat, double[] lon) {
        super(fieldName);
        this.lat = lat;
        this.lon = lon;
    }

    abstract public GeoPoint value(int docId);

    abstract public GeoPoint[] values(int docId);

    abstract public double latValue(int docId);

    abstract public double lonValue(int docId);

    abstract public double[] latValues(int docId);

    abstract public double[] lonValues(int docId);

    public double distance(int docId, DistanceUnit unit, double lat, double lon) {
        return GeoDistance.PLANE.calculate(latValue(docId), lonValue(docId), lat, lon, unit);
    }

    public double arcDistance(int docId, DistanceUnit unit, double lat, double lon) {
        return GeoDistance.ARC.calculate(latValue(docId), lonValue(docId), lat, lon, unit);
    }

    public double factorDistance(int docId, DistanceUnit unit, double lat, double lon) {
        return GeoDistance.FACTOR.calculate(latValue(docId), lonValue(docId), lat, lon, unit);
    }

    public double distanceGeohash(int docId, DistanceUnit unit, String geoHash) {
        GeoPointHash geoPointHash = geoHashCache.get().get();
        if (geoPointHash.geoHash != geoHash) {
            geoPointHash.geoHash = geoHash;
            double[] decode = GeoHashUtils.decode(geoHash);
            geoPointHash.lat = decode[0];
            geoPointHash.lon = decode[1];
        }
        return GeoDistance.PLANE.calculate(latValue(docId), lonValue(docId), geoPointHash.lat, geoPointHash.lon, unit);
    }

    @Override
    public GeoPointDocFieldData docFieldData(int docId) {
        return super.docFieldData(docId);
    }

    @Override
    protected long computeSizeInBytes() {
        return (RamUsage.NUM_BYTES_DOUBLE * lat.length + RamUsage.NUM_BYTES_ARRAY_HEADER) +
                (RamUsage.NUM_BYTES_DOUBLE * lon.length + RamUsage.NUM_BYTES_ARRAY_HEADER);
    }

    @Override
    public String stringValue(int docId) {
        return value(docId).geohash();
    }

    @Override
    protected GeoPointDocFieldData createFieldData() {
        return new GeoPointDocFieldData(this);
    }

    @Override
    public FieldDataType type() {
        return GeoPointFieldDataType.TYPE;
    }

    @Override
    public void forEachValue(StringValueProc proc) {
        for (int i = 1; i < lat.length; i++) {
            proc.onValue(GeoHashUtils.encode(lat[i], lon[i]));
        }
    }

    public void forEachValue(PointValueProc proc) {
        for (int i = 1; i < lat.length; i++) {
            GeoPoint point = valuesCache.get().get();
            point.latlon(lat[i], lon[i]);
            proc.onValue(point);
        }
    }

    public static interface PointValueProc {
        void onValue(GeoPoint value);
    }

    public void forEachValue(ValueProc proc) {
        for (int i = 1; i < lat.length; i++) {
            proc.onValue(lat[i], lon[i]);
        }
    }

    public static interface ValueProc {
        void onValue(double lat, double lon);
    }

    public abstract void forEachValueInDoc(int docId, ValueInDocProc proc);

    public static interface ValueInDocProc {
        void onValue(int docId, double lat, double lon);
    }

    public static GeoPointFieldData load(IndexReader reader, String field) throws IOException {
        return FieldDataLoader.load(reader, field, new StringTypeLoader());
    }

    static class StringTypeLoader extends FieldDataLoader.FreqsTypeLoader<GeoPointFieldData> {

        private final TDoubleArrayList lat = new TDoubleArrayList();
        private final TDoubleArrayList lon = new TDoubleArrayList();

        StringTypeLoader() {
            super();
            // the first one indicates null value
            lat.add(0);
            lon.add(0);
        }

        @Override
        public void collectTerm(String term) {
            int comma = term.indexOf(',');
            lat.add(Double.parseDouble(term.substring(0, comma)));
            lon.add(Double.parseDouble(term.substring(comma + 1)));

        }

        @Override
        public GeoPointFieldData buildSingleValue(String field, int[] ordinals) {
            return new SingleValueGeoPointFieldData(field, ordinals, lat.toArray(), lon.toArray());
        }

        @Override
        public GeoPointFieldData buildMultiValue(String field, int[][] ordinals) {
            return new MultiValueGeoPointFieldData(field, ordinals, lat.toArray(), lon.toArray());
        }
    }
}
