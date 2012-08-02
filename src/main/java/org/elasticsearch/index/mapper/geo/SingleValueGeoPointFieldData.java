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

import org.elasticsearch.common.RamUsage;
import org.elasticsearch.common.util.concurrent.ThreadLocals;
import org.elasticsearch.index.field.data.doubles.DoubleFieldData;
import org.elasticsearch.index.search.geo.GeoHashUtils;

/**
 *
 */
public class SingleValueGeoPointFieldData extends GeoPointFieldData {

    private static ThreadLocal<ThreadLocals.CleanableValue<GeoPoint[]>> valuesArrayCache = new ThreadLocal<ThreadLocals.CleanableValue<GeoPoint[]>>() {
        @Override
        protected ThreadLocals.CleanableValue<GeoPoint[]> initialValue() {
            GeoPoint[] value = new GeoPoint[1];
            value[0] = new GeoPoint();
            return new ThreadLocals.CleanableValue<GeoPoint[]>(value);
        }
    };

    private ThreadLocal<ThreadLocals.CleanableValue<double[]>> valuesLatCache = new ThreadLocal<ThreadLocals.CleanableValue<double[]>>() {
        @Override
        protected ThreadLocals.CleanableValue<double[]> initialValue() {
            return new ThreadLocals.CleanableValue<double[]>(new double[1]);
        }
    };

    private ThreadLocal<ThreadLocals.CleanableValue<double[]>> valuesLonCache = new ThreadLocal<ThreadLocals.CleanableValue<double[]>>() {
        @Override
        protected ThreadLocals.CleanableValue<double[]> initialValue() {
            return new ThreadLocals.CleanableValue<double[]>(new double[1]);
        }
    };


    // order with value 0 indicates no value
    private final int[] ordinals;

    public SingleValueGeoPointFieldData(String fieldName, int[] ordinals, double[] lat, double[] lon) {
        super(fieldName, lat, lon);
        this.ordinals = ordinals;
    }

    @Override
    protected long computeSizeInBytes() {
        return super.computeSizeInBytes() +
                RamUsage.NUM_BYTES_INT * ordinals.length + RamUsage.NUM_BYTES_ARRAY_HEADER;
    }

    @Override
    public boolean multiValued() {
        return false;
    }

    @Override
    public boolean hasValue(int docId) {
        return ordinals[docId] != 0;
    }

    @Override
    public void forEachValueInDoc(int docId, StringValueInDocProc proc) {
        int loc = ordinals[docId];
        if (loc == 0) {
            proc.onMissing(docId);
            return;
        }
        proc.onValue(docId, GeoHashUtils.encode(lat[loc], lon[loc]));
    }

    @Override
    public void forEachOrdinalInDoc(int docId, OrdinalInDocProc proc) {
        proc.onOrdinal(docId, ordinals[docId]);
    }

    @Override
    public void forEachValueInDoc(int docId, ValueInDocProc proc) {
        int loc = ordinals[docId];
        if (loc == 0) {
            return;
        }
        proc.onValue(docId, lat[loc], lon[loc]);
    }

    @Override
    public GeoPoint value(int docId) {
        int loc = ordinals[docId];
        if (loc == 0) {
            return null;
        }
        GeoPoint point = valuesCache.get().get();
        point.latlon(lat[loc], lon[loc]);
        return point;
    }

    @Override
    public GeoPoint[] values(int docId) {
        int loc = ordinals[docId];
        if (loc == 0) {
            return EMPTY_ARRAY;
        }
        GeoPoint[] ret = valuesArrayCache.get().get();
        ret[0].latlon(lat[loc], lon[loc]);
        return ret;
    }

    @Override
    public double latValue(int docId) {
        return lat[ordinals[docId]];
    }

    @Override
    public double lonValue(int docId) {
        return lon[ordinals[docId]];
    }

    @Override
    public double[] latValues(int docId) {
        int loc = ordinals[docId];
        if (loc == 0) {
            return DoubleFieldData.EMPTY_DOUBLE_ARRAY;
        }
        double[] ret = valuesLatCache.get().get();
        ret[0] = lat[loc];
        return ret;
    }

    @Override
    public double[] lonValues(int docId) {
        int loc = ordinals[docId];
        if (loc == 0) {
            return DoubleFieldData.EMPTY_DOUBLE_ARRAY;
        }
        double[] ret = valuesLonCache.get().get();
        ret[0] = lon[loc];
        return ret;
    }
}
