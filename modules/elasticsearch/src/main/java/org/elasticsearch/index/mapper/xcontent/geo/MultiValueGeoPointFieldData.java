/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.mapper.xcontent.geo;

import org.elasticsearch.common.RamUsage;
import org.elasticsearch.common.thread.ThreadLocals;
import org.elasticsearch.index.field.data.doubles.DoubleFieldData;
import org.elasticsearch.index.search.geo.GeoHashUtils;

/**
 * @author kimchy (shay.banon)
 */
public class MultiValueGeoPointFieldData extends GeoPointFieldData {

    private static final int VALUE_CACHE_SIZE = 100;

    private static ThreadLocal<ThreadLocals.CleanableValue<GeoPoint[][]>> valuesArrayCache = new ThreadLocal<ThreadLocals.CleanableValue<GeoPoint[][]>>() {
        @Override protected ThreadLocals.CleanableValue<GeoPoint[][]> initialValue() {
            GeoPoint[][] value = new GeoPoint[VALUE_CACHE_SIZE][];
            for (int i = 0; i < value.length; i++) {
                value[i] = new GeoPoint[i];
                for (int j = 0; j < value.length; j++) {
                    value[i][j] = new GeoPoint();
                }
            }
            return new ThreadLocals.CleanableValue<GeoPoint[][]>(value);
        }
    };

    private ThreadLocal<ThreadLocals.CleanableValue<double[][]>> valuesLatCache = new ThreadLocal<ThreadLocals.CleanableValue<double[][]>>() {
        @Override protected ThreadLocals.CleanableValue<double[][]> initialValue() {
            double[][] value = new double[VALUE_CACHE_SIZE][];
            for (int i = 0; i < value.length; i++) {
                value[i] = new double[i];
            }
            return new ThreadLocals.CleanableValue<double[][]>(value);
        }
    };

    private ThreadLocal<ThreadLocals.CleanableValue<double[][]>> valuesLonCache = new ThreadLocal<ThreadLocals.CleanableValue<double[][]>>() {
        @Override protected ThreadLocals.CleanableValue<double[][]> initialValue() {
            double[][] value = new double[VALUE_CACHE_SIZE][];
            for (int i = 0; i < value.length; i++) {
                value[i] = new double[i];
            }
            return new ThreadLocals.CleanableValue<double[][]>(value);
        }
    };

    // order with value 0 indicates no value
    private final int[][] ordinals;

    public MultiValueGeoPointFieldData(String fieldName, int[][] ordinals, double[] lat, double[] lon) {
        super(fieldName, lat, lon);
        this.ordinals = ordinals;
    }

    @Override protected long computeSizeInBytes() {
        long size = super.computeSizeInBytes();
        size += RamUsage.NUM_BYTES_ARRAY_HEADER; // for the top level array
        for (int[] ordinal : ordinals) {
            size += RamUsage.NUM_BYTES_INT * ordinal.length + RamUsage.NUM_BYTES_ARRAY_HEADER;
        }
        return size;
    }

    @Override public boolean multiValued() {
        return true;
    }

    @Override public boolean hasValue(int docId) {
        return ordinals[docId] != null;
    }

    @Override public void forEachValueInDoc(int docId, StringValueInDocProc proc) {
        int[] docOrders = ordinals[docId];
        if (docOrders == null) {
            return;
        }
        for (int docOrder : docOrders) {
            proc.onValue(docId, GeoHashUtils.encode(lat[docOrder], lon[docOrder]));
        }
    }

    @Override public GeoPoint value(int docId) {
        int[] docOrders = ordinals[docId];
        if (docOrders == null) {
            return null;
        }
        GeoPoint point = valuesCache.get().get();
        int loc = docOrders[0];
        point.latlon(lat[loc], lon[loc]);
        return point;
    }

    @Override public GeoPoint[] values(int docId) {
        int[] docOrders = ordinals[docId];
        if (docOrders == null) {
            return EMPTY_ARRAY;
        }
        GeoPoint[] points;
        if (docOrders.length < VALUE_CACHE_SIZE) {
            points = valuesArrayCache.get().get()[docOrders.length];
            for (int i = 0; i < docOrders.length; i++) {
                int loc = docOrders[i];
                points[i].latlon(lat[loc], lon[loc]);
            }
        } else {
            points = new GeoPoint[docOrders.length];
            for (int i = 0; i < docOrders.length; i++) {
                int loc = docOrders[i];
                points[i] = new GeoPoint(lat[loc], lon[loc]);
            }
        }
        return points;
    }

    @Override public double latValue(int docId) {
        int[] docOrders = ordinals[docId];
        if (docOrders == null) {
            return 0;
        }
        return lat[docOrders[0]];
    }

    @Override public double lonValue(int docId) {
        int[] docOrders = ordinals[docId];
        if (docOrders == null) {
            return 0;
        }
        return lon[docOrders[0]];
    }

    @Override public double[] latValues(int docId) {
        int[] docOrders = ordinals[docId];
        if (docOrders == null) {
            return DoubleFieldData.EMPTY_DOUBLE_ARRAY;
        }
        double[] doubles;
        if (docOrders.length < VALUE_CACHE_SIZE) {
            doubles = valuesLatCache.get().get()[docOrders.length];
        } else {
            doubles = new double[docOrders.length];
        }
        for (int i = 0; i < docOrders.length; i++) {
            doubles[i] = lat[docOrders[i]];
        }
        return doubles;
    }

    @Override public double[] lonValues(int docId) {
        int[] docOrders = ordinals[docId];
        if (docOrders == null) {
            return DoubleFieldData.EMPTY_DOUBLE_ARRAY;
        }
        double[] doubles;
        if (docOrders.length < VALUE_CACHE_SIZE) {
            doubles = valuesLonCache.get().get()[docOrders.length];
        } else {
            doubles = new double[docOrders.length];
        }
        for (int i = 0; i < docOrders.length; i++) {
            doubles[i] = lon[docOrders[i]];
        }
        return doubles;
    }
}