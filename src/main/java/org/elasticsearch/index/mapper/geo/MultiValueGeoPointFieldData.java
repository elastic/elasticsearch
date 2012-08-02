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
public class MultiValueGeoPointFieldData extends GeoPointFieldData {

    private static final int VALUE_CACHE_SIZE = 100;

    private static ThreadLocal<ThreadLocals.CleanableValue<GeoPoint[][]>> valuesArrayCache = new ThreadLocal<ThreadLocals.CleanableValue<GeoPoint[][]>>() {
        @Override
        protected ThreadLocals.CleanableValue<GeoPoint[][]> initialValue() {
            GeoPoint[][] value = new GeoPoint[VALUE_CACHE_SIZE][];
            for (int i = 0; i < value.length; i++) {
                value[i] = new GeoPoint[i];
                for (int j = 0; j < value[i].length; j++) {
                    value[i][j] = new GeoPoint();
                }
            }
            return new ThreadLocals.CleanableValue<GeoPoint[][]>(value);
        }
    };

    private ThreadLocal<ThreadLocals.CleanableValue<double[][]>> valuesLatCache = new ThreadLocal<ThreadLocals.CleanableValue<double[][]>>() {
        @Override
        protected ThreadLocals.CleanableValue<double[][]> initialValue() {
            double[][] value = new double[VALUE_CACHE_SIZE][];
            for (int i = 0; i < value.length; i++) {
                value[i] = new double[i];
            }
            return new ThreadLocals.CleanableValue<double[][]>(value);
        }
    };

    private ThreadLocal<ThreadLocals.CleanableValue<double[][]>> valuesLonCache = new ThreadLocal<ThreadLocals.CleanableValue<double[][]>>() {
        @Override
        protected ThreadLocals.CleanableValue<double[][]> initialValue() {
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

    @Override
    protected long computeSizeInBytes() {
        long size = super.computeSizeInBytes();
        size += RamUsage.NUM_BYTES_ARRAY_HEADER; // for the top level array
        for (int[] ordinal : ordinals) {
            size += RamUsage.NUM_BYTES_INT * ordinal.length + RamUsage.NUM_BYTES_ARRAY_HEADER;
        }
        return size;
    }

    @Override
    public boolean multiValued() {
        return true;
    }

    @Override
    public boolean hasValue(int docId) {
        for (int[] ordinal : ordinals) {
            if (ordinal[docId] != 0) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void forEachValueInDoc(int docId, StringValueInDocProc proc) {
        for (int i = 0; i < ordinals.length; i++) {
            int loc = ordinals[i][docId];
            if (loc == 0) {
                if (i == 0) {
                    proc.onMissing(docId);
                }
                break;
            }
            proc.onValue(docId, GeoHashUtils.encode(lat[loc], lon[loc]));
        }
    }

    @Override
    public void forEachValueInDoc(int docId, ValueInDocProc proc) {
        for (int[] ordinal : ordinals) {
            int loc = ordinal[docId];
            if (loc == 0) {
                break;
            }
            proc.onValue(docId, lat[loc], lon[loc]);
        }
    }

    @Override
    public void forEachOrdinalInDoc(int docId, OrdinalInDocProc proc) {
        for (int i = 0; i < ordinals.length; i++) {
            int loc = ordinals[i][docId];
            if (loc == 0) {
                if (i == 0) {
                    proc.onOrdinal(docId, 0);
                }
                break;
            }
            proc.onOrdinal(docId, loc);
        }
    }

    @Override
    public GeoPoint value(int docId) {
        for (int[] ordinal : ordinals) {
            int loc = ordinal[docId];
            if (loc != 0) {
                GeoPoint point = valuesCache.get().get();
                point.latlon(lat[loc], lon[loc]);
                return point;
            }
        }
        return null;
    }

    @Override
    public GeoPoint[] values(int docId) {
        int length = 0;
        for (int[] ordinal : ordinals) {
            if (ordinal[docId] == 0) {
                break;
            }
            length++;
        }
        if (length == 0) {
            return EMPTY_ARRAY;
        }
        GeoPoint[] points;
        if (length < VALUE_CACHE_SIZE) {
            points = valuesArrayCache.get().get()[length];
            for (int i = 0; i < length; i++) {
                int loc = ordinals[i][docId];
                points[i].latlon(lat[loc], lon[loc]);
            }
        } else {
            points = new GeoPoint[length];
            for (int i = 0; i < length; i++) {
                int loc = ordinals[i][docId];
                points[i] = new GeoPoint(lat[loc], lon[loc]);
            }
        }
        return points;
    }

    @Override
    public double latValue(int docId) {
        for (int[] ordinal : ordinals) {
            int loc = ordinal[docId];
            if (loc != 0) {
                return lat[loc];
            }
        }
        return 0;
    }

    @Override
    public double lonValue(int docId) {
        for (int[] ordinal : ordinals) {
            int loc = ordinal[docId];
            if (loc != 0) {
                return lon[loc];
            }
        }
        return 0;
    }

    @Override
    public double[] latValues(int docId) {
        int length = 0;
        for (int[] ordinal : ordinals) {
            if (ordinal[docId] == 0) {
                break;
            }
            length++;
        }
        if (length == 0) {
            return DoubleFieldData.EMPTY_DOUBLE_ARRAY;
        }
        double[] doubles;
        if (length < VALUE_CACHE_SIZE) {
            doubles = valuesLatCache.get().get()[length];
        } else {
            doubles = new double[length];
        }
        for (int i = 0; i < length; i++) {
            doubles[i] = lat[ordinals[i][docId]];
        }
        return doubles;
    }

    @Override
    public double[] lonValues(int docId) {
        int length = 0;
        for (int[] ordinal : ordinals) {
            if (ordinal[docId] != 0) {
                length++;
            }
        }
        if (length == 0) {
            return DoubleFieldData.EMPTY_DOUBLE_ARRAY;
        }
        double[] doubles;
        if (length < VALUE_CACHE_SIZE) {
            doubles = valuesLonCache.get().get()[length];
        } else {
            doubles = new double[length];
        }
        for (int i = 0; i < length; i++) {
            doubles[i] = lon[ordinals[i][docId]];
        }
        return doubles;
    }
}