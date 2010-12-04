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

package org.elasticsearch.index.field.data.shorts;

import org.elasticsearch.common.thread.ThreadLocals;
import org.elasticsearch.index.field.data.doubles.DoubleFieldData;

/**
 * @author kimchy (shay.banon)
 */
public class MultiValueShortFieldData extends ShortFieldData {

    private static final int VALUE_CACHE_SIZE = 10;

    private ThreadLocal<ThreadLocals.CleanableValue<double[][]>> doublesValuesCache = new ThreadLocal<ThreadLocals.CleanableValue<double[][]>>() {
        @Override protected ThreadLocals.CleanableValue<double[][]> initialValue() {
            double[][] value = new double[VALUE_CACHE_SIZE][];
            for (int i = 0; i < value.length; i++) {
                value[i] = new double[i];
            }
            return new ThreadLocals.CleanableValue<double[][]>(value);
        }
    };

    private ThreadLocal<ThreadLocals.CleanableValue<short[][]>> valuesCache = new ThreadLocal<ThreadLocals.CleanableValue<short[][]>>() {
        @Override protected ThreadLocals.CleanableValue<short[][]> initialValue() {
            short[][] value = new short[VALUE_CACHE_SIZE][];
            for (int i = 0; i < value.length; i++) {
                value[i] = new short[i];
            }
            return new ThreadLocals.CleanableValue<short[][]>(value);
        }
    };

    // order with value 0 indicates no value
    private final int[][] ordinals;

    public MultiValueShortFieldData(String fieldName, int[][] ordinals, short[] values) {
        super(fieldName, values);
        this.ordinals = ordinals;
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
            proc.onValue(docId, Short.toString(values[docOrder]));
        }
    }

    @Override public void forEachValueInDoc(int docId, DoubleValueInDocProc proc) {
        int[] docOrders = ordinals[docId];
        if (docOrders == null) {
            return;
        }
        for (int docOrder : docOrders) {
            proc.onValue(docId, values[docOrder]);
        }
    }

    @Override public double[] doubleValues(int docId) {
        int[] docOrders = ordinals[docId];
        if (docOrders == null) {
            return DoubleFieldData.EMPTY_DOUBLE_ARRAY;
        }
        double[] doubles;
        if (docOrders.length < VALUE_CACHE_SIZE) {
            doubles = doublesValuesCache.get().get()[docOrders.length];
        } else {
            doubles = new double[docOrders.length];
        }
        for (int i = 0; i < docOrders.length; i++) {
            doubles[i] = values[docOrders[i]];
        }
        return doubles;
    }

    @Override public short value(int docId) {
        int[] docOrders = ordinals[docId];
        if (docOrders == null) {
            return 0;
        }
        return values[docOrders[0]];
    }

    @Override public short[] values(int docId) {
        int[] docOrders = ordinals[docId];
        if (docOrders == null) {
            return EMPTY_SHORT_ARRAY;
        }
        short[] shorts;
        if (docOrders.length < VALUE_CACHE_SIZE) {
            shorts = valuesCache.get().get()[docOrders.length];
        } else {
            shorts = new short[docOrders.length];
        }
        for (int i = 0; i < docOrders.length; i++) {
            shorts[i] = values[docOrders[i]];
        }
        return shorts;
    }
}