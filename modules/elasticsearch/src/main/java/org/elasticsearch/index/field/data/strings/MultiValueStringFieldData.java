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

package org.elasticsearch.index.field.data.strings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.thread.ThreadLocals;
import org.elasticsearch.index.field.data.FieldDataOptions;

/**
 * @author kimchy (shay.banon)
 */
public class MultiValueStringFieldData extends StringFieldData {

    private static final int VALUE_CACHE_SIZE = 100;

    private static ThreadLocal<ThreadLocals.CleanableValue<String[][]>> valuesCache = new ThreadLocal<ThreadLocals.CleanableValue<String[][]>>() {
        @Override protected ThreadLocals.CleanableValue<String[][]> initialValue() {
            String[][] value = new String[VALUE_CACHE_SIZE][];
            for (int i = 0; i < value.length; i++) {
                value[i] = new String[i];
            }
            return new ThreadLocals.CleanableValue<java.lang.String[][]>(value);
        }
    };

    // order with value 0 indicates no value
    private final int[][] order;

    public MultiValueStringFieldData(String fieldName, FieldDataOptions options, int[][] order, String[] values, int[] freqs) {
        super(fieldName, options, values, freqs);
        this.order = order;
    }

    @Override public boolean multiValued() {
        return true;
    }

    @Override public boolean hasValue(int docId) {
        return order[docId] != null;
    }

    @Override public void forEachValueInDoc(int docId, StringValueInDocProc proc) {
        int[] docOrders = order[docId];
        if (docOrders == null) {
            return;
        }
        for (int docOrder : docOrders) {
            proc.onValue(docId, values[docOrder]);
        }
    }

    @Override public String value(int docId) {
        int[] docOrders = order[docId];
        if (docOrders == null) {
            return null;
        }
        return values[docOrders[0]];
    }

    @Override public String[] values(int docId) {
        int[] docOrders = order[docId];
        if (docOrders == null) {
            return Strings.EMPTY_ARRAY;
        }
        String[] strings;
        if (docOrders.length < VALUE_CACHE_SIZE) {
            strings = valuesCache.get().get()[docOrders.length];
        } else {
            strings = new String[docOrders.length];
        }
        for (int i = 0; i < docOrders.length; i++) {
            strings[i] = values[docOrders[i]];
        }
        return strings;
    }
}