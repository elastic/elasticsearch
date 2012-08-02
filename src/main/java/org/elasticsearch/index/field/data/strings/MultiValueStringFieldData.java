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

package org.elasticsearch.index.field.data.strings;

import org.elasticsearch.common.RamUsage;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ThreadLocals;

/**
 *
 */
public class MultiValueStringFieldData extends StringFieldData {

    private static final int VALUE_CACHE_SIZE = 100;

    private static ThreadLocal<ThreadLocals.CleanableValue<String[][]>> valuesCache = new ThreadLocal<ThreadLocals.CleanableValue<String[][]>>() {
        @Override
        protected ThreadLocals.CleanableValue<String[][]> initialValue() {
            String[][] value = new String[VALUE_CACHE_SIZE][];
            for (int i = 0; i < value.length; i++) {
                value[i] = new String[i];
            }
            return new ThreadLocals.CleanableValue<java.lang.String[][]>(value);
        }
    };

    // order with value 0 indicates no value
    private final int[][] ordinals;

    public MultiValueStringFieldData(String fieldName, int[][] ordinals, String[] values) {
        super(fieldName, values);
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
            proc.onValue(docId, values[loc]);
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
    public String value(int docId) {
        for (int[] ordinal : ordinals) {
            int loc = ordinal[docId];
            if (loc != 0) {
                return values[loc];
            }
        }
        return null;
    }

    @Override
    public String[] values(int docId) {
        int length = 0;
        for (int[] ordinal : ordinals) {
            if (ordinal[docId] == 0) {
                break;
            }
            length++;
        }
        if (length == 0) {
            return Strings.EMPTY_ARRAY;
        }
        String[] strings;
        if (length < VALUE_CACHE_SIZE) {
            strings = valuesCache.get().get()[length];
        } else {
            strings = new String[length];
        }
        for (int i = 0; i < length; i++) {
            strings[i] = values[ordinals[i][docId]];
        }
        return strings;
    }
}