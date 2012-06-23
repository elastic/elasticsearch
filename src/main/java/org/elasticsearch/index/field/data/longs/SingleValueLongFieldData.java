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

package org.elasticsearch.index.field.data.longs;

import org.elasticsearch.common.RamUsage;
import org.elasticsearch.common.util.concurrent.ThreadLocals;
import org.elasticsearch.index.field.data.doubles.DoubleFieldData;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

/**
 *
 */
public class SingleValueLongFieldData extends LongFieldData {

    private ThreadLocal<ThreadLocals.CleanableValue<double[]>> doublesValuesCache = new ThreadLocal<ThreadLocals.CleanableValue<double[]>>() {
        @Override
        protected ThreadLocals.CleanableValue<double[]> initialValue() {
            return new ThreadLocals.CleanableValue<double[]>(new double[1]);
        }
    };

    private ThreadLocal<ThreadLocals.CleanableValue<MutableDateTime[]>> datesValuesCache = new ThreadLocal<ThreadLocals.CleanableValue<MutableDateTime[]>>() {
        @Override
        protected ThreadLocals.CleanableValue<MutableDateTime[]> initialValue() {
            MutableDateTime[] date = new MutableDateTime[1];
            date[0] = new MutableDateTime(DateTimeZone.UTC);
            return new ThreadLocals.CleanableValue<MutableDateTime[]>(date);
        }
    };

    private ThreadLocal<long[]> valuesCache = new ThreadLocal<long[]>() {
        @Override
        protected long[] initialValue() {
            return new long[1];
        }
    };

    // order with value 0 indicates no value
    private final int[] ordinals;

    public SingleValueLongFieldData(String fieldName, int[] ordinals, long[] values) {
        super(fieldName, values);
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
        proc.onValue(docId, Long.toString(values[loc]));
    }

    @Override
    public void forEachValueInDoc(int docId, DoubleValueInDocProc proc) {
        int loc = ordinals[docId];
        if (loc == 0) {
            return;
        }
        proc.onValue(docId, values[loc]);
    }

    @Override
    public void forEachValueInDoc(int docId, LongValueInDocProc proc) {
        int loc = ordinals[docId];
        if (loc == 0) {
            return;
        }
        proc.onValue(docId, values[loc]);
    }

    @Override
    public void forEachValueInDoc(int docId, MissingDoubleValueInDocProc proc) {
        int loc = ordinals[docId];
        if (loc == 0) {
            proc.onMissing(docId);
            return;
        }
        proc.onValue(docId, values[loc]);
    }

    @Override
    public void forEachValueInDoc(int docId, MissingLongValueInDocProc proc) {
        int loc = ordinals[docId];
        if (loc == 0) {
            proc.onMissing(docId);
            return;
        }
        proc.onValue(docId, values[loc]);
    }

    @Override
    public void forEachOrdinalInDoc(int docId, OrdinalInDocProc proc) {
        proc.onOrdinal(docId, ordinals[docId]);
    }

    @Override
    public void forEachValueInDoc(int docId, ValueInDocProc proc) {
        int loc = ordinals[docId];
        if (loc == 0) {
            proc.onMissing(docId);
            return;
        }
        proc.onValue(docId, values[loc]);
    }

    @Override
    public void forEachValueInDoc(int docId, DateValueInDocProc proc) {
        int loc = ordinals[docId];
        if (loc == 0) {
            return;
        }
        MutableDateTime dateTime = dateTimeCache.get().get();
        dateTime.setMillis(values[loc]);
        proc.onValue(docId, dateTime);
    }

    @Override
    public void forEachValueInDoc(int docId, MutableDateTime dateTime, DateValueInDocProc proc) {
        int loc = ordinals[docId];
        if (loc == 0) {
            return;
        }
        dateTime.setMillis(values[loc]);
        proc.onValue(docId, dateTime);
    }

    @Override
    public MutableDateTime[] dates(int docId) {
        int loc = ordinals[docId];
        if (loc == 0) {
            return EMPTY_DATETIME_ARRAY;
        }
        MutableDateTime[] ret = datesValuesCache.get().get();
        ret[0].setMillis(values[loc]);
        return ret;
    }

    @Override
    public double[] doubleValues(int docId) {
        int loc = ordinals[docId];
        if (loc == 0) {
            return DoubleFieldData.EMPTY_DOUBLE_ARRAY;
        }
        double[] ret = doublesValuesCache.get().get();
        ret[0] = values[loc];
        return ret;
    }

    @Override
    public long value(int docId) {
        return values[ordinals[docId]];
    }

    @Override
    public long[] values(int docId) {
        int loc = ordinals[docId];
        if (loc == 0) {
            return EMPTY_LONG_ARRAY;
        }
        long[] ret = valuesCache.get();
        ret[0] = values[loc];
        return ret;
    }
}