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

package org.elasticsearch.index.field.data.ints;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldCache;
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.common.trove.list.array.TIntArrayList;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.NumericFieldData;
import org.elasticsearch.index.field.data.support.FieldDataLoader;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public abstract class IntFieldData extends NumericFieldData<IntDocFieldData> {

    static final int[] EMPTY_INT_ARRAY = new int[0];

    protected final int[] values;

    protected IntFieldData(String fieldName, int[] values) {
        super(fieldName);
        this.values = values;
    }

    @Override protected long computeSizeInBytes() {
        return RamUsage.NUM_BYTES_INT * values.length + RamUsage.NUM_BYTES_ARRAY_HEADER;
    }

    public final int[] values() {
        return this.values;
    }

    abstract public int value(int docId);

    abstract public int[] values(int docId);

    @Override public IntDocFieldData docFieldData(int docId) {
        return super.docFieldData(docId);
    }

    @Override protected IntDocFieldData createFieldData() {
        return new IntDocFieldData(this);
    }

    @Override public String stringValue(int docId) {
        return Integer.toString(value(docId));
    }

    @Override public void forEachValue(StringValueProc proc) {
        for (int i = 1; i < values.length; i++) {
            proc.onValue(Integer.toString(values[i]));
        }
    }

    @Override public byte byteValue(int docId) {
        return (byte) value(docId);
    }

    @Override public short shortValue(int docId) {
        return (short) value(docId);
    }

    @Override public int intValue(int docId) {
        return value(docId);
    }

    @Override public long longValue(int docId) {
        return (long) value(docId);
    }

    @Override public float floatValue(int docId) {
        return (float) value(docId);
    }

    @Override public double doubleValue(int docId) {
        return (double) value(docId);
    }

    @Override public FieldDataType type() {
        return FieldDataType.DefaultTypes.INT;
    }

    public void forEachValue(ValueProc proc) {
        for (int i = 1; i < values.length; i++) {
            proc.onValue(values[i]);
        }
    }

    public static interface ValueProc {
        void onValue(int value);
    }

    public abstract void forEachValueInDoc(int docId, ValueInDocProc proc);

    public static interface ValueInDocProc {
        void onValue(int docId, int value);

        void onMissing(int docId);
    }

    public static IntFieldData load(IndexReader reader, String field) throws IOException {
        return FieldDataLoader.load(reader, field, new IntTypeLoader());
    }

    static class IntTypeLoader extends FieldDataLoader.FreqsTypeLoader<IntFieldData> {

        private final TIntArrayList terms = new TIntArrayList();

        IntTypeLoader() {
            super();
            // the first one indicates null value
            terms.add(0);
        }

        @Override public void collectTerm(String term) {
            terms.add(FieldCache.NUMERIC_UTILS_INT_PARSER.parseInt(term));
        }

        @Override public IntFieldData buildSingleValue(String field, int[] ordinals) {
            return new SingleValueIntFieldData(field, ordinals, terms.toArray());
        }

        @Override public IntFieldData buildMultiValue(String field, int[][] ordinals) {
            return new MultiValueIntFieldData(field, ordinals, terms.toArray());
        }
    }
}