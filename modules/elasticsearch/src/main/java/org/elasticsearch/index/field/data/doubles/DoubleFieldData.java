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

package org.elasticsearch.index.field.data.doubles;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldCache;
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.common.trove.list.array.TDoubleArrayList;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.NumericFieldData;
import org.elasticsearch.index.field.data.support.FieldDataLoader;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public abstract class DoubleFieldData extends NumericFieldData<DoubleDocFieldData> {

    public static final double[] EMPTY_DOUBLE_ARRAY = new double[0];

    protected final double[] values;

    protected DoubleFieldData(String fieldName, double[] values) {
        super(fieldName);
        this.values = values;
    }

    @Override protected long computeSizeInBytes() {
        return RamUsage.NUM_BYTES_DOUBLE * values.length + RamUsage.NUM_BYTES_ARRAY_HEADER;
    }

    public final double[] values() {
        return this.values;
    }

    abstract public double value(int docId);

    abstract public double[] values(int docId);

    @Override public DoubleDocFieldData docFieldData(int docId) {
        return super.docFieldData(docId);
    }

    @Override protected DoubleDocFieldData createFieldData() {
        return new DoubleDocFieldData(this);
    }

    @Override public String stringValue(int docId) {
        return Double.toString(value(docId));
    }

    @Override public void forEachValue(StringValueProc proc) {
        for (int i = 1; i < values.length; i++) {
            proc.onValue(Double.toString(values[i]));
        }
    }

    @Override public byte byteValue(int docId) {
        return (byte) value(docId);
    }

    @Override public short shortValue(int docId) {
        return (short) value(docId);
    }

    @Override public int intValue(int docId) {
        return (int) value(docId);
    }

    @Override public long longValue(int docId) {
        return (long) value(docId);
    }

    @Override public float floatValue(int docId) {
        return (float) value(docId);
    }

    @Override public double doubleValue(int docId) {
        return value(docId);
    }

    @Override public FieldDataType type() {
        return FieldDataType.DefaultTypes.DOUBLE;
    }

    public void forEachValue(ValueProc proc) {
        for (int i = 1; i < values.length; i++) {
            proc.onValue(values[i]);
        }
    }

    public static interface ValueProc {
        void onValue(double value);
    }

    public abstract void forEachValueInDoc(int docId, ValueInDocProc proc);

    public static interface ValueInDocProc {
        void onValue(int docId, double value);

        void onMissing(int docId);
    }

    public static DoubleFieldData load(IndexReader reader, String field) throws IOException {
        return FieldDataLoader.load(reader, field, new DoubleTypeLoader());
    }

    static class DoubleTypeLoader extends FieldDataLoader.FreqsTypeLoader<DoubleFieldData> {

        private final TDoubleArrayList terms = new TDoubleArrayList();

        DoubleTypeLoader() {
            super();
            // the first one indicates null value
            terms.add(0);
        }

        @Override public void collectTerm(String term) {
            terms.add(FieldCache.NUMERIC_UTILS_DOUBLE_PARSER.parseDouble(term));
        }

        @Override public DoubleFieldData buildSingleValue(String field, int[] ordinals) {
            return new SingleValueDoubleFieldData(field, ordinals, terms.toArray());
        }

        @Override public DoubleFieldData buildMultiValue(String field, int[][] ordinals) {
            return new MultiValueDoubleFieldData(field, ordinals, terms.toArray());
        }
    }
}