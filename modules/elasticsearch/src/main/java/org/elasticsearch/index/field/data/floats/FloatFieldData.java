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

package org.elasticsearch.index.field.data.floats;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldCache;
import org.elasticsearch.common.trove.TFloatArrayList;
import org.elasticsearch.index.field.data.FieldDataOptions;
import org.elasticsearch.index.field.data.NumericFieldData;
import org.elasticsearch.index.field.data.support.FieldDataLoader;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public abstract class FloatFieldData extends NumericFieldData<FloatDocFieldData> {

    static final float[] EMPTY_FLOAT_ARRAY = new float[0];

    protected final float[] values;
    protected final int[] freqs;

    protected FloatFieldData(String fieldName, FieldDataOptions options, float[] values, int[] freqs) {
        super(fieldName, options);
        this.values = values;
        this.freqs = freqs;
    }

    abstract public float value(int docId);

    abstract public float[] values(int docId);

    @Override public FloatDocFieldData docFieldData(int docId) {
        return super.docFieldData(docId);
    }

    @Override protected FloatDocFieldData createFieldData() {
        return new FloatDocFieldData(this);
    }

    @Override public String stringValue(int docId) {
        return Float.toString(value(docId));
    }

    @Override public void forEachValue(StringValueProc proc) {
        if (freqs == null) {
            for (int i = 1; i < values.length; i++) {
                proc.onValue(Float.toString(values[i]), -1);
            }
        } else {
            for (int i = 1; i < values.length; i++) {
                proc.onValue(Float.toString(values[i]), freqs[i]);
            }
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
        return value(docId);
    }

    @Override public double doubleValue(int docId) {
        return (double) value(docId);
    }

    @Override public Type type() {
        return Type.FLOAT;
    }

    public void forEachValue(ValueProc proc) {
        if (freqs == null) {
            for (int i = 1; i < values.length; i++) {
                proc.onValue(values[i], -1);
            }
        } else {
            for (int i = 1; i < values.length; i++) {
                proc.onValue(values[i], freqs[i]);
            }
        }
    }

    public static interface ValueProc {
        void onValue(float value, int freq);
    }


    public static FloatFieldData load(IndexReader reader, String field, FieldDataOptions options) throws IOException {
        return FieldDataLoader.load(reader, field, options, new FloatTypeLoader());
    }

    static class FloatTypeLoader extends FieldDataLoader.FreqsTypeLoader<FloatFieldData> {

        private final TFloatArrayList terms = new TFloatArrayList();

        FloatTypeLoader() {
            super();
            // the first one indicates null value
            terms.add(0);
        }

        @Override public void collectTerm(String term) {
            terms.add(FieldCache.NUMERIC_UTILS_FLOAT_PARSER.parseFloat(term));
        }

        @Override public FloatFieldData buildSingleValue(String field, int[] order) {
            return new SingleValueFloatFieldData(field, options, order, terms.toNativeArray(), buildFreqs());
        }

        @Override public FloatFieldData buildMultiValue(String field, int[][] order) {
            return new MultiValueFloatFieldData(field, options, order, terms.toNativeArray(), buildFreqs());
        }
    }
}