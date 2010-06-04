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

package org.elasticsearch.index.field.strings;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.index.field.FieldData;
import org.elasticsearch.index.field.FieldDataOptions;
import org.elasticsearch.index.field.support.FieldDataLoader;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class StringFieldData extends FieldData {

    protected final String[] values;
    protected final int[] freqs;

    protected StringFieldData(String fieldName, FieldDataOptions options, String[] values, int[] freqs) {
        super(fieldName, options);
        this.values = values;
        this.freqs = freqs;
    }

    abstract public String value(int docId);

    abstract public String[] values(int docId);

    @Override public String stringValue(int docId) {
        return value(docId);
    }

    @Override public Type type() {
        return Type.STRING;
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
        void onValue(String value, int freq);
    }

    public static StringFieldData load(IndexReader reader, String field, FieldDataOptions options) throws IOException {
        return FieldDataLoader.load(reader, field, options, new StringTypeLoader());
    }

    static class StringTypeLoader extends FieldDataLoader.FreqsTypeLoader<StringFieldData> {

        private final ArrayList<String> terms = new ArrayList<String>();

        StringTypeLoader() {
            super();
            // the first one indicates null value
            terms.add(null);
        }

        @Override public void collectTerm(String term) {
            terms.add(term);
        }

        @Override public StringFieldData buildSingleValue(String field, int[] order) {
            return new SingleValueStringFieldData(field, options, order, terms.toArray(new String[terms.size()]), buildFreqs());
        }

        @Override public StringFieldData buildMultiValue(String field, int[][] order) {
            return new MultiValueStringFieldData(field, options, order, terms.toArray(new String[terms.size()]), buildFreqs());
        }
    }
}
