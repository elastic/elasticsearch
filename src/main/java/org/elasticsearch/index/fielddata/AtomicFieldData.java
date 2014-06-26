/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.index.fielddata;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.ScriptDocValues.Strings;

/**
 * The thread safe {@link org.apache.lucene.index.AtomicReader} level cache of the data.
 */
public interface AtomicFieldData<Script extends ScriptDocValues> extends Accountable {

    /**
     * Use a non thread safe (lightweight) view of the values as bytes.
     */
    BytesValues getBytesValues();

    /**
     * Returns a "scripting" based values.
     */
    Script getScriptValues();

    /**
     * Close the field data.
     */
    void close();

    interface WithOrdinals<Script extends ScriptDocValues> extends AtomicFieldData<Script> {

        public static final WithOrdinals<ScriptDocValues.Strings> EMPTY = new WithOrdinals<ScriptDocValues.Strings>() {

            @Override
            public Strings getScriptValues() {
                return new ScriptDocValues.Strings(getBytesValues());
            }

            @Override
            public void close() {
            }

            @Override
            public long ramBytesUsed() {
                return 0;
            }

            @Override
            public BytesValues.WithOrdinals getBytesValues() {
                return new BytesValues.WithOrdinals(false) {

                    @Override
                    public int setDocument(int docId) {
                        return 0;
                    }

                    @Override
                    public long nextOrd() {
                        return MISSING_ORDINAL;
                    }

                    @Override
                    public BytesRef getValueByOrd(long ord) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public long getOrd(int docId) {
                        return MISSING_ORDINAL;
                    }

                    @Override
                    public long getMaxOrd() {
                        return 0;
                    }
                };
            }

        };

        /**
         * Use a non thread safe (lightweight) view of the values as bytes.
         * @param needsHashes
         */
        BytesValues.WithOrdinals getBytesValues();

    }

    /**
     * This enum provides information about the order of the values for
     * a given document. For instance {@link BytesValues} by default
     * return values in {@link #BYTES} order but if the interface
     * wraps a numeric variant the sort order might change to {@link #NUMERIC}.
     * In that case the values might not be returned in byte sort order but in numeric
     * order instead while maintaining the property of <tt>N < N+1</tt> during the
     * value iterations.
     *
     * @see org.elasticsearch.index.fielddata.BytesValues#getOrder()
     * @see org.elasticsearch.index.fielddata.DoubleValues#getOrder()
     * @see org.elasticsearch.index.fielddata.LongValues#getOrder()
     */
    public enum Order {
        /**
         * Donates Byte sort order
         */
        BYTES,
        /**
         * Donates Numeric sort order
         */
        NUMERIC,
        /**
         * Donates custom sort order
         */
        CUSTOM,
        /**
         * Donates no sort order
         */
        NONE
    }
}
