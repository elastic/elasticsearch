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

package org.elasticsearch.index.fielddata;

import org.apache.lucene.util.BytesRef;

/**
 */
public abstract class AtomicNumericFieldData implements AtomicFieldData<ScriptDocValues> {

    private boolean isFloat;

    public AtomicNumericFieldData(boolean isFloat) {
        this.isFloat = isFloat;
    }

    public abstract LongValues getLongValues();

    public abstract DoubleValues getDoubleValues();


    @Override
    public ScriptDocValues getScriptValues() {
        if (isFloat) {
            return new ScriptDocValues.Doubles(getDoubleValues());
        } else {
            return new ScriptDocValues.Longs(getLongValues());
        }
    }

    @Override
    public BytesValues getBytesValues() {
        if (isFloat) {
            final DoubleValues values = getDoubleValues();
            return new BytesValues(values.isMultiValued()) {

                @Override
                public boolean hasValue(int docId) {
                    return values.hasValue(docId);
                }

                @Override
                public BytesRef getValueScratch(int docId, BytesRef ret) {
                    if (values.hasValue(docId)) {
                        ret.copyChars(Double.toString(values.getValue(docId)));
                    } else {
                        ret.length = 0;
                    }
                    return ret;
                }

                @Override
                public Iter getIter(int docId) {
                    final DoubleValues.Iter iter = values.getIter(docId);
                    return new BytesValues.Iter() {
                        private final BytesRef spare = new BytesRef();

                        @Override
                        public boolean hasNext() {
                            return iter.hasNext();
                        }

                        @Override
                        public BytesRef next() {
                            spare.copyChars(Double.toString(iter.next()));
                            return spare;
                        }

                        @Override
                        public int hash() {
                            return spare.hashCode();
                        }

                    };
                }
            };
        } else {
            final LongValues values = getLongValues();
            return new BytesValues(values.isMultiValued()) {

                @Override
                public boolean hasValue(int docId) {
                    return values.hasValue(docId);
                }

                @Override
                public BytesRef getValueScratch(int docId, BytesRef ret) {
                    if (values.hasValue(docId)) {
                        ret.copyChars(Long.toString(values.getValue(docId)));
                    } else {
                        ret.length = 0;
                    }
                    return ret;
                }

                @Override
                public Iter getIter(int docId) {
                    final LongValues.Iter iter = values.getIter(docId);
                    return new BytesValues.Iter() {
                        private final BytesRef spare = new BytesRef();

                        @Override
                        public boolean hasNext() {
                            return iter.hasNext();
                        }

                        @Override
                        public BytesRef next() {
                            spare.copyChars(Long.toString(iter.next()));
                            return spare;
                        }

                        @Override
                        public int hash() {
                            return spare.hashCode();
                        }

                    };
                }
            };
        }
    }

    @Override
    public BytesValues getHashedBytesValues() {
        return getBytesValues();
    }

}
