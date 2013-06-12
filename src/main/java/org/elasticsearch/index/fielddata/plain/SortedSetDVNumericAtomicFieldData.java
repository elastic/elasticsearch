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

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;

public class SortedSetDVNumericAtomicFieldData extends SortedSetDVAtomicFieldData implements AtomicNumericFieldData {

    private final NumericType numericType;

    SortedSetDVNumericAtomicFieldData(AtomicReader reader, String field, NumericType numericType) {
        super(reader, field);
        this.numericType = numericType;
    }

    @Override
    public boolean isValuesOrdered() {
        return false;
    }

    @Override
    public ScriptDocValues getScriptValues() {
        if (numericType.isFloatingPoint()) {
            return new ScriptDocValues.Doubles(getDoubleValues());
        } else {
            return new ScriptDocValues.Longs(getLongValues());
        }
    }

    @Override
    public LongValues getLongValues() {
        final BytesValues.WithOrdinals values = super.getBytesValues();
        return new LongValues.WithOrdinals(values.ordinals()) {
            @Override
            public long getValueByOrd(long ord) {
                if (ord == 0L) {
                    return 0L;
                }
                return numericType.toLong(values.getValueByOrd(ord));
            }
        };
    }

    @Override
    public DoubleValues getDoubleValues() {
        final BytesValues.WithOrdinals values = super.getBytesValues();
        return new DoubleValues.WithOrdinals(values.ordinals()) {
            @Override
            public double getValueByOrd(long ord) {
                if (ord == 0L) {
                    return 0d;
                }
                return numericType.toDouble(values.getValueByOrd(ord));
            }
        };
    }

    @Override
    public BytesValues.WithOrdinals getBytesValues() {
        final BytesValues.WithOrdinals values = super.getBytesValues();
        return new BytesValues.WithOrdinals(values.ordinals()) {

            BytesRef spare = new BytesRef(16);
            Iter inIter;
            Iter iter = new Iter() {

                BytesRef current = null;

                @Override
                public boolean hasNext() {
                    return inIter.hasNext();
                }

                @Override
                public BytesRef next() {
                    return current = convert(inIter.next());
                }

                @Override
                public int hash() {
                    return current.hashCode();
                }

            };

            private BytesRef convert(BytesRef spare) {
                if (spare.length == 0) {
                    return spare;
                }
                if (numericType.isFloatingPoint()) {
                    return new BytesRef(Double.toString(numericType.toDouble(spare)));
                } else {
                    return new BytesRef(Long.toString(numericType.toLong(spare)));
                }
            }

            @Override
            public BytesRef getValueScratchByOrd(long ord, BytesRef ret) {
                return convert(values.getValueScratchByOrd(ord, spare));
            }

            @Override
            public Iter getIter(int docId) {
                inIter = values.getIter(docId);
                return iter;
            }

        };
    }

    @Override
    public BytesValues.WithOrdinals getHashedBytesValues() {
        return getBytesValues();
    }

}
