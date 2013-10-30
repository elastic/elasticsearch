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
import org.elasticsearch.index.fielddata.ordinals.Ordinals;

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
        final BytesValues.WithOrdinals values = super.getBytesValues(false);
        return new LongValues.WithOrdinals(values.ordinals()) {
            @Override
            public long getValueByOrd(long ord) {
                assert ord != Ordinals.MISSING_ORDINAL;
                return numericType.toLong(values.getValueByOrd(ord));
            }
        };
    }

    @Override
    public DoubleValues getDoubleValues() {
        final BytesValues.WithOrdinals values = super.getBytesValues(false);
        return new DoubleValues.WithOrdinals(values.ordinals()) {
            @Override
            public double getValueByOrd(long ord) {
                assert ord != Ordinals.MISSING_ORDINAL;
                return numericType.toDouble(values.getValueByOrd(ord));
            }
        };
    }

    @Override
    public BytesValues.WithOrdinals getBytesValues(boolean needsHashes) {
        final BytesValues.WithOrdinals values = super.getBytesValues(needsHashes);
        return new BytesValues.WithOrdinals(values.ordinals()) {
            final BytesRef spare = new BytesRef(16);
            private BytesRef convert(BytesRef input, BytesRef output) {
                if (input.length == 0) {
                    return input;
                }
                if (numericType.isFloatingPoint()) {
                    output.copyChars(Double.toString(numericType.toDouble(input)));
                } else {
                    output.copyChars(Long.toString(numericType.toLong(input)));
                }
                return output;
            }

            @Override
            public BytesRef getValueByOrd(long ord) {
                return convert(values.getValueByOrd(ord), scratch);
            }
        };
    }
}
