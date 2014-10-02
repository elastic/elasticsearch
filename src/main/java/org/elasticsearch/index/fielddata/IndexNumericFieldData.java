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

import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;
import org.elasticsearch.index.mapper.core.BooleanFieldMapper;

/**
 */
public interface IndexNumericFieldData extends IndexFieldData<AtomicNumericFieldData> {

    public static enum NumericType {
        BOOLEAN(1, false, SortField.Type.INT, 0, 1) {
            @Override
            public long toLong(BytesRef indexForm) {
                if (indexForm.equals(BooleanFieldMapper.Values.FALSE)) {
                    return 0;
                } else if (indexForm.equals(BooleanFieldMapper.Values.TRUE)) {
                    return 1;
                } else {
                    throw new ElasticsearchIllegalArgumentException("Cannot convert " + indexForm + " to a boolean");
                }
            }

            @Override
            public void toIndexForm(Number number, BytesRefBuilder bytes) {
                bytes.append(number.intValue() != 0 ? BooleanFieldMapper.Values.TRUE : BooleanFieldMapper.Values.FALSE);
            }

            @Override
            public Number toNumber(BytesRef indexForm) {
                return toLong(indexForm);
            }

        },
        BYTE(8, false, SortField.Type.INT, Byte.MIN_VALUE, Byte.MAX_VALUE) {
            @Override
            public long toLong(BytesRef indexForm) {
                return INT.toLong(indexForm);
            }

            @Override
            public void toIndexForm(Number number, BytesRefBuilder bytes) {
                INT.toIndexForm(number, bytes);
            }

            @Override
            public Number toNumber(BytesRef indexForm) {
                return INT.toNumber(indexForm);
            }
        },
        SHORT(16, false, SortField.Type.INT, Short.MIN_VALUE, Short.MAX_VALUE) {
            @Override
            public long toLong(BytesRef indexForm) {
                return INT.toLong(indexForm);
            }

            @Override
            public void toIndexForm(Number number, BytesRefBuilder bytes) {
                INT.toIndexForm(number, bytes);
            }

            @Override
            public Number toNumber(BytesRef indexForm) {
                return INT.toNumber(indexForm);
            }
        },
        INT(32, false, SortField.Type.INT, Integer.MIN_VALUE, Integer.MAX_VALUE) {
            @Override
            public long toLong(BytesRef indexForm) {
                return NumericUtils.prefixCodedToInt(indexForm);
            }

            @Override
            public void toIndexForm(Number number, BytesRefBuilder bytes) {
                NumericUtils.intToPrefixCodedBytes(number.intValue(), 0, bytes);
            }

            @Override
            public Number toNumber(BytesRef indexForm) {
                return NumericUtils.prefixCodedToInt(indexForm);
            }
        },
        LONG(64, false, SortField.Type.LONG, Long.MIN_VALUE, Long.MAX_VALUE) {
            @Override
            public long toLong(BytesRef indexForm) {
                return NumericUtils.prefixCodedToLong(indexForm);
            }

            @Override
            public void toIndexForm(Number number, BytesRefBuilder bytes) {
                NumericUtils.longToPrefixCodedBytes(number.longValue(), 0, bytes);
            }

            @Override
            public Number toNumber(BytesRef indexForm) {
                return NumericUtils.prefixCodedToLong(indexForm);
            }
        },
        FLOAT(32, true, SortField.Type.FLOAT, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY) {
            @Override
            public double toDouble(BytesRef indexForm) {
                return NumericUtils.sortableIntToFloat(NumericUtils.prefixCodedToInt(indexForm));
            }

            @Override
            public void toIndexForm(Number number, BytesRefBuilder bytes) {
                NumericUtils.intToPrefixCodedBytes(NumericUtils.floatToSortableInt(number.floatValue()), 0, bytes);
            }

            @Override
            public Number toNumber(BytesRef indexForm) {
                return NumericUtils.sortableIntToFloat(NumericUtils.prefixCodedToInt(indexForm));
            }
        },
        DOUBLE(64, true, SortField.Type.DOUBLE, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY) {
            @Override
            public double toDouble(BytesRef indexForm) {
                return NumericUtils.sortableLongToDouble(NumericUtils.prefixCodedToLong(indexForm));
            }

            @Override
            public void toIndexForm(Number number, BytesRefBuilder bytes) {
                NumericUtils.longToPrefixCodedBytes(NumericUtils.doubleToSortableLong(number.doubleValue()), 0, bytes);
            }

            @Override
            public Number toNumber(BytesRef indexForm) {
                return NumericUtils.sortableLongToDouble(NumericUtils.prefixCodedToLong(indexForm));
            }
        };

        private final int requiredBits;
        private final boolean floatingPoint;
        private final SortField.Type type;
        private final Number minValue, maxValue;

        private NumericType(int requiredBits, boolean floatingPoint, SortField.Type type, Number minValue, Number maxValue) {
            this.requiredBits = requiredBits;
            this.floatingPoint = floatingPoint;
            this.type = type;
            this.minValue = minValue;
            this.maxValue = maxValue;
        }

        public final SortField.Type sortFieldType() {
            return type;
        }

        public final Number minValue() {
            return minValue;
        }

        public final Number maxValue() {
            return maxValue;
        }

        public final boolean isFloatingPoint() {
            return floatingPoint;
        }

        public final int requiredBits() {
            return requiredBits;
        }

        public abstract void toIndexForm(Number number, BytesRefBuilder bytes);

        public long toLong(BytesRef indexForm) {
            return (long) toDouble(indexForm);
        }

        public double toDouble(BytesRef indexForm) {
            return (double) toLong(indexForm);
        }

        public abstract Number toNumber(BytesRef indexForm);

        public final TermsEnum wrapTermsEnum(TermsEnum termsEnum) {
            if (requiredBits() == 1) { // boolean, no prefix-terms
                return termsEnum;
            } else if (requiredBits() > 32) {
                return OrdinalsBuilder.wrapNumeric64Bit(termsEnum);
            } else {
                return OrdinalsBuilder.wrapNumeric32Bit(termsEnum);
            }
        }
    }

    NumericType getNumericType();
}
