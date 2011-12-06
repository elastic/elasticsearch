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

package org.elasticsearch.index.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.lucene.docset.DocSet;
import org.elasticsearch.common.lucene.docset.GetDocSet;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.bytes.ByteFieldData;
import org.elasticsearch.index.field.data.doubles.DoubleFieldData;
import org.elasticsearch.index.field.data.floats.FloatFieldData;
import org.elasticsearch.index.field.data.ints.IntFieldData;
import org.elasticsearch.index.field.data.longs.LongFieldData;
import org.elasticsearch.index.field.data.shorts.ShortFieldData;

import java.io.IOException;

/**
 * A numeric filter that can be much faster than {@link org.apache.lucene.search.NumericRangeFilter} at the
 * expense of loading numeric values of the field to memory using {@link org.elasticsearch.index.cache.field.data.FieldDataCache}.
 *
 *
 */
public abstract class NumericRangeFieldDataFilter<T> extends Filter {

    final FieldDataCache fieldDataCache;
    final String field;
    final T lowerVal;
    final T upperVal;
    final boolean includeLower;
    final boolean includeUpper;

    public String getField() {
        return field;
    }

    public T getLowerVal() {
        return lowerVal;
    }

    public T getUpperVal() {
        return upperVal;
    }

    public boolean isIncludeLower() {
        return includeLower;
    }

    public boolean isIncludeUpper() {
        return includeUpper;
    }

    protected NumericRangeFieldDataFilter(FieldDataCache fieldDataCache, String field, T lowerVal, T upperVal, boolean includeLower, boolean includeUpper) {
        this.fieldDataCache = fieldDataCache;
        this.field = field;
        this.lowerVal = lowerVal;
        this.upperVal = upperVal;
        this.includeLower = includeLower;
        this.includeUpper = includeUpper;
    }

    @Override
    public final String toString() {
        final StringBuilder sb = new StringBuilder(field).append(":");
        return sb.append(includeLower ? '[' : '{')
                .append((lowerVal == null) ? "*" : lowerVal.toString())
                .append(" TO ")
                .append((upperVal == null) ? "*" : upperVal.toString())
                .append(includeUpper ? ']' : '}')
                .toString();
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NumericRangeFieldDataFilter)) return false;
        NumericRangeFieldDataFilter other = (NumericRangeFieldDataFilter) o;

        if (!this.field.equals(other.field)
                || this.includeLower != other.includeLower
                || this.includeUpper != other.includeUpper
                ) {
            return false;
        }
        if (this.lowerVal != null ? !this.lowerVal.equals(other.lowerVal) : other.lowerVal != null) return false;
        if (this.upperVal != null ? !this.upperVal.equals(other.upperVal) : other.upperVal != null) return false;
        return true;
    }

    @Override
    public final int hashCode() {
        int h = field.hashCode();
        h ^= (lowerVal != null) ? lowerVal.hashCode() : 550356204;
        h = (h << 1) | (h >>> 31);  // rotate to distinguish lower from upper
        h ^= (upperVal != null) ? upperVal.hashCode() : -1674416163;
        h ^= (includeLower ? 1549299360 : -365038026) ^ (includeUpper ? 1721088258 : 1948649653);
        return h;
    }

    public static NumericRangeFieldDataFilter<Byte> newByteRange(FieldDataCache fieldDataCache, String field, Byte lowerVal, Byte upperVal, boolean includeLower, boolean includeUpper) {
        return new NumericRangeFieldDataFilter<Byte>(fieldDataCache, field, lowerVal, upperVal, includeLower, includeUpper) {
            @Override
            public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
                final byte inclusiveLowerPoint, inclusiveUpperPoint;
                if (lowerVal != null) {
                    byte i = lowerVal.byteValue();
                    if (!includeLower && i == Byte.MAX_VALUE)
                        return DocSet.EMPTY_DOC_SET;
                    inclusiveLowerPoint = (byte) (includeLower ? i : (i + 1));
                } else {
                    inclusiveLowerPoint = Byte.MIN_VALUE;
                }
                if (upperVal != null) {
                    byte i = upperVal.byteValue();
                    if (!includeUpper && i == Byte.MIN_VALUE)
                        return DocSet.EMPTY_DOC_SET;
                    inclusiveUpperPoint = (byte) (includeUpper ? i : (i - 1));
                } else {
                    inclusiveUpperPoint = Byte.MAX_VALUE;
                }

                if (inclusiveLowerPoint > inclusiveUpperPoint)
                    return DocSet.EMPTY_DOC_SET;

                final ByteFieldData fieldData = (ByteFieldData) this.fieldDataCache.cache(FieldDataType.DefaultTypes.BYTE, reader, field);
                return new GetDocSet(reader.maxDoc()) {

                    @Override
                    public boolean isCacheable() {
                        // not cacheable for several reasons:
                        // 1. It is only relevant when _cache is set to true, and then, we really want to create in mem bitset
                        // 2. Its already fast without in mem bitset, since it works with field data
                        return false;
                    }

                    @Override
                    public boolean get(int doc) {
                        if (!fieldData.hasValue(doc)) {
                            return false;
                        }
                        if (fieldData.multiValued()) {
                            byte[] values = fieldData.values(doc);
                            for (byte value : values) {
                                if (value >= inclusiveLowerPoint && value <= inclusiveUpperPoint) {
                                    return true;
                                }
                            }
                            return false;
                        } else {
                            byte value = fieldData.value(doc);
                            return value >= inclusiveLowerPoint && value <= inclusiveUpperPoint;
                        }
                    }
                };
            }
        };
    }


    public static NumericRangeFieldDataFilter<Short> newShortRange(FieldDataCache fieldDataCache, String field, Short lowerVal, Short upperVal, boolean includeLower, boolean includeUpper) {
        return new NumericRangeFieldDataFilter<Short>(fieldDataCache, field, lowerVal, upperVal, includeLower, includeUpper) {
            @Override
            public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
                final short inclusiveLowerPoint, inclusiveUpperPoint;
                if (lowerVal != null) {
                    short i = lowerVal.shortValue();
                    if (!includeLower && i == Short.MAX_VALUE)
                        return DocSet.EMPTY_DOC_SET;
                    inclusiveLowerPoint = (short) (includeLower ? i : (i + 1));
                } else {
                    inclusiveLowerPoint = Short.MIN_VALUE;
                }
                if (upperVal != null) {
                    short i = upperVal.shortValue();
                    if (!includeUpper && i == Short.MIN_VALUE)
                        return DocSet.EMPTY_DOC_SET;
                    inclusiveUpperPoint = (short) (includeUpper ? i : (i - 1));
                } else {
                    inclusiveUpperPoint = Short.MAX_VALUE;
                }

                if (inclusiveLowerPoint > inclusiveUpperPoint)
                    return DocSet.EMPTY_DOC_SET;

                final ShortFieldData fieldData = (ShortFieldData) this.fieldDataCache.cache(FieldDataType.DefaultTypes.SHORT, reader, field);
                return new GetDocSet(reader.maxDoc()) {

                    @Override
                    public boolean isCacheable() {
                        // not cacheable for several reasons:
                        // 1. It is only relevant when _cache is set to true, and then, we really want to create in mem bitset
                        // 2. Its already fast without in mem bitset, since it works with field data
                        return false;
                    }

                    @Override
                    public boolean get(int doc) {
                        if (!fieldData.hasValue(doc)) {
                            return false;
                        }
                        if (fieldData.multiValued()) {
                            short[] values = fieldData.values(doc);
                            for (short value : values) {
                                if (value >= inclusiveLowerPoint && value <= inclusiveUpperPoint) {
                                    return true;
                                }
                            }
                            return false;
                        } else {
                            short value = fieldData.value(doc);
                            return value >= inclusiveLowerPoint && value <= inclusiveUpperPoint;
                        }
                    }
                };
            }
        };
    }

    public static NumericRangeFieldDataFilter<Integer> newIntRange(FieldDataCache fieldDataCache, String field, Integer lowerVal, Integer upperVal, boolean includeLower, boolean includeUpper) {
        return new NumericRangeFieldDataFilter<Integer>(fieldDataCache, field, lowerVal, upperVal, includeLower, includeUpper) {
            @Override
            public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
                final int inclusiveLowerPoint, inclusiveUpperPoint;
                if (lowerVal != null) {
                    int i = lowerVal.intValue();
                    if (!includeLower && i == Integer.MAX_VALUE)
                        return DocSet.EMPTY_DOC_SET;
                    inclusiveLowerPoint = includeLower ? i : (i + 1);
                } else {
                    inclusiveLowerPoint = Integer.MIN_VALUE;
                }
                if (upperVal != null) {
                    int i = upperVal.intValue();
                    if (!includeUpper && i == Integer.MIN_VALUE)
                        return DocSet.EMPTY_DOC_SET;
                    inclusiveUpperPoint = includeUpper ? i : (i - 1);
                } else {
                    inclusiveUpperPoint = Integer.MAX_VALUE;
                }

                if (inclusiveLowerPoint > inclusiveUpperPoint)
                    return DocSet.EMPTY_DOC_SET;

                final IntFieldData fieldData = (IntFieldData) this.fieldDataCache.cache(FieldDataType.DefaultTypes.INT, reader, field);
                return new GetDocSet(reader.maxDoc()) {

                    @Override
                    public boolean isCacheable() {
                        // not cacheable for several reasons:
                        // 1. It is only relevant when _cache is set to true, and then, we really want to create in mem bitset
                        // 2. Its already fast without in mem bitset, since it works with field data
                        return false;
                    }

                    @Override
                    public boolean get(int doc) {
                        if (!fieldData.hasValue(doc)) {
                            return false;
                        }
                        if (fieldData.multiValued()) {
                            int[] values = fieldData.values(doc);
                            for (int value : values) {
                                if (value >= inclusiveLowerPoint && value <= inclusiveUpperPoint) {
                                    return true;
                                }
                            }
                            return false;
                        } else {
                            int value = fieldData.value(doc);
                            return value >= inclusiveLowerPoint && value <= inclusiveUpperPoint;
                        }
                    }
                };
            }
        };
    }

    public static NumericRangeFieldDataFilter<Long> newLongRange(FieldDataCache fieldDataCache, String field, Long lowerVal, Long upperVal, boolean includeLower, boolean includeUpper) {
        return new NumericRangeFieldDataFilter<Long>(fieldDataCache, field, lowerVal, upperVal, includeLower, includeUpper) {
            @Override
            public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
                final long inclusiveLowerPoint, inclusiveUpperPoint;
                if (lowerVal != null) {
                    long i = lowerVal.longValue();
                    if (!includeLower && i == Long.MAX_VALUE)
                        return DocSet.EMPTY_DOC_SET;
                    inclusiveLowerPoint = includeLower ? i : (i + 1l);
                } else {
                    inclusiveLowerPoint = Long.MIN_VALUE;
                }
                if (upperVal != null) {
                    long i = upperVal.longValue();
                    if (!includeUpper && i == Long.MIN_VALUE)
                        return DocSet.EMPTY_DOC_SET;
                    inclusiveUpperPoint = includeUpper ? i : (i - 1l);
                } else {
                    inclusiveUpperPoint = Long.MAX_VALUE;
                }

                if (inclusiveLowerPoint > inclusiveUpperPoint)
                    return DocSet.EMPTY_DOC_SET;

                final LongFieldData fieldData = (LongFieldData) this.fieldDataCache.cache(FieldDataType.DefaultTypes.LONG, reader, field);
                return new GetDocSet(reader.maxDoc()) {

                    @Override
                    public boolean isCacheable() {
                        // not cacheable for several reasons:
                        // 1. It is only relevant when _cache is set to true, and then, we really want to create in mem bitset
                        // 2. Its already fast without in mem bitset, since it works with field data
                        return false;
                    }

                    @Override
                    public boolean get(int doc) {
                        if (!fieldData.hasValue(doc)) {
                            return false;
                        }
                        if (fieldData.multiValued()) {
                            long[] values = fieldData.values(doc);
                            for (long value : values) {
                                if (value >= inclusiveLowerPoint && value <= inclusiveUpperPoint) {
                                    return true;
                                }
                            }
                            return false;
                        } else {
                            long value = fieldData.value(doc);
                            return value >= inclusiveLowerPoint && value <= inclusiveUpperPoint;
                        }
                    }
                };
            }
        };
    }

    public static NumericRangeFieldDataFilter<Float> newFloatRange(FieldDataCache fieldDataCache, String field, Float lowerVal, Float upperVal, boolean includeLower, boolean includeUpper) {
        return new NumericRangeFieldDataFilter<Float>(fieldDataCache, field, lowerVal, upperVal, includeLower, includeUpper) {
            @Override
            public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
                // we transform the floating point numbers to sortable integers
                // using NumericUtils to easier find the next bigger/lower value
                final float inclusiveLowerPoint, inclusiveUpperPoint;
                if (lowerVal != null) {
                    float f = lowerVal.floatValue();
                    if (!includeUpper && f > 0.0f && Float.isInfinite(f))
                        return DocSet.EMPTY_DOC_SET;
                    int i = NumericUtils.floatToSortableInt(f);
                    inclusiveLowerPoint = NumericUtils.sortableIntToFloat(includeLower ? i : (i + 1));
                } else {
                    inclusiveLowerPoint = Float.NEGATIVE_INFINITY;
                }
                if (upperVal != null) {
                    float f = upperVal.floatValue();
                    if (!includeUpper && f < 0.0f && Float.isInfinite(f))
                        return DocSet.EMPTY_DOC_SET;
                    int i = NumericUtils.floatToSortableInt(f);
                    inclusiveUpperPoint = NumericUtils.sortableIntToFloat(includeUpper ? i : (i - 1));
                } else {
                    inclusiveUpperPoint = Float.POSITIVE_INFINITY;
                }

                if (inclusiveLowerPoint > inclusiveUpperPoint)
                    return DocSet.EMPTY_DOC_SET;

                final FloatFieldData fieldData = (FloatFieldData) this.fieldDataCache.cache(FieldDataType.DefaultTypes.FLOAT, reader, field);
                return new GetDocSet(reader.maxDoc()) {

                    @Override
                    public boolean isCacheable() {
                        // not cacheable for several reasons:
                        // 1. It is only relevant when _cache is set to true, and then, we really want to create in mem bitset
                        // 2. Its already fast without in mem bitset, since it works with field data
                        return false;
                    }

                    @Override
                    public boolean get(int doc) {
                        if (!fieldData.hasValue(doc)) {
                            return false;
                        }
                        if (fieldData.multiValued()) {
                            float[] values = fieldData.values(doc);
                            for (float value : values) {
                                if (value >= inclusiveLowerPoint && value <= inclusiveUpperPoint) {
                                    return true;
                                }
                            }
                            return false;
                        } else {
                            float value = fieldData.value(doc);
                            return value >= inclusiveLowerPoint && value <= inclusiveUpperPoint;
                        }
                    }
                };
            }
        };
    }

    public static NumericRangeFieldDataFilter<Double> newDoubleRange(FieldDataCache fieldDataCache, String field, Double lowerVal, Double upperVal, boolean includeLower, boolean includeUpper) {
        return new NumericRangeFieldDataFilter<Double>(fieldDataCache, field, lowerVal, upperVal, includeLower, includeUpper) {
            @Override
            public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
                // we transform the floating point numbers to sortable integers
                // using NumericUtils to easier find the next bigger/lower value
                final double inclusiveLowerPoint, inclusiveUpperPoint;
                if (lowerVal != null) {
                    double f = lowerVal.doubleValue();
                    if (!includeUpper && f > 0.0 && Double.isInfinite(f))
                        return DocSet.EMPTY_DOC_SET;
                    long i = NumericUtils.doubleToSortableLong(f);
                    inclusiveLowerPoint = NumericUtils.sortableLongToDouble(includeLower ? i : (i + 1L));
                } else {
                    inclusiveLowerPoint = Double.NEGATIVE_INFINITY;
                }
                if (upperVal != null) {
                    double f = upperVal.doubleValue();
                    if (!includeUpper && f < 0.0 && Double.isInfinite(f))
                        return DocSet.EMPTY_DOC_SET;
                    long i = NumericUtils.doubleToSortableLong(f);
                    inclusiveUpperPoint = NumericUtils.sortableLongToDouble(includeUpper ? i : (i - 1L));
                } else {
                    inclusiveUpperPoint = Double.POSITIVE_INFINITY;
                }

                if (inclusiveLowerPoint > inclusiveUpperPoint)
                    return DocSet.EMPTY_DOC_SET;

                final DoubleFieldData fieldData = (DoubleFieldData) this.fieldDataCache.cache(FieldDataType.DefaultTypes.DOUBLE, reader, field);
                return new GetDocSet(reader.maxDoc()) {

                    @Override
                    public boolean isCacheable() {
                        // not cacheable for several reasons:
                        // 1. It is only relevant when _cache is set to true, and then, we really want to create in mem bitset
                        // 2. Its already fast without in mem bitset, since it works with field data
                        return false;
                    }

                    @Override
                    public boolean get(int doc) {
                        if (!fieldData.hasValue(doc)) {
                            return false;
                        }
                        if (fieldData.multiValued()) {
                            double[] values = fieldData.values(doc);
                            for (double value : values) {
                                if (value >= inclusiveLowerPoint && value <= inclusiveUpperPoint) {
                                    return true;
                                }
                            }
                            return false;
                        } else {
                            double value = fieldData.value(doc);
                            return value >= inclusiveLowerPoint && value <= inclusiveUpperPoint;
                        }
                    }
                };
            }
        };
    }
}
