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

package org.elasticsearch.index.search;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocValuesDocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;

/**
 * A numeric filter that can be much faster than {@link org.apache.lucene.search.NumericRangeFilter} at the
 * expense of loading numeric values of the field to memory using {@link org.elasticsearch.index.cache.field.data.FieldDataCache}.
 */
// TODO: these filters should not iterate over all values but be more selective like Lucene's DocTermOrdsRangeFilter
public abstract class NumericRangeFieldDataFilter<T> extends Filter {
    final IndexNumericFieldData indexFieldData;
    final T lowerVal;
    final T upperVal;
    final boolean includeLower;
    final boolean includeUpper;

    public String getField() {
        return indexFieldData.getFieldNames().indexName();
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

    protected NumericRangeFieldDataFilter(IndexNumericFieldData indexFieldData, T lowerVal, T upperVal, boolean includeLower, boolean includeUpper) {
        this.indexFieldData = indexFieldData;
        this.lowerVal = lowerVal;
        this.upperVal = upperVal;
        this.includeLower = includeLower;
        this.includeUpper = includeUpper;
    }

    @Override
    public final String toString(String field) {
        final StringBuilder sb = new StringBuilder(indexFieldData.getFieldNames().indexName()).append(":");
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
        if (super.equals(o) == false) return false;
        NumericRangeFieldDataFilter other = (NumericRangeFieldDataFilter) o;

        if (!this.indexFieldData.getFieldNames().indexName().equals(other.indexFieldData.getFieldNames().indexName())
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
        int h = super.hashCode();
        h = 31 * h + indexFieldData.getFieldNames().indexName().hashCode();
        h ^= (lowerVal != null) ? lowerVal.hashCode() : 550356204;
        h = (h << 1) | (h >>> 31);  // rotate to distinguish lower from upper
        h ^= (upperVal != null) ? upperVal.hashCode() : -1674416163;
        h ^= (includeLower ? 1549299360 : -365038026) ^ (includeUpper ? 1721088258 : 1948649653);
        return h;
    }

    public static NumericRangeFieldDataFilter<Byte> newByteRange(IndexNumericFieldData indexFieldData, Byte lowerVal, Byte upperVal, boolean includeLower, boolean includeUpper) {
        return new NumericRangeFieldDataFilter<Byte>(indexFieldData, lowerVal, upperVal, includeLower, includeUpper) {
            @Override
            public DocIdSet getDocIdSet(LeafReaderContext ctx, Bits acceptedDocs) throws IOException {
                final byte inclusiveLowerPoint, inclusiveUpperPoint;
                if (lowerVal != null) {
                    byte i = lowerVal.byteValue();
                    if (!includeLower && i == Byte.MAX_VALUE)
                        return null;
                    inclusiveLowerPoint = (byte) (includeLower ? i : (i + 1));
                } else {
                    inclusiveLowerPoint = Byte.MIN_VALUE;
                }
                if (upperVal != null) {
                    byte i = upperVal.byteValue();
                    if (!includeUpper && i == Byte.MIN_VALUE)
                        return null;
                    inclusiveUpperPoint = (byte) (includeUpper ? i : (i - 1));
                } else {
                    inclusiveUpperPoint = Byte.MAX_VALUE;
                }

                if (inclusiveLowerPoint > inclusiveUpperPoint)
                    return null;

                final SortedNumericDocValues values = indexFieldData.load(ctx).getLongValues();
                return new LongRangeMatchDocIdSet(ctx.reader().maxDoc(), acceptedDocs, values, inclusiveLowerPoint, inclusiveUpperPoint);
            }
        };
    }


    public static NumericRangeFieldDataFilter<Short> newShortRange(IndexNumericFieldData indexFieldData, Short lowerVal, Short upperVal, boolean includeLower, boolean includeUpper) {
        return new NumericRangeFieldDataFilter<Short>(indexFieldData, lowerVal, upperVal, includeLower, includeUpper) {
            @Override
            public DocIdSet getDocIdSet(LeafReaderContext ctx, Bits acceptedDocs) throws IOException {
                final short inclusiveLowerPoint, inclusiveUpperPoint;
                if (lowerVal != null) {
                    short i = lowerVal.shortValue();
                    if (!includeLower && i == Short.MAX_VALUE)
                        return null;
                    inclusiveLowerPoint = (short) (includeLower ? i : (i + 1));
                } else {
                    inclusiveLowerPoint = Short.MIN_VALUE;
                }
                if (upperVal != null) {
                    short i = upperVal.shortValue();
                    if (!includeUpper && i == Short.MIN_VALUE)
                        return null;
                    inclusiveUpperPoint = (short) (includeUpper ? i : (i - 1));
                } else {
                    inclusiveUpperPoint = Short.MAX_VALUE;
                }

                if (inclusiveLowerPoint > inclusiveUpperPoint)
                    return null;

                final SortedNumericDocValues values = indexFieldData.load(ctx).getLongValues();
                return new LongRangeMatchDocIdSet(ctx.reader().maxDoc(), acceptedDocs, values, inclusiveLowerPoint, inclusiveUpperPoint);
            }
        };
    }

    public static NumericRangeFieldDataFilter<Integer> newIntRange(IndexNumericFieldData indexFieldData, Integer lowerVal, Integer upperVal, boolean includeLower, boolean includeUpper) {
        return new NumericRangeFieldDataFilter<Integer>(indexFieldData, lowerVal, upperVal, includeLower, includeUpper) {
            @Override
            public DocIdSet getDocIdSet(LeafReaderContext ctx, Bits acceptedDocs) throws IOException {
                final int inclusiveLowerPoint, inclusiveUpperPoint;
                if (lowerVal != null) {
                    int i = lowerVal.intValue();
                    if (!includeLower && i == Integer.MAX_VALUE)
                        return null;
                    inclusiveLowerPoint = includeLower ? i : (i + 1);
                } else {
                    inclusiveLowerPoint = Integer.MIN_VALUE;
                }
                if (upperVal != null) {
                    int i = upperVal.intValue();
                    if (!includeUpper && i == Integer.MIN_VALUE)
                        return null;
                    inclusiveUpperPoint = includeUpper ? i : (i - 1);
                } else {
                    inclusiveUpperPoint = Integer.MAX_VALUE;
                }

                if (inclusiveLowerPoint > inclusiveUpperPoint)
                    return null;

                final SortedNumericDocValues values = indexFieldData.load(ctx).getLongValues();
                return new LongRangeMatchDocIdSet(ctx.reader().maxDoc(), acceptedDocs, values, inclusiveLowerPoint, inclusiveUpperPoint);
            }
        };
    }

    public static NumericRangeFieldDataFilter<Long> newLongRange(IndexNumericFieldData indexFieldData, Long lowerVal, Long upperVal, boolean includeLower, boolean includeUpper) {
        return new NumericRangeFieldDataFilter<Long>(indexFieldData, lowerVal, upperVal, includeLower, includeUpper) {
            @Override
            public DocIdSet getDocIdSet(LeafReaderContext ctx, Bits acceptedDocs) throws IOException {
                final long inclusiveLowerPoint, inclusiveUpperPoint;
                if (lowerVal != null) {
                    long i = lowerVal.longValue();
                    if (!includeLower && i == Long.MAX_VALUE)
                        return null;
                    inclusiveLowerPoint = includeLower ? i : (i + 1l);
                } else {
                    inclusiveLowerPoint = Long.MIN_VALUE;
                }
                if (upperVal != null) {
                    long i = upperVal.longValue();
                    if (!includeUpper && i == Long.MIN_VALUE)
                        return null;
                    inclusiveUpperPoint = includeUpper ? i : (i - 1l);
                } else {
                    inclusiveUpperPoint = Long.MAX_VALUE;
                }

                if (inclusiveLowerPoint > inclusiveUpperPoint)
                    return null;

                final SortedNumericDocValues values = indexFieldData.load(ctx).getLongValues();
                return new LongRangeMatchDocIdSet(ctx.reader().maxDoc(), acceptedDocs, values, inclusiveLowerPoint, inclusiveUpperPoint);

            }
        };
    }

    public static NumericRangeFieldDataFilter<Float> newFloatRange(IndexNumericFieldData indexFieldData, Float lowerVal, Float upperVal, boolean includeLower, boolean includeUpper) {
        return new NumericRangeFieldDataFilter<Float>(indexFieldData, lowerVal, upperVal, includeLower, includeUpper) {
            @Override
            public DocIdSet getDocIdSet(LeafReaderContext ctx, Bits acceptedDocs) throws IOException {
                // we transform the floating point numbers to sortable integers
                // using NumericUtils to easier find the next bigger/lower value
                final float inclusiveLowerPoint, inclusiveUpperPoint;
                if (lowerVal != null) {
                    float f = lowerVal.floatValue();
                    if (!includeUpper && f > 0.0f && Float.isInfinite(f))
                        return null;
                    int i = NumericUtils.floatToSortableInt(f);
                    inclusiveLowerPoint = NumericUtils.sortableIntToFloat(includeLower ? i : (i + 1));
                } else {
                    inclusiveLowerPoint = Float.NEGATIVE_INFINITY;
                }
                if (upperVal != null) {
                    float f = upperVal.floatValue();
                    if (!includeUpper && f < 0.0f && Float.isInfinite(f))
                        return null;
                    int i = NumericUtils.floatToSortableInt(f);
                    inclusiveUpperPoint = NumericUtils.sortableIntToFloat(includeUpper ? i : (i - 1));
                } else {
                    inclusiveUpperPoint = Float.POSITIVE_INFINITY;
                }

                if (inclusiveLowerPoint > inclusiveUpperPoint)
                    return null;

                final SortedNumericDoubleValues values = indexFieldData.load(ctx).getDoubleValues();
                return new DoubleRangeMatchDocIdSet(ctx.reader().maxDoc(), acceptedDocs, values, inclusiveLowerPoint, inclusiveUpperPoint);
            }
        };
    }

    public static NumericRangeFieldDataFilter<Double> newDoubleRange(IndexNumericFieldData indexFieldData, Double lowerVal, Double upperVal, boolean includeLower, boolean includeUpper) {
        return new NumericRangeFieldDataFilter<Double>(indexFieldData, lowerVal, upperVal, includeLower, includeUpper) {
            @Override
            public DocIdSet getDocIdSet(LeafReaderContext ctx, Bits acceptedDocs) throws IOException {
                // we transform the floating point numbers to sortable integers
                // using NumericUtils to easier find the next bigger/lower value
                final double inclusiveLowerPoint, inclusiveUpperPoint;
                if (lowerVal != null) {
                    double f = lowerVal.doubleValue();
                    if (!includeUpper && f > 0.0 && Double.isInfinite(f))
                        return null;
                    long i = NumericUtils.doubleToSortableLong(f);
                    inclusiveLowerPoint = NumericUtils.sortableLongToDouble(includeLower ? i : (i + 1L));
                } else {
                    inclusiveLowerPoint = Double.NEGATIVE_INFINITY;
                }
                if (upperVal != null) {
                    double f = upperVal.doubleValue();
                    if (!includeUpper && f < 0.0 && Double.isInfinite(f))
                        return null;
                    long i = NumericUtils.doubleToSortableLong(f);
                    inclusiveUpperPoint = NumericUtils.sortableLongToDouble(includeUpper ? i : (i - 1L));
                } else {
                    inclusiveUpperPoint = Double.POSITIVE_INFINITY;
                }

                if (inclusiveLowerPoint > inclusiveUpperPoint)
                    return null;

                final SortedNumericDoubleValues values = indexFieldData.load(ctx).getDoubleValues();
                return new DoubleRangeMatchDocIdSet(ctx.reader().maxDoc(), acceptedDocs, values, inclusiveLowerPoint, inclusiveUpperPoint);
            }
        };
    }
    
    private static final class DoubleRangeMatchDocIdSet extends DocValuesDocIdSet {
        private final SortedNumericDoubleValues values;
        private final double inclusiveLowerPoint;
        private final double inclusiveUpperPoint;

        protected DoubleRangeMatchDocIdSet(int maxDoc, Bits acceptDocs, final SortedNumericDoubleValues values,final double inclusiveLowerPoint, final double inclusiveUpperPoint ) {
            super(maxDoc, acceptDocs);
            this.inclusiveLowerPoint = inclusiveLowerPoint;
            this.inclusiveUpperPoint = inclusiveUpperPoint; 
            this.values = values;
        }

        @Override
        protected boolean matchDoc(int doc) {
            values.setDocument(doc);
            int numValues = values.count();
            for (int i = 0; i < numValues; i++) {
                double value = values.valueAt(i);
                if (value >= inclusiveLowerPoint && value <= inclusiveUpperPoint) {
                    return true;
                }
            }
            return false;
        }
        
    }
    
    private static final class LongRangeMatchDocIdSet extends DocValuesDocIdSet {
        private final SortedNumericDocValues values;
        private final long inclusiveLowerPoint;
        private final long inclusiveUpperPoint;

        protected LongRangeMatchDocIdSet(int maxDoc, Bits acceptDocs, final SortedNumericDocValues values,final long inclusiveLowerPoint, final long inclusiveUpperPoint ) {
            super(maxDoc, acceptDocs);
            this.inclusiveLowerPoint = inclusiveLowerPoint;
            this.inclusiveUpperPoint = inclusiveUpperPoint; 
            this.values = values;
        }

        @Override
        protected boolean matchDoc(int doc) {
            values.setDocument(doc);
            int numValues = values.count();
            for (int i = 0; i < numValues; i++) {
                long value = values.valueAt(i);
                if (value >= inclusiveLowerPoint && value <= inclusiveUpperPoint) {
                    return true;
                }
            }
            return false;
        }
        
    }

}
