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

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.document.HalfFloatPoint;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.AbstractSortedNumericDocValues;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.fieldcomparator.DoubleValuesComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.FloatValuesComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.LongValuesComparatorSource;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.function.LongUnaryOperator;

/**
 * FieldData backed by {@link LeafReader#getSortedNumericDocValues(String)}
 * @see DocValuesType#SORTED_NUMERIC
 */
public class SortedNumericIndexFieldData implements IndexNumericFieldData {
    public static class Builder implements IndexFieldData.Builder {

        private final NumericType numericType;

        public Builder(NumericType numericType) {
            this.numericType = numericType;
        }

        @Override
        public SortedNumericIndexFieldData build(
            IndexSettings indexSettings,
            MappedFieldType fieldType,
            IndexFieldDataCache cache,
            CircuitBreakerService breakerService,
            MapperService mapperService
        ) {
            final String fieldName = fieldType.name();
            return new SortedNumericIndexFieldData(indexSettings.getIndex(), fieldName, numericType);
        }
    }

    private final NumericType numericType;
    protected final Index index;
    protected final String fieldName;

    public SortedNumericIndexFieldData(Index index, String fieldName, NumericType numericType) {
        this.index = index;
        this.fieldName = fieldName;
        this.numericType = Objects.requireNonNull(numericType);
    }

    @Override
    public final String getFieldName() {
        return fieldName;
    }

    @Override
    public final void clear() {
        // can't do
    }

    @Override
    public final Index index() {
        return index;
    }

    /**
     * Returns the {@link SortField} to used for sorting.
     * Values are casted to the provided <code>targetNumericType</code> type if it doesn't
     * match the field's <code>numericType</code>.
     */
    public SortField sortField(NumericType targetNumericType, Object missingValue, MultiValueMode sortMode,
                               Nested nested, boolean reverse) {
        final XFieldComparatorSource source = comparatorSource(targetNumericType, missingValue, sortMode, nested);

        /**
         * Check if we can use a simple {@link SortedNumericSortField} compatible with index sorting and
         * returns a custom sort field otherwise.
         */
        if (nested != null
                || (sortMode != MultiValueMode.MAX && sortMode != MultiValueMode.MIN)
                || numericType == NumericType.HALF_FLOAT
                || targetNumericType != numericType) {
            return new SortField(fieldName, source, reverse);
        }

        final SortField sortField;
        final SortedNumericSelector.Type selectorType = sortMode == MultiValueMode.MAX ?
            SortedNumericSelector.Type.MAX : SortedNumericSelector.Type.MIN;
        switch (numericType) {
            case FLOAT:
                sortField = new SortedNumericSortField(fieldName, SortField.Type.FLOAT, reverse, selectorType);
                break;

            case DOUBLE:
                sortField = new SortedNumericSortField(fieldName, SortField.Type.DOUBLE, reverse, selectorType);
                break;

            default:
                assert !numericType.isFloatingPoint();
                sortField = new SortedNumericSortField(fieldName, SortField.Type.LONG, reverse, selectorType);
                break;
        }
        sortField.setMissingValue(source.missingObject(missingValue, reverse));
        return sortField;
    }

    @Override
    public SortField sortField(Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
        return sortField(numericType, missingValue, sortMode, nested, reverse);
    }

    /**
     * Builds a {@linkplain BucketedSort} for the {@code targetNumericType},
     * casting the values if their native type doesn't match.
     */
    public BucketedSort newBucketedSort(NumericType targetNumericType, BigArrays bigArrays, @Nullable Object missingValue,
            MultiValueMode sortMode, Nested nested, SortOrder sortOrder, DocValueFormat format,
            int bucketSize, BucketedSort.ExtraData extra) {
        return comparatorSource(targetNumericType, missingValue, sortMode, nested)
                .newBucketedSort(bigArrays, sortOrder, format, bucketSize, extra);
    }

    @Override
    public BucketedSort newBucketedSort(BigArrays bigArrays, @Nullable Object missingValue, MultiValueMode sortMode, Nested nested,
            SortOrder sortOrder, DocValueFormat format, int bucketSize, BucketedSort.ExtraData extra) {
        return newBucketedSort(numericType, bigArrays, missingValue, sortMode, nested, sortOrder, format, bucketSize, extra);
    }

    private XFieldComparatorSource comparatorSource(NumericType targetNumericType, @Nullable Object missingValue, MultiValueMode sortMode,
            Nested nested) {
        switch (targetNumericType) {
        case HALF_FLOAT:
        case FLOAT:
            return new FloatValuesComparatorSource(this, missingValue, sortMode, nested);
        case DOUBLE:
            return new DoubleValuesComparatorSource(this, missingValue, sortMode, nested);
        case DATE:
            if (numericType == NumericType.DATE_NANOSECONDS) {
                // converts date values to nanosecond resolution
                return new LongValuesComparatorSource(this, missingValue,
                    sortMode, nested, dvs -> convertNanosToMillis(dvs));
            }
            return new LongValuesComparatorSource(this, missingValue, sortMode, nested);
        case DATE_NANOSECONDS:
            if (numericType == NumericType.DATE) {
                // converts date_nanos values to millisecond resolution
                return new LongValuesComparatorSource(this, missingValue,
                    sortMode, nested, dvs -> convertMillisToNanos(dvs));
            }
            return new LongValuesComparatorSource(this, missingValue, sortMode, nested);
        default:
            assert !targetNumericType.isFloatingPoint();
            return new LongValuesComparatorSource(this, missingValue, sortMode, nested);
        }
    }

    @Override
    public NumericType getNumericType() {
        return numericType;
    }

    @Override
    public LeafNumericFieldData loadDirect(LeafReaderContext context) throws Exception {
        return load(context);
    }

    @Override
    public LeafNumericFieldData load(LeafReaderContext context) {
        final LeafReader reader = context.reader();
        final String field = fieldName;

        switch (numericType) {
            case HALF_FLOAT:
                return new SortedNumericHalfFloatFieldData(reader, field);
            case FLOAT:
                return new SortedNumericFloatFieldData(reader, field);
            case DOUBLE:
                return new SortedNumericDoubleFieldData(reader, field);
            case DATE_NANOSECONDS:
                return new NanoSecondFieldData(reader, field, numericType);
            default:
                return new SortedNumericLongFieldData(reader, field, numericType);
        }
    }

    /**
     * A small helper class that can be configured to load nanosecond field data either in nanosecond resolution retaining the original
     * values or in millisecond resolution converting the nanosecond values to milliseconds
     */
    public final class NanoSecondFieldData extends LeafLongFieldData {

        private final LeafReader reader;
        private final String fieldName;

        NanoSecondFieldData(LeafReader reader, String fieldName, NumericType numericType) {
            super(0L, numericType);
            this.reader = reader;
            this.fieldName = fieldName;
        }

        @Override
        public SortedNumericDocValues getLongValues() {
            return convertNanosToMillis(getLongValuesAsNanos());
        }

        public SortedNumericDocValues getLongValuesAsNanos() {
            try {
                return DocValues.getSortedNumeric(reader, fieldName);
            } catch (IOException e) {
                throw new IllegalStateException("Cannot load doc values", e);
            }
        }
    }

    /**
     * FieldData implementation for integral types.
     * <p>
     * Order of values within a document is consistent with
     * {@link Long#compareTo(Long)}.
     * <p>
     * Although the API is multi-valued, most codecs in Lucene specialize
     * for the case where documents have at most one value. In this case
     * {@link DocValues#unwrapSingleton(SortedNumericDocValues)} will return
     * the underlying single-valued NumericDocValues representation.
     */
    static final class SortedNumericLongFieldData extends LeafLongFieldData {
        final LeafReader reader;
        final String field;

        SortedNumericLongFieldData(LeafReader reader, String field, NumericType numericType) {
            super(0L, numericType);
            this.reader = reader;
            this.field = field;
        }

        @Override
        public SortedNumericDocValues getLongValues() {
            try {
                return DocValues.getSortedNumeric(reader, field);
            } catch (IOException e) {
                throw new IllegalStateException("Cannot load doc values", e);
            }
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }
    }

    /**
     * FieldData implementation for 16-bit float values.
     * <p>
     * Order of values within a document is consistent with
     * {@link Float#compareTo(Float)}, hence the following reversible
     * transformation is applied at both index and search:
     * {@code bits ^ (bits >> 15) & 0x7fff}
     * <p>
     * Although the API is multi-valued, most codecs in Lucene specialize
     * for the case where documents have at most one value. In this case
     * {@link FieldData#unwrapSingleton(SortedNumericDoubleValues)} will return
     * the underlying single-valued NumericDoubleValues representation.
     */
    static final class SortedNumericHalfFloatFieldData extends LeafDoubleFieldData {
        final LeafReader reader;
        final String field;

        SortedNumericHalfFloatFieldData(LeafReader reader, String field) {
            super(0L);
            this.reader = reader;
            this.field = field;
        }

        @Override
        public SortedNumericDoubleValues getDoubleValues() {
            try {
                SortedNumericDocValues raw = DocValues.getSortedNumeric(reader, field);

                NumericDocValues single = DocValues.unwrapSingleton(raw);
                if (single != null) {
                    return FieldData.singleton(new SingleHalfFloatValues(single));
                } else {
                    return new MultiHalfFloatValues(raw);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Cannot load doc values", e);
            }
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }
    }

    /**
     * Wraps a NumericDocValues and exposes a single 16-bit float per document.
     */
    static final class SingleHalfFloatValues extends NumericDoubleValues {
        final NumericDocValues in;

        SingleHalfFloatValues(NumericDocValues in) {
            this.in = in;
        }

        @Override
        public double doubleValue() throws IOException {
            return HalfFloatPoint.sortableShortToHalfFloat((short) in.longValue());
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
            return in.advanceExact(doc);
        }
    }

    /**
     * Wraps a SortedNumericDocValues and exposes multiple 16-bit floats per document.
     */
    static final class MultiHalfFloatValues extends SortedNumericDoubleValues {
        final SortedNumericDocValues in;

        MultiHalfFloatValues(SortedNumericDocValues in) {
            this.in = in;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return in.advanceExact(target);
        }

        @Override
        public double nextValue() throws IOException {
            return HalfFloatPoint.sortableShortToHalfFloat((short) in.nextValue());
        }

        @Override
        public int docValueCount() {
            return in.docValueCount();
        }
    }

    /**
     * FieldData implementation for 32-bit float values.
     * <p>
     * Order of values within a document is consistent with
     * {@link Float#compareTo(Float)}, hence the following reversible
     * transformation is applied at both index and search:
     * {@code bits ^ (bits >> 31) & 0x7fffffff}
     * <p>
     * Although the API is multi-valued, most codecs in Lucene specialize
     * for the case where documents have at most one value. In this case
     * {@link FieldData#unwrapSingleton(SortedNumericDoubleValues)} will return
     * the underlying single-valued NumericDoubleValues representation.
     */
    static final class SortedNumericFloatFieldData extends LeafDoubleFieldData {
        final LeafReader reader;
        final String field;

        SortedNumericFloatFieldData(LeafReader reader, String field) {
            super(0L);
            this.reader = reader;
            this.field = field;
        }

        @Override
        public SortedNumericDoubleValues getDoubleValues() {
            try {
                SortedNumericDocValues raw = DocValues.getSortedNumeric(reader, field);

                NumericDocValues single = DocValues.unwrapSingleton(raw);
                if (single != null) {
                    return FieldData.singleton(new SingleFloatValues(single));
                } else {
                    return new MultiFloatValues(raw);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Cannot load doc values", e);
            }
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }
    }

    /**
     * Wraps a NumericDocValues and exposes a single 32-bit float per document.
     */
    static final class SingleFloatValues extends NumericDoubleValues {
        final NumericDocValues in;

        SingleFloatValues(NumericDocValues in) {
            this.in = in;
        }

        @Override
        public double doubleValue() throws IOException {
            return NumericUtils.sortableIntToFloat((int) in.longValue());
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
            return in.advanceExact(doc);
        }
    }

    /**
     * Wraps a SortedNumericDocValues and exposes multiple 32-bit floats per document.
     */
    static final class MultiFloatValues extends SortedNumericDoubleValues {
        final SortedNumericDocValues in;

        MultiFloatValues(SortedNumericDocValues in) {
            this.in = in;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return in.advanceExact(target);
        }

        @Override
        public double nextValue() throws IOException {
            return NumericUtils.sortableIntToFloat((int) in.nextValue());
        }

        @Override
        public int docValueCount() {
            return in.docValueCount();
        }
    }

    /**
     * FieldData implementation for 64-bit double values.
     * <p>
     * Order of values within a document is consistent with
     * {@link Double#compareTo(Double)}, hence the following reversible
     * transformation is applied at both index and search:
     * {@code bits ^ (bits >> 63) & 0x7fffffffffffffffL}
     * <p>
     * Although the API is multi-valued, most codecs in Lucene specialize
     * for the case where documents have at most one value. In this case
     * {@link FieldData#unwrapSingleton(SortedNumericDoubleValues)} will return
     * the underlying single-valued NumericDoubleValues representation.
     */
    static final class SortedNumericDoubleFieldData extends LeafDoubleFieldData {
        final LeafReader reader;
        final String field;

        SortedNumericDoubleFieldData(LeafReader reader, String field) {
            super(0L);
            this.reader = reader;
            this.field = field;
        }

        @Override
        public SortedNumericDoubleValues getDoubleValues() {
            try {
                SortedNumericDocValues raw = DocValues.getSortedNumeric(reader, field);
                return FieldData.sortableLongBitsToDoubles(raw);
            } catch (IOException e) {
                throw new IllegalStateException("Cannot load doc values", e);
            }
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }
    }

    /**
     * Convert the values in <code>dvs</code> from nanosecond to millisecond resolution.
     */
    static SortedNumericDocValues convertNanosToMillis(SortedNumericDocValues dvs) {
        return convertNumeric(dvs, DateUtils::toMilliSeconds);
    }

    /**
     * Convert the values in <code>dvs</code> from millisecond to nanosecond resolution.
     */
    static SortedNumericDocValues convertMillisToNanos(SortedNumericDocValues values) {
        return convertNumeric(values, DateUtils::toNanoSeconds);
    }

    /**
     * Convert the values in <code>dvs</code> using the provided <code>converter</code>.
     */
    private static SortedNumericDocValues convertNumeric(SortedNumericDocValues values, LongUnaryOperator converter) {
        return new AbstractSortedNumericDocValues() {

            @Override
            public boolean advanceExact(int target) throws IOException {
                return values.advanceExact(target);
            }

            @Override
            public long nextValue() throws IOException {
                return converter.applyAsLong(values.nextValue());
            }

            @Override
            public int docValueCount() {
                return values.docValueCount();
            }

            @Override
            public int nextDoc() throws IOException {
                return values.nextDoc();
            }
        };
    }

}
