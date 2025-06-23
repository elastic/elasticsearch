/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.fieldcomparator.DoubleValuesComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.FloatValuesComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.HalfFloatValuesComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.IntValuesComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.LongValuesComparatorSource;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.function.LongUnaryOperator;

import static org.elasticsearch.index.IndexVersions.UPGRADE_TO_LUCENE_10_0_0;

/**
 * Base class for numeric field data.
 */
public abstract class IndexNumericFieldData implements IndexFieldData<LeafNumericFieldData> {

    /**
     * The type of number.
     */
    public enum NumericType {
        BOOLEAN(false, SortField.Type.LONG, CoreValuesSourceType.BOOLEAN),
        BYTE(false, SortField.Type.INT, CoreValuesSourceType.NUMERIC),
        SHORT(false, SortField.Type.INT, CoreValuesSourceType.NUMERIC),
        INT(false, SortField.Type.INT, CoreValuesSourceType.NUMERIC),
        LONG(false, SortField.Type.LONG, CoreValuesSourceType.NUMERIC),
        DATE(false, SortField.Type.LONG, CoreValuesSourceType.DATE),
        DATE_NANOSECONDS(false, SortField.Type.LONG, CoreValuesSourceType.DATE),
        HALF_FLOAT(true, SortField.Type.FLOAT, CoreValuesSourceType.NUMERIC),
        FLOAT(true, SortField.Type.FLOAT, CoreValuesSourceType.NUMERIC),
        DOUBLE(true, SortField.Type.DOUBLE, CoreValuesSourceType.NUMERIC);

        private final boolean floatingPoint;
        private final ValuesSourceType valuesSourceType;
        private final SortField.Type sortFieldType;

        NumericType(boolean floatingPoint, SortField.Type sortFieldType, ValuesSourceType valuesSourceType) {
            this.floatingPoint = floatingPoint;
            this.sortFieldType = sortFieldType;
            this.valuesSourceType = valuesSourceType;
        }

        public final boolean isFloatingPoint() {
            return floatingPoint;
        }

        public final ValuesSourceType getValuesSourceType() {
            return valuesSourceType;
        }
    }

    /**
     * The numeric type of this number.
     */
    public abstract NumericType getNumericType();

    /**
     * Returns the {@link SortField} to used for sorting.
     * Values are casted to the provided <code>targetNumericType</code> type if it doesn't
     * match the field's <code>numericType</code>.
     */
    public final SortField sortField(
        NumericType targetNumericType,
        Object missingValue,
        MultiValueMode sortMode,
        Nested nested,
        boolean reverse
    ) {
        XFieldComparatorSource source = comparatorSource(targetNumericType, missingValue, sortMode, nested);

        /*
         * Use a SortField with the custom comparator logic if required because
         * 1. The underlying data source needs it.
         * 2. We need to read the value from a nested field.
         * 3. We Aren't using max or min to resolve the duplicates.
         * 4. We have to cast the results to another type.
         */
        boolean requiresCustomComparator = nested != null
            || (sortMode != MultiValueMode.MAX && sortMode != MultiValueMode.MIN)
            || targetNumericType != getNumericType();
        if (sortRequiresCustomComparator() || requiresCustomComparator) {
            SortField sortField = new SortField(getFieldName(), source, reverse);
            sortField.setOptimizeSortWithPoints(requiresCustomComparator == false && isIndexed());
            return sortField;
        }

        SortedNumericSelector.Type selectorType = sortMode == MultiValueMode.MAX
            ? SortedNumericSelector.Type.MAX
            : SortedNumericSelector.Type.MIN;
        SortField sortField = new SortedNumericSortField(getFieldName(), getNumericType().sortFieldType, reverse, selectorType);
        sortField.setMissingValue(source.missingObject(missingValue, reverse));
        sortField.setOptimizeSortWithPoints(isIndexed());
        return sortField;
    }

    /**
     * Should sorting use a custom comparator source vs. rely on a Lucene {@link SortField}. Using a Lucene {@link SortField} when possible
     * is important because index sorting cannot be configured with a custom comparator, and because it gives better performance by
     * dynamically pruning irrelevant hits. On the other hand, Lucene {@link SortField}s are less flexible and make stronger assumptions
     * about how the data is indexed. Therefore, they cannot be used in all cases.
     */
    protected abstract boolean sortRequiresCustomComparator();

    /**
     * Return true if, and only if the field is indexed with points that match the content of doc values.
     */
    protected abstract boolean isIndexed();

    @Override
    public final SortField sortField(Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
        return sortField(getNumericType(), missingValue, sortMode, nested, reverse);
    }

    @Override
    public SortField sortField(
        IndexVersion indexCreatedVersion,
        Object missingValue,
        MultiValueMode sortMode,
        Nested nested,
        boolean reverse
    ) {
        SortField sortField = sortField(missingValue, sortMode, nested, reverse);
        // we introduced INT sort type in 8.19 and from 9.1
        if (getNumericType().sortFieldType != SortField.Type.INT
            || indexCreatedVersion.onOrAfter(IndexVersions.INDEX_INT_SORT_INT_TYPE)
            || indexCreatedVersion.between(IndexVersions.INDEX_INT_SORT_INT_TYPE_8_19, UPGRADE_TO_LUCENE_10_0_0)) {
            return sortField;
        }
        if ((sortField instanceof SortedNumericSortField) == false) {
            return sortField;
        }
        // if the index was created before 8.19, or in 9.0
        // we need to rewrite the sort field to use LONG sort type

        // Rewrite INT sort to LONG sort.
        // Before indices used TYPE.LONG for index sorting on integer field,
        // and this is stored in their index writer config on disk and can't be modified.
        // Now sortField() returns TYPE.INT when sorting on integer field,
        // but to support sorting on old indices, we need to rewrite this sort to TYPE.LONG.
        SortedNumericSortField numericSortField = (SortedNumericSortField) sortField;
        SortedNumericSortField rewrittenSortField = new SortedNumericSortField(
            sortField.getField(),
            SortField.Type.LONG,
            sortField.getReverse(),
            numericSortField.getSelector()
        );
        XFieldComparatorSource longSource = comparatorSource(NumericType.LONG, missingValue, sortMode, nested);
        rewrittenSortField.setMissingValue(longSource.missingObject(missingValue, reverse));
        // we don't optimize sorting on int field for old indices
        rewrittenSortField.setOptimizeSortWithPoints(false);
        return rewrittenSortField;
    }

    /**
     * Builds a {@linkplain BucketedSort} for the {@code targetNumericType},
     * casting the values if their native type doesn't match.
     */
    public final BucketedSort newBucketedSort(
        NumericType targetNumericType,
        BigArrays bigArrays,
        @Nullable Object missingValue,
        MultiValueMode sortMode,
        Nested nested,
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra
    ) {
        return comparatorSource(targetNumericType, missingValue, sortMode, nested).newBucketedSort(
            bigArrays,
            sortOrder,
            format,
            bucketSize,
            extra
        );
    }

    @Override
    public final BucketedSort newBucketedSort(
        BigArrays bigArrays,
        @Nullable Object missingValue,
        MultiValueMode sortMode,
        Nested nested,
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra
    ) {
        return newBucketedSort(getNumericType(), bigArrays, missingValue, sortMode, nested, sortOrder, format, bucketSize, extra);
    }

    /**
     * Build a {@link XFieldComparatorSource} matching the parameters.
     */
    private XFieldComparatorSource comparatorSource(
        NumericType targetNumericType,
        @Nullable Object missingValue,
        MultiValueMode sortMode,
        Nested nested
    ) {
        return switch (targetNumericType) {
            case FLOAT -> new FloatValuesComparatorSource(this, missingValue, sortMode, nested);
            case HALF_FLOAT -> new HalfFloatValuesComparatorSource(this, missingValue, sortMode, nested);
            case DOUBLE -> new DoubleValuesComparatorSource(this, missingValue, sortMode, nested);
            case BYTE, SHORT, INT -> new IntValuesComparatorSource(this, missingValue, sortMode, nested, targetNumericType);
            case DATE -> dateComparatorSource(missingValue, sortMode, nested);
            case DATE_NANOSECONDS -> dateNanosComparatorSource(missingValue, sortMode, nested);
            default -> {
                assert targetNumericType.isFloatingPoint() == false;
                yield new LongValuesComparatorSource(this, missingValue, sortMode, nested, targetNumericType);
            }
        };
    }

    protected XFieldComparatorSource dateComparatorSource(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested) {
        return new LongValuesComparatorSource(this, missingValue, sortMode, nested, NumericType.DATE);
    }

    protected XFieldComparatorSource dateNanosComparatorSource(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested) {
        return new LongValuesComparatorSource(
            this,
            missingValue,
            sortMode,
            nested,
            dvs -> convertNumeric(dvs, DateUtils::toNanoSeconds),
            NumericType.DATE_NANOSECONDS
        );
    }

    /**
     * Convert the values in <code>dvs</code> using the provided <code>converter</code>.
     */
    protected static SortedNumericDocValues convertNumeric(SortedNumericDocValues values, LongUnaryOperator converter) {
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
