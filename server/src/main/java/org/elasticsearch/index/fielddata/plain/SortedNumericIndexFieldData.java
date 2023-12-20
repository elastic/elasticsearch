/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.LongValuesComparatorSource;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Objects;

/**
 * FieldData for integral types
 * backed by {@link LeafReader#getSortedNumericDocValues(String)}
 * @see DocValuesType#SORTED_NUMERIC
 */
public class SortedNumericIndexFieldData extends IndexNumericFieldData {
    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final NumericType numericType;
        private final ValuesSourceType valuesSourceType;
        protected final ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory;

        public Builder(String name, NumericType numericType, ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory) {
            this(name, numericType, numericType.getValuesSourceType(), toScriptFieldFactory);
        }

        public Builder(
            String name,
            NumericType numericType,
            ValuesSourceType valuesSourceType,
            ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory
        ) {
            this.name = name;
            this.numericType = numericType;
            this.valuesSourceType = valuesSourceType;
            this.toScriptFieldFactory = toScriptFieldFactory;
        }

        @Override
        public SortedNumericIndexFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new SortedNumericIndexFieldData(name, numericType, valuesSourceType, toScriptFieldFactory);
        }
    }

    private final NumericType numericType;
    protected final String fieldName;
    protected final ValuesSourceType valuesSourceType;
    protected final ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory;

    public SortedNumericIndexFieldData(
        String fieldName,
        NumericType numericType,
        ValuesSourceType valuesSourceType,
        ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory
    ) {
        this.fieldName = fieldName;
        this.numericType = Objects.requireNonNull(numericType);
        assert this.numericType.isFloatingPoint() == false;
        this.valuesSourceType = valuesSourceType;
        this.toScriptFieldFactory = toScriptFieldFactory;
    }

    @Override
    public final String getFieldName() {
        return fieldName;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return valuesSourceType;
    }

    @Override
    protected boolean sortRequiresCustomComparator() {
        return false;
    }

    @Override
    protected XFieldComparatorSource dateComparatorSource(Object missingValue, MultiValueMode sortMode, Nested nested) {
        if (numericType == NumericType.DATE_NANOSECONDS) {
            // converts date_nanos values to millisecond resolution
            return new LongValuesComparatorSource(
                this,
                missingValue,
                sortMode,
                nested,
                dvs -> convertNumeric(dvs, DateUtils::toMilliSeconds),
                NumericType.DATE
            );
        }
        return new LongValuesComparatorSource(this, missingValue, sortMode, nested, NumericType.DATE);
    }

    @Override
    protected XFieldComparatorSource dateNanosComparatorSource(Object missingValue, MultiValueMode sortMode, Nested nested) {
        if (numericType == NumericType.DATE) {
            // converts date values to nanosecond resolution
            return new LongValuesComparatorSource(
                this,
                missingValue,
                sortMode,
                nested,
                dvs -> convertNumeric(dvs, DateUtils::toNanoSeconds),
                NumericType.DATE_NANOSECONDS
            );
        }
        return new LongValuesComparatorSource(this, missingValue, sortMode, nested, NumericType.DATE_NANOSECONDS);
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

        if (numericType == NumericType.DATE_NANOSECONDS) {
            return new NanoSecondFieldData(reader, field, toScriptFieldFactory);
        }

        return new SortedNumericLongFieldData(reader, field, toScriptFieldFactory);
    }

    /**
     * A small helper class that can be configured to load nanosecond field data either in nanosecond resolution retaining the original
     * values or in millisecond resolution converting the nanosecond values to milliseconds
     */
    public static final class NanoSecondFieldData extends LeafLongFieldData {

        private final LeafReader reader;
        private final String fieldName;
        protected final ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory;

        NanoSecondFieldData(LeafReader reader, String fieldName, ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory) {
            super(0L);
            this.reader = reader;
            this.fieldName = fieldName;
            this.toScriptFieldFactory = toScriptFieldFactory;
        }

        @Override
        public SortedNumericDocValues getLongValues() {
            return convertNumeric(getLongValuesAsNanos(), DateUtils::toMilliSeconds);
        }

        public SortedNumericDocValues getLongValuesAsNanos() {
            try {
                return DocValues.getSortedNumeric(reader, fieldName);
            } catch (IOException e) {
                throw new IllegalStateException("Cannot load doc values", e);
            }
        }

        @Override
        public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
            return toScriptFieldFactory.getScriptFieldFactory(getLongValuesAsNanos(), name);
        }

        @Override
        public FormattedDocValues getFormattedValues(DocValueFormat format) {
            DocValueFormat nanosFormat = DocValueFormat.withNanosecondResolution(format);
            SortedNumericDocValues values = getLongValuesAsNanos();
            return new FormattedDocValues() {
                @Override
                public boolean advanceExact(int docId) throws IOException {
                    return values.advanceExact(docId);
                }

                @Override
                public int docValueCount() throws IOException {
                    return values.docValueCount();
                }

                @Override
                public Object nextValue() throws IOException {
                    return nanosFormat.format(values.nextValue());
                }
            };
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
        protected final ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory;

        SortedNumericLongFieldData(LeafReader reader, String field, ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory) {
            super(0L);
            this.reader = reader;
            this.field = field;
            this.toScriptFieldFactory = toScriptFieldFactory;
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
        public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
            return toScriptFieldFactory.getScriptFieldFactory(getLongValues(), name);
        }
    }
}
