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
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * FieldData for floating point types
 * backed by {@link LeafReader#getSortedNumericDocValues(String)}
 * @see DocValuesType#SORTED_NUMERIC
 */
public class SortedDoublesIndexFieldData extends IndexNumericFieldData {
    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final NumericType numericType;
        protected final ToScriptFieldFactory<SortedNumericDoubleValues> toScriptFieldFactory;

        public Builder(String name, NumericType numericType, ToScriptFieldFactory<SortedNumericDoubleValues> toScriptFieldFactory) {
            this.name = name;
            this.numericType = numericType;
            this.toScriptFieldFactory = toScriptFieldFactory;
        }

        @Override
        public SortedDoublesIndexFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new SortedDoublesIndexFieldData(name, numericType, toScriptFieldFactory);
        }
    }

    private final NumericType numericType;
    protected final String fieldName;
    protected final ValuesSourceType valuesSourceType;
    protected final ToScriptFieldFactory<SortedNumericDoubleValues> toScriptFieldFactory;

    public SortedDoublesIndexFieldData(
        String fieldName,
        NumericType numericType,
        ToScriptFieldFactory<SortedNumericDoubleValues> toScriptFieldFactory
    ) {
        this.fieldName = fieldName;
        this.numericType = Objects.requireNonNull(numericType);
        assert this.numericType.isFloatingPoint();
        this.valuesSourceType = numericType.getValuesSourceType();
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
        return numericType == NumericType.HALF_FLOAT;
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

        return switch (numericType) {
            case HALF_FLOAT -> new SortedNumericHalfFloatFieldData(reader, field, toScriptFieldFactory);
            case FLOAT -> new SortedNumericFloatFieldData(reader, field, toScriptFieldFactory);
            default -> new SortedNumericDoubleFieldData(reader, field, toScriptFieldFactory);
        };
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
        protected final ToScriptFieldFactory<SortedNumericDoubleValues> toScriptFieldFactory;

        SortedNumericHalfFloatFieldData(
            LeafReader reader,
            String field,
            ToScriptFieldFactory<SortedNumericDoubleValues> toScriptFieldFactory
        ) {
            super(0L);
            this.reader = reader;
            this.field = field;
            this.toScriptFieldFactory = toScriptFieldFactory;
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

        @Override
        public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
            return toScriptFieldFactory.getScriptFieldFactory(getDoubleValues(), name);
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
        protected final ToScriptFieldFactory<SortedNumericDoubleValues> toScriptFieldFactory;

        SortedNumericFloatFieldData(LeafReader reader, String field, ToScriptFieldFactory<SortedNumericDoubleValues> toScriptFieldFactory) {
            super(0L);
            this.reader = reader;
            this.field = field;
            this.toScriptFieldFactory = toScriptFieldFactory;
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

        @Override
        public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
            return toScriptFieldFactory.getScriptFieldFactory(getDoubleValues(), name);
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
        protected final ToScriptFieldFactory<SortedNumericDoubleValues> toScriptFieldFactory;

        SortedNumericDoubleFieldData(
            LeafReader reader,
            String field,
            ToScriptFieldFactory<SortedNumericDoubleValues> toScriptFieldFactory
        ) {
            super(0L);
            this.reader = reader;
            this.field = field;
            this.toScriptFieldFactory = toScriptFieldFactory;
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

        @Override
        public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
            return toScriptFieldFactory.getScriptFieldFactory(getDoubleValues(), name);
        }
    }
}
