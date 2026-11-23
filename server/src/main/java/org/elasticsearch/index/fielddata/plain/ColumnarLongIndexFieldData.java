/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LongValues;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.columnar.numeric.NumericBinaryPayload;
import org.elasticsearch.columnar.numeric.NumericBinaryValues;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericLongValues;
import org.elasticsearch.index.fielddata.fieldcomparator.LongValuesComparatorSource;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;

/**
 * Field data for a {@code long} field whose doc values the ColumNAR codec stores: binary doc values holding a
 * {@link NumericBinaryPayload} per document, with the document's values in the order they were written.
 *
 * <p>Field data hands values out sorted, as {@code SORTED_NUMERIC} doc values do, so every consumer of
 * {@link SortedNumericLongValues} (aggregations, sorting, scripts) reads these fields unchanged. A column in which
 * every document holds one value is served as a singleton, which is what those consumers specialize for.
 */
public class ColumnarLongIndexFieldData extends IndexNumericFieldData {

    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final ValuesSourceType valuesSourceType;
        private final ToScriptFieldFactory<SortedNumericLongValues> toScriptFieldFactory;
        private final IndexType indexType;

        public Builder(
            String name,
            ValuesSourceType valuesSourceType,
            ToScriptFieldFactory<SortedNumericLongValues> toScriptFieldFactory,
            IndexType indexType
        ) {
            this.name = name;
            this.valuesSourceType = valuesSourceType;
            this.toScriptFieldFactory = toScriptFieldFactory;
            this.indexType = indexType;
        }

        @Override
        public ColumnarLongIndexFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new ColumnarLongIndexFieldData(name, valuesSourceType, toScriptFieldFactory, indexType);
        }
    }

    private final String fieldName;
    private final ValuesSourceType valuesSourceType;
    private final ToScriptFieldFactory<SortedNumericLongValues> toScriptFieldFactory;
    private final IndexType indexType;

    public ColumnarLongIndexFieldData(
        String fieldName,
        ValuesSourceType valuesSourceType,
        ToScriptFieldFactory<SortedNumericLongValues> toScriptFieldFactory,
        IndexType indexType
    ) {
        this.fieldName = fieldName;
        this.valuesSourceType = valuesSourceType;
        this.toScriptFieldFactory = toScriptFieldFactory;
        this.indexType = indexType;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return valuesSourceType;
    }

    @Override
    public NumericType getNumericType() {
        return NumericType.LONG;
    }

    /** Lucene's native numeric sort reads {@code SORTED_NUMERIC} doc values, which these fields don't have. */
    @Override
    protected boolean sortRequiresCustomComparator() {
        return true;
    }

    @Override
    public IndexType indexType() {
        return indexType;
    }

    /** Lucene's native numeric sort can't sort an index by these fields, so they sort it with a {@link ColumnarNumericSortField}. */
    @Override
    public SortField indexSort(IndexVersion indexCreatedVersion, Object missingValue, MultiValueMode sortMode, boolean reverse) {
        assert sortMode == MultiValueMode.MIN || sortMode == MultiValueMode.MAX : "an index sorts by a field's min or max";
        Object missing = new LongValuesComparatorSource(this, missingValue, sortMode, null, NumericType.LONG).missingObject(
            missingValue,
            reverse
        );
        return new ColumnarNumericSortField(getFieldName(), SortField.Type.LONG, reverse, sortMode == MultiValueMode.MAX, missing);
    }

    @Override
    public LeafNumericFieldData load(LeafReaderContext context) {
        return new LeafLongFieldData(0L) {
            @Override
            public SortedNumericLongValues getLongValues() {
                try {
                    return longValues(context.reader(), fieldName);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
                return toScriptFieldFactory.getScriptFieldFactory(getLongValues(), name);
            }
        };
    }

    @Override
    public LeafNumericFieldData loadDirect(LeafReaderContext context) {
        return load(context);
    }

    /**
     * The field's values as {@link SortedNumericLongValues}: sorted within each document, and a singleton when every
     * document holds one value.
     */
    public static SortedNumericLongValues longValues(LeafReader reader, String field) throws IOException {
        final BinaryDocValues binary = reader.getBinaryDocValues(field);
        if (binary == null) {
            return SortedNumericLongValues.EMPTY;
        }
        // The payloads may not have been through the codec, such as a document an index-time script reads as it is indexed.
        final NumericBinaryValues values = NumericBinaryValues.of(binary);
        return values.isSingleValued() ? singleValued(values) : multiValued(values);
    }

    private static SortedNumericLongValues singleValued(NumericBinaryValues in) {
        return SortedNumericLongValues.singleton(new LongValues() {
            @Override
            public boolean advanceExact(int doc) throws IOException {
                return in.advanceExact(doc);
            }

            @Override
            public long longValue() throws IOException {
                return in.valueAt(0);
            }
        }, in);
    }

    private static SortedNumericLongValues multiValued(NumericBinaryValues in) {
        return new SortedValues(in) {
            @Override
            public boolean advanceExact(int target) throws IOException {
                if (in.advanceExact(target) == false) {
                    return false;
                }
                final int count = in.docValueCount();
                final long[] values = grow(count);
                for (int i = 0; i < count; i++) {
                    values[i] = in.valueAt(i);
                }
                sort(count);
                return true;
            }
        };
    }

    /** One document's values, sorted, read out of a buffer that {@link #advanceExact} fills. */
    private abstract static class SortedValues extends SortedNumericLongValues {
        private long[] values = new long[NumericBinaryValues.INITIAL_DECODE_CAPACITY];
        private int count;
        private int upto;

        SortedValues(DocIdSetIterator iterator) {
            super(iterator);
        }

        long[] grow(int size) {
            if (values.length < size) {
                values = new long[ArrayUtil.oversize(size, Long.BYTES)];
            }
            return values;
        }

        void sort(int size) {
            Arrays.sort(values, 0, size);
            count = size;
            upto = 0;
        }

        @Override
        public long nextValue() {
            return values[upto++];
        }

        @Override
        public int docValueCount() {
            return count;
        }
    }
}
