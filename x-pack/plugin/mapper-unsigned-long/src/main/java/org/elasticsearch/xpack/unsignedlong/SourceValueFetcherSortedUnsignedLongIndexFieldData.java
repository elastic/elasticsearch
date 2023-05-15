/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.unsignedlong;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.SourceValueFetcherIndexFieldData;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.search.DocValueFormat.MASK_2_63;

/**
 * {@code SourceValueFetcherSortedUnsignedLongIndexFieldData} uses a {@link ValueFetcher} to
 * retrieve values from source that are parsed as an unsigned long. These values are used to
 * emulate unsigned long values pulled directly from a doc values data structure through a
 * {@link SortedNumericDocValues}.
 */
public class SourceValueFetcherSortedUnsignedLongIndexFieldData extends SourceValueFetcherIndexFieldData<SortedNumericDocValues> {

    public static class Builder extends SourceValueFetcherIndexFieldData.Builder<SortedNumericDocValues> {

        public Builder(
            String fieldName,
            ValuesSourceType valuesSourceType,
            ValueFetcher valueFetcher,
            SourceProvider sourceProvider,
            ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory
        ) {
            super(fieldName, valuesSourceType, valueFetcher, sourceProvider, toScriptFieldFactory);
        }

        @Override
        public SourceValueFetcherSortedUnsignedLongIndexFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new SourceValueFetcherSortedUnsignedLongIndexFieldData(
                fieldName,
                valuesSourceType,
                valueFetcher,
                sourceProvider,
                toScriptFieldFactory
            );
        }
    }

    protected SourceValueFetcherSortedUnsignedLongIndexFieldData(
        String fieldName,
        ValuesSourceType valuesSourceType,
        ValueFetcher valueFetcher,
        SourceProvider sourceProvider,
        ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory
    ) {
        super(fieldName, valuesSourceType, valueFetcher, sourceProvider, toScriptFieldFactory);
    }

    @Override
    public SourceValueFetcherLeafFieldData<SortedNumericDocValues> loadDirect(LeafReaderContext context) {
        return new SourceValueFetcherSortedUnsignedLongLeafFieldData(toScriptFieldFactory, context, valueFetcher, sourceProvider);
    }

    private static class SourceValueFetcherSortedUnsignedLongLeafFieldData extends SourceValueFetcherLeafFieldData<SortedNumericDocValues> {

        private SourceValueFetcherSortedUnsignedLongLeafFieldData(
            ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory,
            LeafReaderContext leafReaderContext,
            ValueFetcher valueFetcher,
            SourceProvider sourceProvider
        ) {
            super(toScriptFieldFactory, leafReaderContext, valueFetcher, sourceProvider);
        }

        @Override
        public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
            return toScriptFieldFactory.getScriptFieldFactory(
                new SourceValueFetcherSortedUnsignedLongDocValues(leafReaderContext, valueFetcher, sourceProvider),
                name
            );
        }
    }

    private static class SourceValueFetcherSortedUnsignedLongDocValues extends SortedNumericDocValues implements ValueFetcherDocValues {

        private final LeafReaderContext leafReaderContext;

        private final ValueFetcher valueFetcher;
        private final SourceProvider sourceProvider;

        private List<Long> values;
        private Iterator<Long> iterator;

        private SourceValueFetcherSortedUnsignedLongDocValues(
            LeafReaderContext leafReaderContext,
            ValueFetcher valueFetcher,
            SourceProvider sourceProvider
        ) {
            this.leafReaderContext = leafReaderContext;
            this.valueFetcher = valueFetcher;
            this.sourceProvider = sourceProvider;

            values = new ArrayList<>();
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
            values.clear();
            Source source = sourceProvider.getSource(leafReaderContext, doc);
            for (Object value : valueFetcher.fetchValues(source, doc, Collections.emptyList())) {
                assert value instanceof Number;
                values.add(((Number) value).longValue() ^ MASK_2_63);
            }

            values.sort(Long::compare);
            iterator = values.iterator();

            return true;
        }

        @Override
        public int docValueCount() {
            return values.size();
        }

        @Override
        public long nextValue() throws IOException {
            assert iterator.hasNext();
            return iterator.next();
        }

        @Override
        public int docID() {
            throw new UnsupportedOperationException("not supported for source fallback");
        }

        @Override
        public int nextDoc() throws IOException {
            throw new UnsupportedOperationException("not supported for source fallback");
        }

        @Override
        public int advance(int target) throws IOException {
            throw new UnsupportedOperationException("not supported for source fallback");
        }

        @Override
        public long cost() {
            throw new UnsupportedOperationException("not supported for source fallback");
        }
    }
}
