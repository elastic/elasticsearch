/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
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

public class SourceValueFetcherSortedNumericIndexFieldData extends SourceValueFetcherIndexFieldData<SortedNumericDocValues> {

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
        public SourceValueFetcherSortedNumericIndexFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new SourceValueFetcherSortedNumericIndexFieldData(
                fieldName,
                valuesSourceType,
                valueFetcher,
                sourceProvider,
                toScriptFieldFactory
            );
        }
    }

    protected SourceValueFetcherSortedNumericIndexFieldData(
        String fieldName,
        ValuesSourceType valuesSourceType,
        ValueFetcher valueFetcher,
        SourceProvider sourceProvider,
        ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory
    ) {
        super(fieldName, valuesSourceType, valueFetcher, sourceProvider, toScriptFieldFactory);
    }

    @Override
    public SourceValueFetcherSortedNumericLeafFieldData loadDirect(LeafReaderContext context) throws Exception {
        return new SourceValueFetcherSortedNumericLeafFieldData(toScriptFieldFactory, context, valueFetcher, sourceProvider);
    }

    public static class SourceValueFetcherSortedNumericLeafFieldData extends SourceValueFetcherLeafFieldData<SortedNumericDocValues> {

        public SourceValueFetcherSortedNumericLeafFieldData(
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
                new SourceValueFetcherSortedNumericDocValues(leafReaderContext, valueFetcher, sourceProvider),
                name
            );
        }
    }

    public static class SourceValueFetcherSortedNumericDocValues extends SortedNumericDocValues implements ValueFetcherDocValues {

        protected final LeafReaderContext leafReaderContext;

        protected final ValueFetcher valueFetcher;
        protected final SourceProvider sourceProvider;

        protected final List<Long> values;
        protected Iterator<Long> iterator;

        public SourceValueFetcherSortedNumericDocValues(
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
                values.add(((Number) value).longValue());
            }

            values.sort(Long::compare);
            iterator = values.iterator();

            return values.isEmpty() == false;
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
