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
import java.util.Collections;

public class SourceValueFetcherSortedBooleanIndexFieldData extends SourceValueFetcherIndexFieldData<SortedNumericDocValues> {

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
        public SourceValueFetcherSortedBooleanIndexFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new SourceValueFetcherSortedBooleanIndexFieldData(
                fieldName,
                valuesSourceType,
                valueFetcher,
                sourceProvider,
                toScriptFieldFactory
            );
        }
    }

    protected SourceValueFetcherSortedBooleanIndexFieldData(
        String fieldName,
        ValuesSourceType valuesSourceType,
        ValueFetcher valueFetcher,
        SourceProvider sourceProvider,
        ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory
    ) {
        super(fieldName, valuesSourceType, valueFetcher, sourceProvider, toScriptFieldFactory);
    }

    @Override
    public SourceValueFetcherLeafFieldData<SortedNumericDocValues> loadDirect(LeafReaderContext context) throws Exception {
        return new SourceValueFetcherSortedBooleanLeafFieldData(toScriptFieldFactory, context, valueFetcher, sourceProvider);
    }

    private static class SourceValueFetcherSortedBooleanLeafFieldData extends SourceValueFetcherLeafFieldData<SortedNumericDocValues> {

        private SourceValueFetcherSortedBooleanLeafFieldData(
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
                new SourceValueFetcherSortedBooleanDocValues(leafReaderContext, valueFetcher, sourceProvider),
                name
            );
        }
    }

    static class SourceValueFetcherSortedBooleanDocValues extends SortedNumericDocValues implements ValueFetcherDocValues {

        private final LeafReaderContext leafReaderContext;

        private final ValueFetcher valueFetcher;
        private final SourceProvider sourceProvider;

        private int trueCount;
        private int falseCount;
        private int iteratorIndex;

        SourceValueFetcherSortedBooleanDocValues(
            LeafReaderContext leafReaderContext,
            ValueFetcher valueFetcher,
            SourceProvider sourceProvider
        ) {
            this.leafReaderContext = leafReaderContext;
            this.valueFetcher = valueFetcher;
            this.sourceProvider = sourceProvider;
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
            trueCount = 0;
            falseCount = 0;
            Source source = sourceProvider.getSource(leafReaderContext, doc);
            for (Object value : valueFetcher.fetchValues(source, doc, Collections.emptyList())) {
                assert value instanceof Boolean;
                if ((Boolean) value) {
                    ++trueCount;
                } else {
                    ++falseCount;
                }
            }

            iteratorIndex = 0;

            return true;
        }

        @Override
        public int docValueCount() {
            return trueCount + falseCount;
        }

        @Override
        public long nextValue() throws IOException {
            assert iteratorIndex < trueCount + falseCount;
            return iteratorIndex++ < falseCount ? 0L : 1L;
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
