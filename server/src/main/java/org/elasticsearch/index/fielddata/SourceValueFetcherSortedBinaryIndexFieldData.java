/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceProvider;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

public class SourceValueFetcherSortedBinaryIndexFieldData extends SourceValueFetcherIndexFieldData<SortedBinaryDocValues> {

    public static class Builder extends SourceValueFetcherIndexFieldData.Builder<SortedBinaryDocValues> {

        public Builder(
            String fieldName,
            ValuesSourceType valuesSourceType,
            ValueFetcher valueFetcher,
            SourceProvider sourceProvider,
            ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory
        ) {
            super(fieldName, valuesSourceType, valueFetcher, sourceProvider, toScriptFieldFactory);
        }

        @Override
        public SourceValueFetcherSortedBinaryIndexFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new SourceValueFetcherSortedBinaryIndexFieldData(
                fieldName,
                valuesSourceType,
                valueFetcher,
                sourceProvider,
                toScriptFieldFactory
            );
        }
    }

    protected SourceValueFetcherSortedBinaryIndexFieldData(
        String fieldName,
        ValuesSourceType valuesSourceType,
        ValueFetcher valueFetcher,
        SourceProvider sourceProvider,
        ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory
    ) {
        super(fieldName, valuesSourceType, valueFetcher, sourceProvider, toScriptFieldFactory);
    }

    @Override
    public SourceValueFetcherSortedBinaryLeafFieldData loadDirect(LeafReaderContext context) throws Exception {
        return new SourceValueFetcherSortedBinaryLeafFieldData(toScriptFieldFactory, context, valueFetcher, sourceProvider);
    }

    public static class SourceValueFetcherSortedBinaryLeafFieldData extends SourceValueFetcherLeafFieldData<SortedBinaryDocValues> {

        public SourceValueFetcherSortedBinaryLeafFieldData(
            ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory,
            LeafReaderContext leafReaderContext,
            ValueFetcher valueFetcher,
            SourceProvider sourceProvider
        ) {
            super(toScriptFieldFactory, leafReaderContext, valueFetcher, sourceProvider);
        }

        @Override
        public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
            return toScriptFieldFactory.getScriptFieldFactory(
                new SourceValueFetcherSortedBinaryDocValues(leafReaderContext, valueFetcher, sourceProvider),
                name
            );
        }
    }

    public static class SourceValueFetcherSortedBinaryDocValues extends SortedBinaryDocValues implements ValueFetcherDocValues {

        private final LeafReaderContext leafReaderContext;

        private final ValueFetcher valueFetcher;
        private final SourceProvider sourceProvider;

        private final SortedSet<BytesRef> values;
        private Iterator<BytesRef> iterator;

        public SourceValueFetcherSortedBinaryDocValues(
            LeafReaderContext leafReaderContext,
            ValueFetcher valueFetcher,
            SourceProvider sourceProvider
        ) {
            this.leafReaderContext = leafReaderContext;
            this.valueFetcher = valueFetcher;
            this.sourceProvider = sourceProvider;

            values = new TreeSet<>();
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
            values.clear();
            Source source = sourceProvider.getSource(leafReaderContext, doc);
            for (Object object : valueFetcher.fetchValues(source, doc, Collections.emptyList())) {
                values.add(new BytesRef(object.toString()));
            }

            iterator = values.iterator();

            return true;
        }

        @Override
        public int docValueCount() {
            return values.size();
        }

        @Override
        public BytesRef nextValue() throws IOException {
            assert iterator.hasNext();
            return iterator.next();
        }
    }
}
