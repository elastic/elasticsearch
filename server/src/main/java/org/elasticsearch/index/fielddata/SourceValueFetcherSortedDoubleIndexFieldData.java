/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class SourceValueFetcherSortedDoubleIndexFieldData extends SourceValueFetcherIndexFieldData<SortedNumericDoubleValues> {

    public static class Builder extends SourceValueFetcherIndexFieldData.Builder<SortedNumericDoubleValues> {

        public Builder(
            String fieldName,
            ValuesSourceType valuesSourceType,
            ValueFetcher valueFetcher,
            SourceLookup sourceLookup,
            ToScriptFieldFactory<SortedNumericDoubleValues> toScriptFieldFactory
        ) {
            super(fieldName, valuesSourceType, valueFetcher, sourceLookup, toScriptFieldFactory);
        }

        @Override
        public SourceValueFetcherSortedDoubleIndexFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new SourceValueFetcherSortedDoubleIndexFieldData(
                fieldName,
                valuesSourceType,
                valueFetcher,
                sourceLookup,
                toScriptFieldFactory
            );
        }
    }

    protected SourceValueFetcherSortedDoubleIndexFieldData(
        String fieldName,
        ValuesSourceType valuesSourceType,
        ValueFetcher valueFetcher,
        SourceLookup sourceLookup,
        ToScriptFieldFactory<SortedNumericDoubleValues> toScriptFieldFactory
    ) {
        super(fieldName, valuesSourceType, valueFetcher, sourceLookup, toScriptFieldFactory);
    }

    @Override
    public SourceValueFetcherLeafFieldData<SortedNumericDoubleValues> loadDirect(LeafReaderContext context) throws Exception {
        return new SourceValueFetcherSortedDoubleLeafFieldData(toScriptFieldFactory, context, valueFetcher, sourceLookup);
    }

    private static class SourceValueFetcherSortedDoubleLeafFieldData extends SourceValueFetcherLeafFieldData<SortedNumericDoubleValues> {

        private SourceValueFetcherSortedDoubleLeafFieldData(
            ToScriptFieldFactory<SortedNumericDoubleValues> toScriptFieldFactory,
            LeafReaderContext leafReaderContext,
            ValueFetcher valueFetcher,
            SourceLookup sourceLookup
        ) {
            super(toScriptFieldFactory, leafReaderContext, valueFetcher, sourceLookup);
        }

        @Override
        public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
            return toScriptFieldFactory.getScriptFieldFactory(
                new SourceValueFetcherSortedNumericDoubleValues(leafReaderContext, valueFetcher, sourceLookup),
                name
            );
        }
    }

    private static class SourceValueFetcherSortedNumericDoubleValues extends SortedNumericDoubleValues implements ValueFetcherDocValues {

        private final LeafReaderContext leafReaderContext;

        private final ValueFetcher valueFetcher;
        private final SourceLookup sourceLookup;

        private final List<Double> values;
        private Iterator<Double> iterator;

        private SourceValueFetcherSortedNumericDoubleValues(
            LeafReaderContext leafReaderContext,
            ValueFetcher valueFetcher,
            SourceLookup sourceLookup
        ) {
            this.leafReaderContext = leafReaderContext;
            this.valueFetcher = valueFetcher;
            this.sourceLookup = sourceLookup;

            values = new ArrayList<>();
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
            sourceLookup.setSegmentAndDocument(leafReaderContext, doc);
            values.clear();

            for (Object value : valueFetcher.fetchValues(sourceLookup, Collections.emptyList())) {
                assert value instanceof Number;
                values.add(((Number) value).doubleValue());
            }

            values.sort(Double::compare);
            iterator = values.iterator();

            return true;
        }

        @Override
        public int docValueCount() {
            return values.size();
        }

        @Override
        public double nextValue() throws IOException {
            assert iterator.hasNext();
            return iterator.next();
        }
    }
}
