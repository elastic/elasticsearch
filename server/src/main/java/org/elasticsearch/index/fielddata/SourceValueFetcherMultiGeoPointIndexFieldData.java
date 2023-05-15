/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceProvider;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SourceValueFetcherMultiGeoPointIndexFieldData extends SourceValueFetcherIndexFieldData<MultiGeoPointValues> {

    public static class Builder extends SourceValueFetcherIndexFieldData.Builder<MultiGeoPointValues> {

        public Builder(
            String fieldName,
            ValuesSourceType valuesSourceType,
            ValueFetcher valueFetcher,
            SourceProvider sourceProvider,
            ToScriptFieldFactory<MultiGeoPointValues> toScriptFieldFactory
        ) {
            super(fieldName, valuesSourceType, valueFetcher, sourceProvider, toScriptFieldFactory);
        }

        @Override
        public SourceValueFetcherMultiGeoPointIndexFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new SourceValueFetcherMultiGeoPointIndexFieldData(
                fieldName,
                valuesSourceType,
                valueFetcher,
                sourceProvider,
                toScriptFieldFactory
            );
        }
    }

    protected SourceValueFetcherMultiGeoPointIndexFieldData(
        String fieldName,
        ValuesSourceType valuesSourceType,
        ValueFetcher valueFetcher,
        SourceProvider sourceProvider,
        ToScriptFieldFactory<MultiGeoPointValues> toScriptFieldFactory
    ) {
        super(fieldName, valuesSourceType, valueFetcher, sourceProvider, toScriptFieldFactory);
    }

    @Override
    public SourceValueFetcherMultiGeoPointLeafFieldData loadDirect(LeafReaderContext context) throws Exception {
        return new SourceValueFetcherMultiGeoPointLeafFieldData(toScriptFieldFactory, context, valueFetcher, sourceProvider);
    }

    public static class SourceValueFetcherMultiGeoPointLeafFieldData extends
        SourceValueFetcherIndexFieldData.SourceValueFetcherLeafFieldData<MultiGeoPointValues> {

        public SourceValueFetcherMultiGeoPointLeafFieldData(
            ToScriptFieldFactory<MultiGeoPointValues> toScriptFieldFactory,
            LeafReaderContext leafReaderContext,
            ValueFetcher valueFetcher,
            SourceProvider sourceProvider
        ) {
            super(toScriptFieldFactory, leafReaderContext, valueFetcher, sourceProvider);
        }

        @Override
        public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
            return toScriptFieldFactory.getScriptFieldFactory(
                new MultiGeoPointValues(new SourceValueFetcherMultiGeoPointDocValues(leafReaderContext, valueFetcher, sourceProvider)),
                name
            );
        }
    }

    public static class SourceValueFetcherMultiGeoPointDocValues extends
        SourceValueFetcherSortedNumericIndexFieldData.SourceValueFetcherSortedNumericDocValues {

        public SourceValueFetcherMultiGeoPointDocValues(
            LeafReaderContext leafReaderContext,
            ValueFetcher valueFetcher,
            SourceProvider sourceProvider
        ) {
            super(leafReaderContext, valueFetcher, sourceProvider);
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean advanceExact(int doc) throws IOException {
            values.clear();
            Source source = sourceProvider.getSource(leafReaderContext, doc);
            for (Object value : valueFetcher.fetchValues(source, doc, Collections.emptyList())) {
                assert value instanceof Map && ((Map<Object, Object>) value).get("coordinates") instanceof List;
                List<Object> coordinates = ((Map<String, List<Object>>) value).get("coordinates");
                assert coordinates.size() == 2 && coordinates.get(1) instanceof Number && coordinates.get(0) instanceof Number;
                values.add(
                    new GeoPoint(((Number) coordinates.get(1)).doubleValue(), ((Number) coordinates.get(0)).doubleValue()).getEncoded()
                );
            }

            values.sort(Long::compare);
            iterator = values.iterator();

            return true;
        }
    }
}
