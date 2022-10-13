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
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.plain.LeafGeoPointFieldData;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.GeoPointFieldScript;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

public final class GeoPointScriptFieldData implements IndexGeoPointFieldData {
    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final GeoPointFieldScript.LeafFactory leafFactory;
        private final ToScriptFieldFactory<MultiGeoPointValues> toScriptFieldFactory;

        public Builder(
            String name,
            GeoPointFieldScript.LeafFactory leafFactory,
            ToScriptFieldFactory<MultiGeoPointValues> toScriptFieldFactory
        ) {
            this.name = name;
            this.leafFactory = leafFactory;
            this.toScriptFieldFactory = toScriptFieldFactory;
        }

        @Override
        public GeoPointScriptFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new GeoPointScriptFieldData(name, leafFactory, toScriptFieldFactory);
        }
    }

    private final GeoPointFieldScript.LeafFactory leafFactory;
    private final String name;
    private final ToScriptFieldFactory<MultiGeoPointValues> toScriptFieldFactory;

    private GeoPointScriptFieldData(
        String fieldName,
        GeoPointFieldScript.LeafFactory leafFactory,
        ToScriptFieldFactory<MultiGeoPointValues> toScriptFieldFactory
    ) {
        this.name = fieldName;
        this.leafFactory = leafFactory;
        this.toScriptFieldFactory = toScriptFieldFactory;
    }

    @Override
    public SortField sortField(Object missingValue, MultiValueMode sortMode, XFieldComparatorSource.Nested nested, boolean reverse) {
        throw new IllegalArgumentException("can't sort on geo_point field without using specific sorting feature, like geo_distance");
    }

    @Override
    public BucketedSort newBucketedSort(
        BigArrays bigArrays,
        Object missingValue,
        MultiValueMode sortMode,
        XFieldComparatorSource.Nested nested,
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra
    ) {
        throw new IllegalArgumentException("can't sort on geo_point field without using specific sorting feature, like geo_distance");
    }

    @Override
    public String getFieldName() {
        return name;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return CoreValuesSourceType.GEOPOINT;
    }

    @Override
    public LeafPointFieldData<MultiGeoPointValues> load(LeafReaderContext context) {
        GeoPointFieldScript script = leafFactory.newInstance(context);
        return new LeafGeoPointFieldData(toScriptFieldFactory) {
            @Override
            public SortedNumericDocValues getSortedNumericDocValues() {
                return new GeoPointScriptDocValues(script);
            }

            @Override
            public long ramBytesUsed() {
                return 0;
            }

            @Override
            public void close() {

            }
        };
    }

    @Override
    public LeafPointFieldData<MultiGeoPointValues> loadDirect(LeafReaderContext context) {
        return load(context);
    }
}
