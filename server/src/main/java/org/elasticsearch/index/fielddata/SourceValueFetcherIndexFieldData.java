/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

public abstract class SourceValueFetcherIndexFieldData<T>
    implements
        IndexFieldData<SourceValueFetcherIndexFieldData.SourceValueFetcherLeafFieldData<T>> {

    public abstract static class Builder<T> implements IndexFieldData.Builder {

        protected final String fieldName;
        protected final ValuesSourceType valuesSourceType;
        protected final ValueFetcher valueFetcher;
        protected final SourceProvider sourceProvider;
        protected final ToScriptFieldFactory<T> toScriptFieldFactory;

        public Builder(
            String fieldName,
            ValuesSourceType valuesSourceType,
            ValueFetcher valueFetcher,
            SourceProvider sourceProvider,
            ToScriptFieldFactory<T> toScriptFieldFactory
        ) {
            this.fieldName = fieldName;
            this.valuesSourceType = valuesSourceType;
            this.valueFetcher = valueFetcher;
            this.sourceProvider = sourceProvider;
            this.toScriptFieldFactory = toScriptFieldFactory;
        }
    }

    protected final String fieldName;
    protected final ValuesSourceType valuesSourceType;
    protected final ValueFetcher valueFetcher;
    protected final SourceProvider sourceProvider;
    protected final ToScriptFieldFactory<T> toScriptFieldFactory;

    protected SourceValueFetcherIndexFieldData(
        String fieldName,
        ValuesSourceType valuesSourceType,
        ValueFetcher valueFetcher,
        SourceProvider sourceProvider,
        ToScriptFieldFactory<T> toScriptFieldFactory
    ) {
        this.fieldName = fieldName;
        this.valuesSourceType = valuesSourceType;
        this.valueFetcher = valueFetcher;
        this.sourceProvider = sourceProvider;
        this.toScriptFieldFactory = toScriptFieldFactory;
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
    public SourceValueFetcherLeafFieldData<T> load(LeafReaderContext context) {
        try {
            return loadDirect(context);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    @Override
    public SortField sortField(Object missingValue, MultiValueMode sortMode, XFieldComparatorSource.Nested nested, boolean reverse) {
        throw new IllegalArgumentException("not supported for source fallback");
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
        throw new IllegalArgumentException("not supported for source fallback");
    }

    public abstract static class SourceValueFetcherLeafFieldData<T> implements LeafFieldData {

        protected final ToScriptFieldFactory<T> toScriptFieldFactory;
        protected final LeafReaderContext leafReaderContext;

        protected final ValueFetcher valueFetcher;
        protected final SourceProvider sourceProvider;

        public SourceValueFetcherLeafFieldData(
            ToScriptFieldFactory<T> toScriptFieldFactory,
            LeafReaderContext leafReaderContext,
            ValueFetcher valueFetcher,
            SourceProvider sourceProvider
        ) {
            this.toScriptFieldFactory = toScriptFieldFactory;
            this.leafReaderContext = leafReaderContext;
            this.valueFetcher = valueFetcher;
            this.sourceProvider = sourceProvider;
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }

        @Override
        public void close() {

        }

        @Override
        public SortedBinaryDocValues getBytesValues() {
            throw new IllegalArgumentException("not supported for source fallback");
        }
    }

    /**
     * Marker interface to indicate these doc values are generated
     * on-the-fly from a {@code ValueFetcher}.
     */
    public interface ValueFetcherDocValues {
        // marker interface
    }
}
