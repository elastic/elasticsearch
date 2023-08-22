/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Set;

public class IdFieldIndexFieldData implements IndexFieldData<IdFieldIndexFieldData.IdFieldLeafFieldData> {

    private static final String FIELD_NAME = IdFieldMapper.NAME;
    private final ValuesSourceType valuesSourceType;
    private final StoredFieldLoader loader;

    protected IdFieldIndexFieldData(ValuesSourceType valuesSourceType) {
        this.valuesSourceType = valuesSourceType;
        this.loader = StoredFieldLoader.create(false, Set.of(FIELD_NAME));
    }

    @Override
    public String getFieldName() {
        return FIELD_NAME;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return valuesSourceType;
    }

    @Override
    public final IdFieldLeafFieldData load(LeafReaderContext context) {
        try {
            return loadDirect(context);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    @Override
    public final IdFieldLeafFieldData loadDirect(LeafReaderContext context) throws Exception {
        return new IdFieldLeafFieldData(loader.getLoader(context, null));
    }

    @Override
    public SortField sortField(Object missingValue, MultiValueMode sortMode, XFieldComparatorSource.Nested nested, boolean reverse) {
        throw new IllegalArgumentException("not supported for stored field fallback");
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
        throw new IllegalArgumentException("not supported for stored field fallback");
    }

    class IdFieldLeafFieldData implements LeafFieldData {
        private final LeafStoredFieldLoader loader;

        protected IdFieldLeafFieldData(LeafStoredFieldLoader loader) {
            this.loader = loader;
        }

        @Override
        public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
            throw new IllegalArgumentException("not supported for _id field");
        }

        @Override
        public long ramBytesUsed() {
            return 0L;
        }

        @Override
        public void close() {}

        @Override
        public SortedBinaryDocValues getBytesValues() {
            return new SortedBinaryDocValues() {
                private String id;

                @Override
                public boolean advanceExact(int doc) throws IOException {
                    loader.advanceTo(doc);
                    id = loader.id();
                    return id != null;
                }

                @Override
                public int docValueCount() {
                    return 1;
                }

                @Override
                public BytesRef nextValue() throws IOException {
                    return new BytesRef(id);
                }
            };
        }
    }
}
