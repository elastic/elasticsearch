/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Set;

/**
 * Per segment values for a field loaded from stored fields.
 */
public abstract class StoredFieldIndexFieldData<T> implements IndexFieldData<StoredFieldIndexFieldData<T>.StoredFieldLeafFieldData> {
    private final String fieldName;
    private final ValuesSourceType valuesSourceType;
    protected final ToScriptFieldFactory<T> toScriptFieldFactory;
    protected final StoredFieldLoader loader;

    protected StoredFieldIndexFieldData(String fieldName, ValuesSourceType valuesSourceType, ToScriptFieldFactory<T> toScriptFieldFactory) {
        this.fieldName = fieldName;
        this.valuesSourceType = valuesSourceType;
        this.toScriptFieldFactory = toScriptFieldFactory;
        this.loader = StoredFieldLoader.create(false, Set.of(fieldName));
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
    public final StoredFieldLeafFieldData load(LeafReaderContext context) {
        try {
            return loadDirect(context);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    @Override
    public final StoredFieldLeafFieldData loadDirect(LeafReaderContext context) throws IOException {
        return new StoredFieldLeafFieldData(loader.getLoader(context, null));
    }

    protected abstract T loadLeaf(LeafStoredFieldLoader leafStoredFieldLoader);

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

    public class StoredFieldLeafFieldData implements LeafFieldData {
        private final LeafStoredFieldLoader loader;

        protected StoredFieldLeafFieldData(LeafStoredFieldLoader loader) {
            this.loader = loader;
        }

        @Override
        public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
            return toScriptFieldFactory.getScriptFieldFactory(loadLeaf(loader), fieldName);
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }

        @Override
        public SortedBinaryDocValues getBytesValues() {
            throw new IllegalArgumentException("not supported for source fallback");
        }
    }
}
