/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Per segment values for a field loaded from stored fields exposing {@link SortedBinaryDocValues}.
 */
public abstract class StoredFieldSortedBinaryIndexFieldData extends StoredFieldIndexFieldData<SortedBinaryDocValues> {

    protected StoredFieldSortedBinaryIndexFieldData(
        String fieldName,
        ValuesSourceType valuesSourceType,
        ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory
    ) {
        super(fieldName, valuesSourceType, toScriptFieldFactory);
    }

    @Override
    protected SourceValueFetcherSortedBinaryDocValues loadLeaf(LeafStoredFieldLoader leafStoredFieldLoader) {
        return new SourceValueFetcherSortedBinaryDocValues(leafStoredFieldLoader);
    }

    protected abstract BytesRef storedToBytesRef(Object stored);

    class SourceValueFetcherSortedBinaryDocValues extends SortedBinaryDocValues {
        private final LeafStoredFieldLoader loader;
        private final List<BytesRef> sorted = new ArrayList<>();

        private int current;
        private int docValueCount;

        SourceValueFetcherSortedBinaryDocValues(LeafStoredFieldLoader loader) {
            this.loader = loader;
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
            loader.advanceTo(doc);
            List<Object> values = loader.storedFields().get(getFieldName());
            if (values == null || values.isEmpty()) {
                current = 0;
                docValueCount = 0;
                return false;
            }
            sorted.clear();
            for (Object o : values) {
                sorted.add(storedToBytesRef(o));
            }
            Collections.sort(sorted);
            current = 0;
            docValueCount = sorted.size();
            return true;
        }

        @Override
        public int docValueCount() {
            return docValueCount;
        }

        @Override
        public BytesRef nextValue() throws IOException {
            assert current < docValueCount;
            return sorted.get(current++);
        }
    }
}
