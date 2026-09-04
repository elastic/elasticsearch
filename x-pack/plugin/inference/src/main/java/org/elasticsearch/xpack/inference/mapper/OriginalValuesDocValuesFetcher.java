/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.fielddata.MultiValuedSortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Fetches a semantic field's original input value(s) from its internal binary doc values store
 * ({@link SemanticTextField#getOriginalValuesFieldName}) instead of from {@code _source}.
 * <p>
 * Used in synthetic-source and columnar indices: reading the doc values column directly lets retrieval (the {@code fields} option,
 * highlighting) skip rebuilding {@code _source}, so this fetcher declares {@link StoredFieldsSpec#NO_REQUIREMENTS}. The {@code
 * decoder} turns each stored value into its {@code _source} form ({@code semantic_text} stores raw UTF-8).
 */
class OriginalValuesDocValuesFetcher implements ValueFetcher {
    private final String fieldName;
    private final CheckedFunction<BytesRef, Object, IOException> decoder;
    private SortedBinaryDocValues values;

    OriginalValuesDocValuesFetcher(String fieldName, CheckedFunction<BytesRef, Object, IOException> decoder) {
        this.fieldName = fieldName;
        this.decoder = decoder;
    }

    @Override
    public void setNextReader(LeafReaderContext context) {
        try {
            // fromMultiValued handles both the separate-counts and (older) integrated-counts encodings; the values are returned in
            // the document order they were written (the store uses ValueOrdering.UNSORTED), so array order round-trips exactly.
            values = MultiValuedSortedBinaryDocValues.fromMultiValued(context.reader(), fieldName);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public List<Object> fetchValues(Source source, int doc, List<Object> ignoredValues) throws IOException {
        if (values == null || values.advanceExact(doc) == false) {
            return List.of();
        }
        int count = values.docValueCount();
        List<Object> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            result.add(decoder.apply(values.nextValue()));
        }
        return result;
    }

    @Override
    public StoredFieldsSpec storedFieldsSpec() {
        return StoredFieldsSpec.NO_REQUIREMENTS;
    }
}
