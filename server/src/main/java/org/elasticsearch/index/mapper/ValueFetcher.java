/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.fetch.subphase.FetchFieldsPhase;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A helper class for fetching field values during the {@link FetchFieldsPhase}. Each {@link MappedFieldType}
 * is in charge of defining a value fetcher through {@link MappedFieldType#valueFetcher}.
 */
public interface ValueFetcher {
    /**
     * This method is consumed by {@link #fetchDocumentField(String, SourceLookup)}.
     *
     * Given access to a document's _source, return this field's values.
     * <p>
     * In addition to pulling out the values, they will be parsed into a standard form.
     * For example numeric field mappers make sure to parse the source value into a number
     * of the right type.
     * <p>
     * Note that for array values, the order in which values are returned is undefined and
     * should not be relied on.
     *
     * @param lookup        a lookup structure over the document's source.
     * @param ignoredValues a mutable list to collect any ignored values as they were originally presented in source
     * @return a list a standardized field values.
     */
    List<Object> fetchValues(SourceLookup lookup, List<Object> ignoredValues) throws IOException;

    /**
     * Prefer implementing {@link #fetchValues(SourceLookup, List)}, which is simpler, when possible instead of this method.
     * The default implementation creates a {@link DocumentField} using the values from {@link #fetchValues(SourceLookup, List)}
     *
     * @param docName the name of the document field
     * @param lookup  a lookup structure over the document's source.
     * @return a document field if this fetcher has values; otherwise returns null
     */
    @Nullable
    default DocumentField fetchDocumentField(String docName, SourceLookup lookup) throws IOException {
        final List<Object> ignoredValues = new ArrayList<>();
        final List<Object> values = fetchValues(lookup, ignoredValues);
        if (values.isEmpty() && ignoredValues.isEmpty()) {
            return null;
        } else {
            return new DocumentField(docName, values, ignoredValues);
        }
    }

    /**
     * Update the leaf reader used to fetch values.
     */
    default void setNextReader(LeafReaderContext context) {}
}
