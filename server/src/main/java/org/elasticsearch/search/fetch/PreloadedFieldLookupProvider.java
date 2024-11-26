/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.search.lookup.FieldLookup;
import org.elasticsearch.search.lookup.LeafFieldLookupProvider;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Makes pre-loaded stored fields available via a LeafSearchLookup.
 *
 * If a stored field is requested that is not in the pre-loaded list,
 * this loader will fall back to loading directly from the context
 * stored fields
 */
class PreloadedFieldLookupProvider implements LeafFieldLookupProvider {

    private final SetOnce<Set<String>> preloadedStoredFieldNames = new SetOnce<>();
    private Map<String, List<Object>> preloadedStoredFieldValues;
    private String id;
    private LeafFieldLookupProvider backUpLoader;
    private Supplier<LeafFieldLookupProvider> loaderSupplier;

    @Override
    public void populateFieldLookup(FieldLookup fieldLookup, int doc) throws IOException {
        String field = fieldLookup.fieldType().name();

        if (field.equals(IdFieldMapper.NAME)) {
            fieldLookup.setValues(Collections.singletonList(id));
            return;
        }
        if (preloadedStoredFieldNames.get().contains(field)) {
            fieldLookup.setValues(preloadedStoredFieldValues.get(field));
            return;
        }
        // stored field not preloaded, go and get it directly
        if (backUpLoader == null) {
            backUpLoader = loaderSupplier.get();
        }
        backUpLoader.populateFieldLookup(fieldLookup, doc);
    }

    void setPreloadedStoredFieldNames(Set<String> preloadedStoredFieldNames) {
        this.preloadedStoredFieldNames.set(preloadedStoredFieldNames);
    }

    void setPreloadedStoredFieldValues(String id, Map<String, List<Object>> preloadedStoredFieldValues) {
        assert preloadedStoredFieldNames.get().containsAll(preloadedStoredFieldValues.keySet())
            : "Provided stored field that was not expected to be preloaded? "
                + preloadedStoredFieldValues.keySet()
                + " - "
                + preloadedStoredFieldNames;
        this.preloadedStoredFieldValues = preloadedStoredFieldValues;
        this.id = id;
    }

    void setNextReader(LeafReaderContext ctx) {
        backUpLoader = null;
        loaderSupplier = () -> LeafFieldLookupProvider.fromStoredFields().apply(ctx);
    }

    LeafFieldLookupProvider getBackUpLoader() {
        return backUpLoader;
    }
}
