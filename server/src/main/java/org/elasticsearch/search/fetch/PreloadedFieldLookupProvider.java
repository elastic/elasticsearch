/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.search.lookup.FieldLookup;
import org.elasticsearch.search.lookup.LeafFieldLookupProvider;

import java.io.IOException;
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

    private final Set<String> preloadedStoredFields;
    private Map<String, List<Object>> storedFields;
    private LeafFieldLookupProvider backUpLoader;
    private Supplier<LeafFieldLookupProvider> loaderSupplier;

    PreloadedFieldLookupProvider(Set<String> preloadedStoredFields) {
        this.preloadedStoredFields = preloadedStoredFields;
    }

    @Override
    public void populateFieldLookup(FieldLookup fieldLookup, int doc) throws IOException {
        String field = fieldLookup.fieldType().name();
        if (preloadedStoredFields.contains(field)) {
            fieldLookup.setValues(storedFields.get(field));
            //TODO remove this assert
            assert assertBackupValueSameAsPreloaded(fieldLookup, doc);
            return;
        }
        // stored field not preloaded, go and get it directly
        if (backUpLoader == null) {
            backUpLoader = loaderSupplier.get();
        }
        backUpLoader.populateFieldLookup(fieldLookup, doc);
    }

    private boolean assertBackupValueSameAsPreloaded(FieldLookup fieldLookup, int doc) throws IOException {
        FieldLookup fl = new FieldLookup(fieldLookup.fieldType());
        loaderSupplier.get().populateFieldLookup(fl, doc);
        if (fieldLookup.getValue() == null) {
            assert fl.getValue() == null : "loaded unexpected value for " + fieldLookup.fieldType().name() + ": " + fl.getValue();
        } else {
            assert fieldLookup.getValue().equals(fl.getValue())
                : "loaded different value for " + fieldLookup.fieldType().name() + ": " + fl.getValue() + " - " + fieldLookup.getValue();
        }
        return true;
    }

    void setStoredFields(Map<String, List<Object>> storedFields) {
        assert preloadedStoredFields.containsAll(storedFields.keySet())
            : "Provided stored field that was not expected to be preloaded? " + storedFields.keySet() + " - " + preloadedStoredFields;
        this.storedFields = storedFields;
    }

    void setNextReader(LeafReaderContext ctx) {
        backUpLoader = null;
        loaderSupplier = () -> LeafFieldLookupProvider.fromStoredFields().apply(ctx);
    }

    LeafFieldLookupProvider getBackUpLoader() {
        return backUpLoader;
    }
}
