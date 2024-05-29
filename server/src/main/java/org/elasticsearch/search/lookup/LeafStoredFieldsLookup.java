/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.lookup;

import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class LeafStoredFieldsLookup implements Map<Object, FieldLookup> {

    private final Function<String, MappedFieldType> fieldTypeLookup;
    private final LeafFieldLookupProvider leafFieldLookupProvider;

    private int docId = -1;

    private final Map<String, FieldLookup> cachedFieldData = new HashMap<>();

    LeafStoredFieldsLookup(Function<String, MappedFieldType> fieldTypeLookup, LeafFieldLookupProvider leafFieldLookupProvider) {
        this.fieldTypeLookup = fieldTypeLookup;
        this.leafFieldLookupProvider = leafFieldLookupProvider;
    }

    public void setDocument(int docId) {
        if (this.docId == docId) { // if we are called with the same docId, don't invalidate source
            return;
        }
        this.docId = docId;
        clearCache();
    }

    @Override
    public FieldLookup get(Object key) {
        try {
            return loadFieldData(key.toString());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load stored fields for document [" + docId + "]", e);
        }
    }

    @Override
    public boolean containsKey(Object key) {
        try {
            loadFieldData(key.toString());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set keySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection values() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set entrySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public FieldLookup put(Object key, FieldLookup value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FieldLookup remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    private FieldLookup loadFieldData(String name) throws IOException {
        FieldLookup data = cachedFieldData.get(name);
        if (data == null) {
            MappedFieldType fieldType = fieldTypeLookup.apply(name);
            if (fieldType == null) {
                throw new IllegalArgumentException("No field found for [" + name + "] in mapping");
            }
            data = new FieldLookup(fieldType);
            cachedFieldData.put(name, data);
        }
        if (data.isLoaded() == false) {
            leafFieldLookupProvider.populateFieldLookup(data, docId);
        }
        return data;
    }

    private void clearCache() {
        if (cachedFieldData.isEmpty()) {
            /*
             * This code is in the hot path for things like ScoreScript and
             * runtime fields but the map is almost always empty. So we
             * bail early then instead of building the entrySet.
             */
            return;
        }
        for (Entry<String, FieldLookup> entry : cachedFieldData.entrySet()) {
            entry.getValue().clear();
        }
    }
}
