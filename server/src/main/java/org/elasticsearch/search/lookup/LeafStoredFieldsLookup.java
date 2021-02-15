/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.lookup;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fieldvisitor.StoredFieldsLoader;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Uid;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

@SuppressWarnings({"unchecked", "rawtypes"})
public class LeafStoredFieldsLookup implements Map<Object, Object> {

    private final Function<String, MappedFieldType> fieldTypeLookup;
    private final StoredFieldsLoader fieldsLoader;

    private int docId = -1;
    private boolean loaded = false;

    private final Map<String, FieldLookup> cachedFieldData = new HashMap<>();
    private final Map<String, MappedFieldType> fieldsToLoad = new HashMap<>();

    private final StoredFieldVisitor fieldsVisitor = new StoredFieldVisitor() {

        private void addValue(String field, Object value) {
            FieldLookup fieldLookup = cachedFieldData.computeIfAbsent(field, f -> new FieldLookup(fieldTypeLookup.apply(f)));
            fieldLookup.addValue(value);
        }

        @Override
        public Status needsField(FieldInfo fieldInfo) {
            return fieldsToLoad.containsKey(fieldInfo.name) ? Status.YES : Status.NO;
        }

        @Override
        public void binaryField(FieldInfo fieldInfo, byte[] value) {
            // TODO can we turn this round so that MappedFieldTypes register what
            // type they are and also handle converting values?
            if (IdFieldMapper.NAME.equals(fieldInfo.name)) {
                addValue(fieldInfo.name, Uid.decodeId(value));
            } else {
                addValue(fieldInfo.name, new BytesRef(value));
            }
        }

        @Override
        public void stringField(FieldInfo fieldInfo, byte[] value) {
            addValue(fieldInfo.name, new String(value, StandardCharsets.UTF_8));
        }

        @Override
        public void intField(FieldInfo fieldInfo, int value) {
            addValue(fieldInfo.name, value);
        }

        @Override
        public void longField(FieldInfo fieldInfo, long value) {
            addValue(fieldInfo.name, value);
        }

        @Override
        public void floatField(FieldInfo fieldInfo, float value) {
            addValue(fieldInfo.name, value);
        }

        @Override
        public void doubleField(FieldInfo fieldInfo, double value) {
            addValue(fieldInfo.name, value);
        }
    };

    LeafStoredFieldsLookup(Function<String, MappedFieldType> fieldTypeLookup, StoredFieldsLoader fieldsLoader) {
        this.fieldTypeLookup = fieldTypeLookup;
        this.fieldsLoader = fieldsLoader;
    }

    public void setDocument(int docId) {
        if (this.docId == docId) { // if we are called with the same docId, don't invalidate source
            return;
        }
        this.docId = docId;
        clearCache();
    }

    public void registerFieldToLoad(MappedFieldType field) {
        if (field.isStored() == false) {
            return;
        }
        fieldsToLoad.put(field.name(), field);
        cachedFieldData.put(field.name(), new FieldLookup(field));
    }

    @Override
    public Object get(Object key) {
        MappedFieldType field = fieldTypeLookup.apply(Objects.toString(key));
        if (field == null || field.isStored() == false) {
            return null;
        }
        if (fieldsToLoad.containsKey(field.name()) == false) {
            registerFieldToLoad(field);
            loaded = false;
        }
        loadFieldData();
        return cachedFieldData.get(field.name());
    }

    @Override
    public boolean containsKey(Object key) {
        MappedFieldType field = fieldTypeLookup.apply(Objects.toString(key));
        if (field == null || field.isStored() == false) {
            return false;
        }
        if (fieldsToLoad.containsKey(field.name()) == false) {
            registerFieldToLoad(field);
            loaded = false;
        }
        loadFieldData();
        return cachedFieldData.get(field.name()).isEmpty() == false;
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
    public Object put(Object key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object remove(Object key) {
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

    private void loadFieldData() {
        if (loaded) {
            return;
        }
        try {
            fieldsLoader.loadStoredFields(docId, fieldsVisitor);
            loaded = true;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
        loaded = false;
    }
}
