/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.lookup;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

public class LeafDocLookup implements Map<String, ScriptDocValues<?>> {

    private final Map<String, ScriptDocValues<?>> localCacheFieldData = new HashMap<>(4);

    private final Predicate<String> fieldExists;
    private final Function<String, ScriptDocValues<?>> loader;

    private int docId = -1;

    public LeafDocLookup(Predicate<String> fieldExists, Function<String, ScriptDocValues<?>> loader) {
        this.loader = loader;
        this.fieldExists = fieldExists;
    }

    LeafDocLookup(Function<String, MappedFieldType> fieldTypeLookup, Function<MappedFieldType, IndexFieldData<?>> fieldDataLookup,
                  LeafReaderContext reader) {
        this(s -> fieldTypeLookup.apply(s) != null, fieldName -> {
            final MappedFieldType fieldType = fieldTypeLookup.apply(fieldName);
            if (fieldType == null) {
                throw new IllegalArgumentException("No field found for [" + fieldName + "] in mapping");
            }
            // load fielddata on behalf of the script: otherwise it would need additional permissions
            // to deal with pagedbytes/ramusagestimator/etc
            return AccessController.doPrivileged(new PrivilegedAction<>() {
                @Override
                public ScriptDocValues<?> run() {
                    return fieldDataLookup.apply(fieldType).load(reader).getScriptValues();
                }
            });
        });
    }

    public void setDocument(int docId) {
        this.docId = docId;
    }

    @Override
    public ScriptDocValues<?> get(Object key) {
        // assume its a string...
        String fieldName = key.toString();
        ScriptDocValues<?> scriptValues = localCacheFieldData.get(fieldName);
        if (scriptValues == null) {
            scriptValues = loader.apply(fieldName);
            localCacheFieldData.put(fieldName, scriptValues);
        }
        try {
            scriptValues.setNextDocId(docId);
        } catch (IOException e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
        return scriptValues;
    }

    @Override
    public boolean containsKey(Object key) {
        // assume its a string...
        String fieldName = key.toString();
        ScriptDocValues<?> scriptValues = localCacheFieldData.get(fieldName);
        return scriptValues != null || fieldExists.test(fieldName);
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
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScriptDocValues<?> put(String key, ScriptDocValues<?> value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScriptDocValues<?> remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends String, ? extends ScriptDocValues<?>> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> keySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<ScriptDocValues<?>> values() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Map.Entry<String, ScriptDocValues<?>>> entrySet() {
        throw new UnsupportedOperationException();
    }
}
