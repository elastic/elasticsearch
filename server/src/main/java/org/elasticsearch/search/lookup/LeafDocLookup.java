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
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

public class LeafDocLookup implements Map<String, ScriptDocValues<?>> {

    private final Map<String, ScriptDocValues<?>> localCacheFieldData = new HashMap<>(4);
    private final Function<String, MappedFieldType> fieldTypeLookup;
    private final Function<MappedFieldType, IndexFieldData<?>> fieldDataLookup;
    private final Map<FormatKey, ScriptDocValues<?>> localCacheFormattedData = new HashMap<>();

    private final LeafReaderContext reader;

    private int docId = -1;

    LeafDocLookup(Function<String, MappedFieldType> fieldTypeLookup, Function<MappedFieldType, IndexFieldData<?>> fieldDataLookup,
                  LeafReaderContext reader) {
        this.fieldTypeLookup = fieldTypeLookup;
        this.fieldDataLookup = fieldDataLookup;
        this.reader = reader;
    }

    public void setDocument(int docId) {
        this.docId = docId;
    }

    private static class FormatKey {
        final String field;
        final DocValueFormat format;

        private FormatKey(String field, DocValueFormat format) {
            this.field = field;
            this.format = format;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FormatKey formatKey = (FormatKey) o;
            return Objects.equals(field, formatKey.field) && Objects.equals(format, formatKey.format);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, format);
        }
    }

    public ScriptDocValues<?> get(String name, DocValueFormat format) {
        FormatKey key = new FormatKey(name, format);
        ScriptDocValues<?> scriptValues = localCacheFormattedData.get(key);
        if (scriptValues == null) {
            MappedFieldType fieldType = fieldTypeLookup.apply(name);
            if (fieldType == null) {
                throw new IllegalArgumentException("Unknown field [" + name + "]");
            }
            // load fielddata on behalf of the script: otherwise it would need additional permissions
            // to deal with pagedbytes/ramusagestimator/etc
            scriptValues = AccessController.doPrivileged(new PrivilegedAction<ScriptDocValues<?>>() {
                @Override
                public ScriptDocValues<?> run() {
                    return ScriptDocValues.wrap(fieldDataLookup.apply(fieldType).load(reader).getFormattedValues(format));
                }
            });
            localCacheFormattedData.put(key, scriptValues);
        }
        try {
            scriptValues.setNextDocId(docId);
        } catch (IOException e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
        return scriptValues;
    }

    @Override
    public ScriptDocValues<?> get(Object key) {
        // assume its a string...
        String fieldName = key.toString();
        ScriptDocValues<?> scriptValues = localCacheFieldData.get(fieldName);
        if (scriptValues == null) {
            final MappedFieldType fieldType = fieldTypeLookup.apply(fieldName);
            if (fieldType == null) {
                throw new IllegalArgumentException("No field found for [" + fieldName + "] in mapping");
            }
            // load fielddata on behalf of the script: otherwise it would need additional permissions
            // to deal with pagedbytes/ramusagestimator/etc
            scriptValues = AccessController.doPrivileged(new PrivilegedAction<ScriptDocValues<?>>() {
                @Override
                public ScriptDocValues<?> run() {
                    return fieldDataLookup.apply(fieldType).load(reader).getScriptValues();
                }
            });
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
        return scriptValues != null || fieldTypeLookup.apply(fieldName) != null;
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
