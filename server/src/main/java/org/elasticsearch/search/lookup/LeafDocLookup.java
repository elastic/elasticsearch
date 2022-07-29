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
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SourceValueFetcherIndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.Field;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

public class LeafDocLookup implements Map<String, ScriptDocValues<?>> {

    private final Function<String, MappedFieldType> fieldTypeLookup;
    private final BiFunction<MappedFieldType, MappedFieldType.FielddataOperation, IndexFieldData<?>> fieldDataLookup;
    private final LeafReaderContext reader;

    private int docId = -1;

    private final Map<String, DocValuesScriptFieldFactory> localCacheScriptFieldData = Maps.newMapWithExpectedSize(4);

    LeafDocLookup(
        Function<String, MappedFieldType> fieldTypeLookup,
        BiFunction<MappedFieldType, MappedFieldType.FielddataOperation, IndexFieldData<?>> fieldDataLookup,
        LeafReaderContext reader
    ) {
        this.fieldTypeLookup = fieldTypeLookup;
        this.fieldDataLookup = fieldDataLookup;
        this.reader = reader;
    }

    public void setDocument(int docId) {
        this.docId = docId;
    }

    protected DocValuesScriptFieldFactory getScriptFieldFactory(String fieldName, MappedFieldType.FielddataOperation options) {
        DocValuesScriptFieldFactory factory = localCacheScriptFieldData.get(fieldName);

        // do not use cached source fallback fields for old style doc access
        if (options == MappedFieldType.FielddataOperation.SEARCH
            && factory instanceof SourceValueFetcherIndexFieldData.ValueFetcherDocValues) {
            factory = null;
        }

        if (factory == null) {
            final MappedFieldType fieldType = fieldTypeLookup.apply(fieldName);

            if (fieldType == null) {
                throw new IllegalArgumentException("No field found for [" + fieldName + "] in mapping");
            }

            // Load the field data on behalf of the script. Otherwise, it would require
            // additional permissions to deal with pagedbytes/ramusagestimator/etc.
            factory = AccessController.doPrivileged(new PrivilegedAction<DocValuesScriptFieldFactory>() {
                @Override
                public DocValuesScriptFieldFactory run() {
                    return fieldDataLookup.apply(fieldType, options).load(reader).getScriptFieldFactory(fieldName);
                }
            });

            localCacheScriptFieldData.put(fieldName, factory);
        }

        try {
            factory.setNextDocId(docId);
        } catch (IOException ioe) {
            throw ExceptionsHelper.convertToElastic(ioe);
        }

        return factory;
    }

    public Field<?> getScriptField(String fieldName) {
        return getScriptFieldFactory(fieldName, MappedFieldType.FielddataOperation.SCRIPT).toScriptField();
    }

    @Override
    public ScriptDocValues<?> get(Object key) {
        return getScriptFieldFactory(key.toString(), MappedFieldType.FielddataOperation.SEARCH).toScriptDocValues();
    }

    @Override
    public boolean containsKey(Object key) {
        String fieldName = key.toString();
        return localCacheScriptFieldData.get(fieldName) != null || fieldTypeLookup.apply(fieldName) != null;
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
