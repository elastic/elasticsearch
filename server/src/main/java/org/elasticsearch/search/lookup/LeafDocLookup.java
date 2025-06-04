/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.lookup;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SourceValueFetcherIndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.Field;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.elasticsearch.index.mapper.MappedFieldType.FielddataOperation.SCRIPT;
import static org.elasticsearch.index.mapper.MappedFieldType.FielddataOperation.SEARCH;

public class LeafDocLookup implements Map<String, ScriptDocValues<?>> {

    private final Function<String, MappedFieldType> fieldTypeLookup;
    private final BiFunction<MappedFieldType, MappedFieldType.FielddataOperation, IndexFieldData<?>> fieldDataLookup;
    private final LeafReaderContext reader;

    /*
      Field factories require a privileged action to advance to docids (files could be loaded). This wrapper
      is a place for that privileged action to be cached so that it does not need to be recreated for every document.
     */
    class FieldFactoryWrapper {
        final DocValuesScriptFieldFactory factory;

        FieldFactoryWrapper(DocValuesScriptFieldFactory factory) {
            this.factory = factory;
        }

        // advances the factory to the current docid for the enclosing LeafDocLookup
        void advanceToDoc() {
            try {
                factory.setNextDocId(docId);
            } catch (IOException ioe) {
                throw ExceptionsHelper.convertToElastic(ioe);
            }
        }
    }

    private int docId = -1;

    /*
    We run parallel caches for the fields-access API ( field('f') ) and
    the doc-access API.( doc['f'] ) for two reasons:
    1. correctness - the field cache can store fields that retrieve values
                     from both doc values and source whereas the doc cache
                     can only store doc values. This leads to cases such as text
                     field where sharing a cache could lead to incorrect results in a
                     script that uses both types of access (likely common during upgrades)
    2. performance - to keep the performance reasonable we move all caching updates to
                     per-segment computation as opposed to per-document computation
    Note that we share doc values between both caches when possible.
    */
    final Map<String, FieldFactoryWrapper> fieldFactoryCache = Maps.newMapWithExpectedSize(4);
    final Map<String, FieldFactoryWrapper> docFactoryCache = Maps.newMapWithExpectedSize(4);

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

    // used to load data for a field-style api accessor
    private FieldFactoryWrapper getFactoryForField(String fieldName) {
        final MappedFieldType fieldType = fieldTypeLookup.apply(fieldName);

        if (fieldType == null) {
            throw new IllegalArgumentException("No field found for [" + fieldName + "] in mapping");
        }

        IndexFieldData<?> indexFieldData = fieldDataLookup.apply(fieldType, SCRIPT);

        FieldFactoryWrapper docFactory = null;

        if (docFactoryCache.isEmpty() == false) {
            docFactory = docFactoryCache.get(fieldName);
        }

        // if this field has already been accessed via the doc-access API and the field-access API
        // uses doc values then we share to avoid double-loading
        FieldFactoryWrapper fieldFactory;
        if (docFactory != null && indexFieldData instanceof SourceValueFetcherIndexFieldData == false) {
            fieldFactory = docFactory;
        } else {
            fieldFactory = new FieldFactoryWrapper(indexFieldData.load(reader).getScriptFieldFactory(fieldName));
        }

        fieldFactoryCache.put(fieldName, fieldFactory);

        return fieldFactory;
    }

    public Field<?> getScriptField(String fieldName) {
        FieldFactoryWrapper factoryWrapper = fieldFactoryCache.get(fieldName);

        if (factoryWrapper == null) {
            factoryWrapper = getFactoryForField(fieldName);
        }
        factoryWrapper.advanceToDoc();

        return factoryWrapper.factory.toScriptField();
    }

    // used to load data for a doc-style api accessor
    private FieldFactoryWrapper getFactoryForDoc(String fieldName) {
        final MappedFieldType fieldType = fieldTypeLookup.apply(fieldName);

        if (fieldType == null) {
            throw new IllegalArgumentException("No field found for [" + fieldName + "] in mapping");
        }

        FieldFactoryWrapper docFactory = null;
        FieldFactoryWrapper fieldFactory = null;

        if (fieldFactoryCache.isEmpty() == false) {
            fieldFactory = fieldFactoryCache.get(fieldName);
        }

        if (fieldFactory != null) {
            IndexFieldData<?> fieldIndexFieldData = fieldDataLookup.apply(fieldType, SCRIPT);

            // if this field has already been accessed via the field-access API and the field-access API
            // uses doc values then we share to avoid double-loading
            if (fieldIndexFieldData instanceof SourceValueFetcherIndexFieldData == false) {
                docFactory = fieldFactory;
            }
        }

        if (docFactory == null) {
            IndexFieldData<?> indexFieldData = fieldDataLookup.apply(fieldType, SEARCH);
            docFactory = new FieldFactoryWrapper(indexFieldData.load(reader).getScriptFieldFactory(fieldName));
        }

        docFactoryCache.put(fieldName, docFactory);

        return docFactory;
    }

    @Override
    public ScriptDocValues<?> get(Object key) {
        String fieldName = key.toString();
        FieldFactoryWrapper factoryWrapper = docFactoryCache.get(fieldName);

        if (factoryWrapper == null) {
            factoryWrapper = getFactoryForDoc(key.toString());
        }
        factoryWrapper.advanceToDoc();

        return factoryWrapper.factory.toScriptDocValues();
    }

    @Override
    public boolean containsKey(Object key) {
        String fieldName = key.toString();
        return docFactoryCache.containsKey(key) || fieldFactoryCache.containsKey(key) || fieldTypeLookup.apply(fieldName) != null;
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
