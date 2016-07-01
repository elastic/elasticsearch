/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.lookup;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class LeafDocLookup implements Map {

    private final Map<String, ScriptDocValues> localCacheFieldData = new HashMap<>(4);

    private final MapperService mapperService;
    private final IndexFieldDataService fieldDataService;

    @Nullable
    private final String[] types;

    private final LeafReaderContext reader;

    private int docId = -1;

    LeafDocLookup(MapperService mapperService, IndexFieldDataService fieldDataService, @Nullable String[] types, LeafReaderContext reader) {
        this.mapperService = mapperService;
        this.fieldDataService = fieldDataService;
        this.types = types;
        this.reader = reader;
    }

    public MapperService mapperService() {
        return this.mapperService;
    }

    public IndexFieldDataService fieldDataService() {
        return this.fieldDataService;
    }

    public void setDocument(int docId) {
        this.docId = docId;
    }

    @Override
    public Object get(Object key) {
        // assume its a string...
        String fieldName = key.toString();
        ScriptDocValues scriptValues = localCacheFieldData.get(fieldName);
        if (scriptValues == null) {
            final MappedFieldType fieldType = mapperService.fullName(fieldName);
            if (fieldType == null) {
                throw new IllegalArgumentException("No field found for [" + fieldName + "] in mapping with types " + Arrays.toString(types) + "");
            }
            // load fielddata on behalf of the script: otherwise it would need additional permissions
            // to deal with pagedbytes/ramusagestimator/etc
            scriptValues = AccessController.doPrivileged(new PrivilegedAction<ScriptDocValues>() {
                @Override
                public ScriptDocValues run() {
                    return fieldDataService.getForField(fieldType).load(reader).getScriptValues();
                }
            });
            localCacheFieldData.put(fieldName, scriptValues);
        }
        scriptValues.setNextDocId(docId);
        return scriptValues;
    }

    @Override
    public boolean containsKey(Object key) {
        // assume its a string...
        String fieldName = key.toString();
        ScriptDocValues scriptValues = localCacheFieldData.get(fieldName);
        if (scriptValues == null) {
            MappedFieldType fieldType = mapperService.fullName(fieldName);
            if (fieldType == null) {
                return false;
            }
        }
        return true;
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
    public Object put(Object key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
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
}
