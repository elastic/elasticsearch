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
package org.elasticsearch.search.lookup.impl;

import com.google.common.collect.Maps;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.lookup.DoubleDocValues;
import org.elasticsearch.search.lookup.GeoDocValues;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.LongDocValues;
import org.elasticsearch.search.lookup.StringDocValues;
import org.apache.lucene.index.LeafReaderContext;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class LeafDocLookupImpl implements LeafDocLookup {

    private final Map<String, ScriptDocValuesImpl> localCacheFieldData = Maps.newHashMapWithExpectedSize(4);

    private final MapperService mapperService;
    private final IndexFieldDataService fieldDataService;

    @Nullable
    private final String[] types;

    private final LeafReaderContext reader;

    private int docId = -1;

    LeafDocLookupImpl(MapperService mapperService, IndexFieldDataService fieldDataService, @Nullable String[] types, LeafReaderContext reader) {
        this.mapperService = mapperService;
        this.fieldDataService = fieldDataService;
        this.types = types;
        this.reader = reader;
    }

    public void setDocument(int docId) {
        this.docId = docId;
    }

    @Override
    public Object get(Object key) {
        // assume its a string...
        String fieldName = key.toString();
        ScriptDocValuesImpl scriptValues = localCacheFieldData.get(fieldName);
        if (scriptValues == null) {
            final FieldMapper mapper = mapperService.smartNameFieldMapper(fieldName, types);
            if (mapper == null) {
                throw new IllegalArgumentException("No field found for [" + fieldName + "] in mapping with types " + Arrays.toString(types) + "");
            }
            // load fielddata on behalf of the script: otherwise it would need additional permissions
            // to deal with pagedbytes/ramusagestimator/etc
            scriptValues = AccessController.doPrivileged(new PrivilegedAction<ScriptDocValuesImpl>() {
                @Override
                public ScriptDocValuesImpl run() {
                    return fieldDataService.getForField(mapper).load(reader).getScriptValues();
                }
            });
            localCacheFieldData.put(fieldName, scriptValues);
        }
        scriptValues.setNextDocId(docId);
        if (scriptValues instanceof DoubleDocValues) {
            return LookupImpl.createProxy(DoubleDocValues.class, (DoubleDocValues) scriptValues);
        } else if (scriptValues instanceof GeoDocValues) {
            return LookupImpl.createProxy(GeoDocValues.class, (GeoDocValues) scriptValues);
        } else if (scriptValues instanceof LongDocValues) {
            return LookupImpl.createProxy(LongDocValues.class, (LongDocValues) scriptValues);
        } else if (scriptValues instanceof StringDocValues) {
            return LookupImpl.createProxy(StringDocValues.class, (StringDocValues) scriptValues);
        } else {
            throw new AssertionError();
        }
    }

    @Override
    public boolean containsKey(Object key) {
        // assume its a string...
        String fieldName = key.toString();
        ScriptDocValuesImpl scriptValues = localCacheFieldData.get(fieldName);
        if (scriptValues == null) {
            FieldMapper mapper = mapperService.smartNameFieldMapper(fieldName, types);
            if (mapper == null) {
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
