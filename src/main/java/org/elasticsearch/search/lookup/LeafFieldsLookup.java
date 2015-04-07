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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.lucene.index.LeafReader;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.fieldvisitor.SingleFieldsVisitor;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class LeafFieldsLookup implements Map {

    private final MapperService mapperService;

    @Nullable
    private final String[] types;

    private final LeafReader reader;

    private int docId = -1;

    private final Map<String, FieldLookup> cachedFieldData = Maps.newHashMap();

    private final SingleFieldsVisitor fieldVisitor;

    LeafFieldsLookup(MapperService mapperService, @Nullable String[] types, LeafReader reader) {
        this.mapperService = mapperService;
        this.types = types;
        this.reader = reader;
        this.fieldVisitor = new SingleFieldsVisitor(null);
    }

    public void setDocument(int docId) {
        if (this.docId == docId) { // if we are called with the same docId, don't invalidate source
            return;
        }
        this.docId = docId;
        clearCache();
    }


    @Override
    public Object get(Object key) {
        return loadFieldData(key.toString());
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

    private FieldLookup loadFieldData(String name) {
        FieldLookup data = cachedFieldData.get(name);
        if (data == null) {
            FieldMapper mapper = mapperService.smartNameFieldMapper(name, types);
            if (mapper == null) {
                throw new ElasticsearchIllegalArgumentException("No field found for [" + name + "] in mapping with types " + Arrays.toString(types) + "");
            }
            data = new FieldLookup(mapper);
            cachedFieldData.put(name, data);
        }
        if (data.fields() == null) {
            String fieldName = data.mapper().names().indexName();
            fieldVisitor.reset(fieldName);
            try {
                reader.document(docId, fieldVisitor);
                fieldVisitor.postProcess(data.mapper());
                data.fields(ImmutableMap.of(name, fieldVisitor.fields().get(data.mapper().names().indexName())));
            } catch (IOException e) {
                throw new ElasticsearchParseException("failed to load field [" + name + "]", e);
            }
        }
        return data;
    }

    private void clearCache() {
        for (Entry<String, FieldLookup> entry : cachedFieldData.entrySet()) {
            entry.getValue().clear();
        }
    }

}
