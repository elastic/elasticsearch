/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.lookup;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * @author kimchy (shay.banon)
 */
public class DocLookup implements Map {

    private final Map<String, FieldData> localCacheFieldData = Maps.newHashMapWithExpectedSize(4);

    private final MapperService mapperService;

    private final FieldDataCache fieldDataCache;

    private IndexReader reader;

    private int docId = -1;

    DocLookup(MapperService mapperService, FieldDataCache fieldDataCache) {
        this.mapperService = mapperService;
        this.fieldDataCache = fieldDataCache;
    }

    public void setNextReader(IndexReader reader) {
        if (this.reader == reader) { // if we are called with the same reader, don't invalidate source
            return;
        }
        this.reader = reader;
        this.docId = -1;
        localCacheFieldData.clear();
    }

    public void setNextDocId(int docId) {
        this.docId = docId;
    }

    @Override public Object get(Object key) {
        // assume its a string...
        String fieldName = key.toString();
        FieldData fieldData = localCacheFieldData.get(fieldName);
        if (fieldData == null) {
            FieldMapper mapper = mapperService.smartNameFieldMapper(fieldName);
            if (mapper == null) {
                throw new ElasticSearchIllegalArgumentException("No field found for [" + fieldName + "]");
            }
            try {
                fieldData = fieldDataCache.cache(mapper.fieldDataType(), reader, mapper.names().indexName());
            } catch (IOException e) {
                throw new ElasticSearchException("Failed to load field data for [" + fieldName + "]", e);
            }
            localCacheFieldData.put(fieldName, fieldData);
        }
        return fieldData.docFieldData(docId);
    }

    public boolean containsKey(Object key) {
        // assume its a string...
        String fieldName = key.toString();
        FieldData fieldData = localCacheFieldData.get(fieldName);
        if (fieldData == null) {
            FieldMapper mapper = mapperService.smartNameFieldMapper(fieldName);
            if (mapper == null) {
                return false;
            }
        }
        return true;
    }

    public int size() {
        throw new UnsupportedOperationException();
    }

    public boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    public Object put(Object key, Object value) {
        throw new UnsupportedOperationException();
    }

    public Object remove(Object key) {
        throw new UnsupportedOperationException();
    }

    public void putAll(Map m) {
        throw new UnsupportedOperationException();
    }

    public void clear() {
        throw new UnsupportedOperationException();
    }

    public Set keySet() {
        throw new UnsupportedOperationException();
    }

    public Collection values() {
        throw new UnsupportedOperationException();
    }

    public Set entrySet() {
        throw new UnsupportedOperationException();
    }
}
