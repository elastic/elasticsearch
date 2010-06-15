/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.field.function.script;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.thread.ThreadLocals;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.FieldDataOptions;
import org.elasticsearch.index.field.function.FieldsFunction;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author kimchy (shay.banon)
 */
public class ScriptFieldsFunction implements FieldsFunction, Map {

    private static ThreadLocal<ThreadLocals.CleanableValue<Map<String, FieldData>>> cachedFieldData = new ThreadLocal<ThreadLocals.CleanableValue<Map<String, FieldData>>>() {
        @Override protected ThreadLocals.CleanableValue<Map<String, FieldData>> initialValue() {
            return new ThreadLocals.CleanableValue<Map<String, FieldData>>(new HashMap<String, FieldData>());
        }
    };

    final Object script;

    final MapperService mapperService;

    final FieldDataCache fieldDataCache;

    final ScriptService scriptService;

    final Map<String, FieldData> localCacheFieldData = cachedFieldData.get().get();

    IndexReader reader;

    int docId;

    public ScriptFieldsFunction(String script, ScriptService scriptService, MapperService mapperService, FieldDataCache fieldDataCache) {
        this.scriptService = scriptService;
        this.mapperService = mapperService;
        this.fieldDataCache = fieldDataCache;
        this.script = scriptService.compile(script);
    }

    @Override public void setNextReader(IndexReader reader) {
        this.reader = reader;
        localCacheFieldData.clear();
    }

    @Override public Object execute(int docId, Map vars) {
        this.docId = docId;
        vars.put("doc", this);
        return scriptService.execute(script, vars);
    }

    // --- Map implementation for doc field data lookup

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
                fieldData = fieldDataCache.cache(mapper.fieldDataType(), reader, mapper.names().indexName(), FieldDataOptions.fieldDataOptions().withFreqs(false));
            } catch (IOException e) {
                throw new ElasticSearchException("Failed to load field data for [" + fieldName + "]", e);
            }
            localCacheFieldData.put(fieldName, fieldData);
        }
        return fieldData.docFieldData(docId);
    }

    public int size() {
        throw new UnsupportedOperationException();
    }

    public boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    public boolean containsKey(Object key) {
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
