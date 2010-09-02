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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.common.compress.lzf.LZFDecoder;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.CachedStreamInput;
import org.elasticsearch.common.io.stream.LZFStreamInput;
import org.elasticsearch.common.thread.ThreadLocals;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.function.FieldsFunction;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldSelector;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ScriptService;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author kimchy (shay.banon)
 */
public class ScriptFieldsFunction implements FieldsFunction {

    private static ThreadLocal<ThreadLocals.CleanableValue<Map<String, FieldData>>> cachedFieldData = new ThreadLocal<ThreadLocals.CleanableValue<Map<String, FieldData>>>() {
        @Override protected ThreadLocals.CleanableValue<Map<String, FieldData>> initialValue() {
            return new ThreadLocals.CleanableValue<Map<String, FieldData>>(new HashMap<String, FieldData>());
        }
    };

    private static ThreadLocal<ThreadLocals.CleanableValue<Map<String, Object>>> cachedVars = new ThreadLocal<ThreadLocals.CleanableValue<Map<String, Object>>>() {
        @Override protected ThreadLocals.CleanableValue<Map<String, Object>> initialValue() {
            return new ThreadLocals.CleanableValue<java.util.Map<java.lang.String, java.lang.Object>>(new HashMap<String, Object>());
        }
    };

    final ScriptService scriptService;

    final CompiledScript script;

    final DocMap docMap;

    final SourceMap sourceMap;

    public ScriptFieldsFunction(String script, ScriptService scriptService, MapperService mapperService, FieldDataCache fieldDataCache) {
        this.scriptService = scriptService;
        this.script = scriptService.compile(script);
        this.docMap = new DocMap(cachedFieldData.get().get(), mapperService, fieldDataCache);
        this.sourceMap = new SourceMap();
    }

    @Override public void setNextReader(IndexReader reader) {
        docMap.setNextReader(reader);
        sourceMap.setNextReader(reader);
    }

    @Override public Object execute(int docId, Map<String, Object> vars) {
        return execute(docId, vars, null);
    }

    @Override public Object execute(int docId, Map<String, Object> vars, @Nullable Map<String, Object> sameDocCache) {
        docMap.setNextDocId(docId);
        sourceMap.setNextDocId(docId);
        if (sameDocCache != null) {
            sourceMap.parsedSource((Map<String, Object>) sameDocCache.get("parsedSource"));
        }
        if (vars == null) {
            vars = cachedVars.get().get();
            vars.clear();
        }
        vars.put("doc", docMap);
        vars.put("_source", sourceMap);
        Object retVal = scriptService.execute(script, vars);
        if (sameDocCache != null) {
            sameDocCache.put("parsedSource", sourceMap.parsedSource());
        }
        return retVal;
    }

    static class SourceMap implements Map {

        private IndexReader reader;

        private int docId;

        private Map<String, Object> source;

        public Map<String, Object> parsedSource() {
            return source;
        }

        public void parsedSource(Map<String, Object> source) {
            this.source = source;
        }

        private Map<String, Object> loadSourceIfNeeded() {
            if (source != null) {
                return source;
            }
            try {
                Document doc = reader.document(docId, SourceFieldSelector.INSTANCE);
                Fieldable sourceField = doc.getFieldable(SourceFieldMapper.NAME);
                byte[] source = sourceField.getBinaryValue();
                if (LZFDecoder.isCompressed(source)) {
                    BytesStreamInput siBytes = new BytesStreamInput(source);
                    LZFStreamInput siLzf = CachedStreamInput.cachedLzf(siBytes);
                    XContentType contentType = XContentFactory.xContentType(siLzf);
                    siLzf.resetToBufferStart();
                    this.source = XContentFactory.xContent(contentType).createParser(siLzf).map();
                } else {
                    this.source = XContentFactory.xContent(source).createParser(source).map();
                }
            } catch (Exception e) {
                throw new ElasticSearchParseException("failed to parse source", e);
            }
            return this.source;
        }

        public void setNextReader(IndexReader reader) {
            this.reader = reader;
            this.source = null;
        }

        public void setNextDocId(int docId) {
            this.docId = docId;
            this.source = null;
        }

        @Override public Object get(Object key) {
            return loadSourceIfNeeded().get(key);
        }

        @Override public int size() {
            return loadSourceIfNeeded().size();
        }

        @Override public boolean isEmpty() {
            return loadSourceIfNeeded().isEmpty();
        }

        @Override public boolean containsKey(Object key) {
            return loadSourceIfNeeded().containsKey(key);
        }

        @Override public boolean containsValue(Object value) {
            return loadSourceIfNeeded().containsValue(value);
        }

        @Override public Set keySet() {
            return loadSourceIfNeeded().keySet();
        }

        @Override public Collection values() {
            return loadSourceIfNeeded().values();
        }

        @Override public Set entrySet() {
            return loadSourceIfNeeded().entrySet();
        }

        @Override public Object put(Object key, Object value) {
            throw new UnsupportedOperationException();
        }

        @Override public Object remove(Object key) {
            throw new UnsupportedOperationException();
        }

        @Override public void putAll(Map m) {
            throw new UnsupportedOperationException();
        }

        @Override public void clear() {
            throw new UnsupportedOperationException();
        }
    }

    // --- Map implementation for doc field data lookup

    static class DocMap implements Map {

        private final Map<String, FieldData> localCacheFieldData;

        private final MapperService mapperService;

        private final FieldDataCache fieldDataCache;

        private IndexReader reader;

        private int docId;

        DocMap(Map<String, FieldData> localCacheFieldData, MapperService mapperService, FieldDataCache fieldDataCache) {
            this.localCacheFieldData = localCacheFieldData;
            this.mapperService = mapperService;
            this.fieldDataCache = fieldDataCache;
        }

        public void setNextReader(IndexReader reader) {
            this.reader = reader;
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
}
