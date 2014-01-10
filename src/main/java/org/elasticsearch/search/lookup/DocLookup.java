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

import com.google.common.collect.Maps;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fielddata.ScriptDocValues;
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
public class DocLookup implements Map {

    private final Map<String, ScriptDocValues> localCacheFieldData = Maps.newHashMapWithExpectedSize(4);

    private final MapperService mapperService;
    private final IndexFieldDataService fieldDataService;

    @Nullable
    private final String[] types;

    private AtomicReaderContext reader;

    private Scorer scorer;

    private int docId = -1;

    DocLookup(MapperService mapperService, IndexFieldDataService fieldDataService, @Nullable String[] types) {
        this.mapperService = mapperService;
        this.fieldDataService = fieldDataService;
        this.types = types;
    }

    public MapperService mapperService() {
        return this.mapperService;
    }

    public void setNextReader(AtomicReaderContext context) {
        if (this.reader == context) { // if we are called with the same reader, don't invalidate source
            return;
        }
        this.reader = context;
        this.docId = -1;
        localCacheFieldData.clear();
    }

    public void setScorer(Scorer scorer) {
        this.scorer = scorer;
    }

    public void setNextDocId(int docId) {
        this.docId = docId;
    }

    public float score() throws IOException {
        return scorer.score();
    }

    public float getScore() throws IOException {
        return scorer.score();
    }

    @Override
    public Object get(Object key) {
        // assume its a string...
        String fieldName = key.toString();
        ScriptDocValues scriptValues = localCacheFieldData.get(fieldName);
        if (scriptValues == null) {
            FieldMapper mapper = mapperService.smartNameFieldMapper(fieldName, types);
            if (mapper == null) {
                throw new ElasticsearchIllegalArgumentException("No field found for [" + fieldName + "] in mapping with types " + Arrays.toString(types) + "");
            }
            scriptValues = fieldDataService.getForField(mapper).load(reader).getScriptValues();
            localCacheFieldData.put(fieldName, scriptValues);
        }
        scriptValues.setNextDocId(docId);
        return scriptValues;
    }

    public boolean containsKey(Object key) {
        // assume its a string...
        String fieldName = key.toString();
        ScriptDocValues scriptValues = localCacheFieldData.get(fieldName);
        if (scriptValues == null) {
            FieldMapper mapper = mapperService.smartNameFieldMapper(fieldName, types);
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
