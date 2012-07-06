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

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.SourceFieldSelector;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
// TODO: If we are processing it in the per hit fetch phase, we cna initialize it with a source if it was loaded..
public class SourceLookup implements Map {

    private IndexReader reader;

    private int docId = -1;

    private BytesReference sourceAsBytes;
    private Map<String, Object> source;

    public Map<String, Object> source() {
        return source;
    }

    private Map<String, Object> loadSourceIfNeeded() {
        if (source != null) {
            return source;
        }
        if (sourceAsBytes != null) {
            source = sourceAsMap(sourceAsBytes);
            return source;
        }
        try {
            Document doc = reader.document(docId, SourceFieldSelector.INSTANCE);
            Fieldable sourceField = doc.getFieldable(SourceFieldMapper.NAME);
            if (sourceField == null) {
                source = ImmutableMap.of();
            } else {
                this.source = sourceAsMap(sourceField.getBinaryValue(), sourceField.getBinaryOffset(), sourceField.getBinaryLength());
            }
        } catch (Exception e) {
            throw new ElasticSearchParseException("failed to parse / load source", e);
        }
        return this.source;
    }

    public static Map<String, Object> sourceAsMap(BytesReference source) throws ElasticSearchParseException {
        return XContentHelper.convertToMap(source, false).v2();
    }

    public static Map<String, Object> sourceAsMap(byte[] bytes, int offset, int length) throws ElasticSearchParseException {
        return XContentHelper.convertToMap(bytes, offset, length, false).v2();
    }

    public void setNextReader(IndexReader reader) {
        if (this.reader == reader) { // if we are called with the same reader, don't invalidate source
            return;
        }
        this.reader = reader;
        this.source = null;
        this.sourceAsBytes = null;
        this.docId = -1;
    }

    public void setNextDocId(int docId) {
        if (this.docId == docId) { // if we are called with the same docId, don't invalidate source
            return;
        }
        this.docId = docId;
        this.sourceAsBytes = null;
        this.source = null;
    }

    public void setNextSource(BytesReference source) {
        this.sourceAsBytes = source;
    }

    public void setNextSource(Map<String, Object> source) {
        this.source = source;
    }

    /**
     * Returns the values associated with the path. Those are "low" level values, and it can
     * handle path expression where an array/list is navigated within.
     */
    public List<Object> extractRawValues(String path) {
        return XContentMapValues.extractRawValues(path, loadSourceIfNeeded());
    }

    public Object filter(String[] includes, String[] excludes) {
        return XContentMapValues.filter(loadSourceIfNeeded(), includes, excludes);
    }

    public Object extractValue(String path) {
        return XContentMapValues.extractValue(path, loadSourceIfNeeded());
    }

    @Override
    public Object get(Object key) {
        return loadSourceIfNeeded().get(key);
    }

    @Override
    public int size() {
        return loadSourceIfNeeded().size();
    }

    @Override
    public boolean isEmpty() {
        return loadSourceIfNeeded().isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return loadSourceIfNeeded().containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return loadSourceIfNeeded().containsValue(value);
    }

    @Override
    public Set keySet() {
        return loadSourceIfNeeded().keySet();
    }

    @Override
    public Collection values() {
        return loadSourceIfNeeded().values();
    }

    @Override
    public Set entrySet() {
        return loadSourceIfNeeded().entrySet();
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
}
