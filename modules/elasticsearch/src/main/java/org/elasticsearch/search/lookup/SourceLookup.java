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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.compress.lzf.LZF;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.CachedStreamInput;
import org.elasticsearch.common.io.stream.LZFStreamInput;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldSelector;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * @author kimchy (shay.banon)
 */
// TODO: If we are processing it in the per hit fetch phase, we cna initialize it with a source if it was loaded..
public class SourceLookup implements Map {

    private IndexReader reader;

    private int docId = -1;

    private Map<String, Object> source;

    public Map<String, Object> source() {
        return source;
    }

    private Map<String, Object> loadSourceIfNeeded() {
        if (source != null) {
            return source;
        }
        XContentParser parser = null;
        try {
            Document doc = reader.document(docId, SourceFieldSelector.INSTANCE);
            Fieldable sourceField = doc.getFieldable(SourceFieldMapper.NAME);
            byte[] source = sourceField.getBinaryValue();
            if (LZF.isCompressed(source)) {
                BytesStreamInput siBytes = new BytesStreamInput(source);
                LZFStreamInput siLzf = CachedStreamInput.cachedLzf(siBytes);
                XContentType contentType = XContentFactory.xContentType(siLzf);
                siLzf.resetToBufferStart();
                parser = XContentFactory.xContent(contentType).createParser(siLzf);
                this.source = parser.map();
            } else {
                parser = XContentFactory.xContent(source).createParser(source);
                this.source = parser.map();
            }
        } catch (Exception e) {
            throw new ElasticSearchParseException("failed to parse / load source", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
        return this.source;
    }

    public void setNextReader(IndexReader reader) {
        if (this.reader == reader) { // if we are called with the same reader, don't invalidate source
            return;
        }
        this.reader = reader;
        this.source = null;
        this.docId = -1;
    }

    public void setNextDocId(int docId) {
        if (this.docId == docId) { // if we are called with the same docId, don't invalidate source
            return;
        }
        this.docId = docId;
        this.source = null;
    }

    private final static Pattern dotPattern = Pattern.compile("\\.");

    /**
     * Returns the values associated with the path. Those are "low" level values, and it can
     * handle path expression where an array/list is navigated within.
     */
    public List<Object> getValues(String path) {
        List<Object> values = Lists.newArrayList();
        String[] pathElements = dotPattern.split(path);
        getValues(values, loadSourceIfNeeded(), pathElements, 0);
        return values;
    }

    @SuppressWarnings({"unchecked"})
    private void getValues(List<Object> values, Map<String, Object> part, String[] pathElements, int index) {
        if (index == pathElements.length) {
            return;
        }
        String currentPath = pathElements[index];
        Object currentValue = part.get(currentPath);
        if (currentValue == null) {
            return;
        }
        if (currentValue instanceof Map) {
            getValues(values, (Map<String, Object>) currentValue, pathElements, index + 1);
        } else if (currentValue instanceof List) {
            getValues(values, (List<Object>) currentValue, pathElements, index + 1);
        } else {
            values.add(currentValue);
        }
    }

    @SuppressWarnings({"unchecked"})
    private void getValues(List<Object> values, List<Object> part, String[] pathElements, int index) {
        for (Object value : part) {
            if (value == null) {
                continue;
            }
            if (value instanceof Map) {
                getValues(values, (Map<String, Object>) value, pathElements, index);
            } else if (value instanceof List) {
                getValues(values, (List<Object>) value, pathElements, index);
            } else {
                values.add(value);
            }
        }
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
