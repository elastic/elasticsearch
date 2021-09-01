/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.lookup;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;

public class SourceLookup implements Map<String, Object> {

    private LeafReader reader;
    CheckedBiConsumer<Integer, FieldsVisitor, IOException> fieldReader;

    private int docId = -1;

    private BytesReference sourceAsBytes;
    private Map<String, Object> source;
    private XContentType sourceContentType;

    public XContentType sourceContentType() {
        return sourceContentType;
    }

    public int docId() {
        return docId;
    }

    /**
     * Return the source as a map that will be unchanged when the lookup
     * moves to a different document.
     * <p>
     * Important: This can lose precision on numbers with a decimal point. It
     * converts numbers like {@code "n": 1234.567} to a {@code double} which
     * only has 52 bits of precision in the mantissa. This will come up most
     * frequently when folks write nanosecond precision dates as a decimal
     * number.
     */
    public Map<String, Object> source() {
        if (source != null) {
            return source;
        }
        if (sourceAsBytes != null) {
            Tuple<XContentType, Map<String, Object>> tuple = sourceAsMapAndType(sourceAsBytes);
            sourceContentType = tuple.v1();
            source = tuple.v2();
            return source;
        }
        try {
            FieldsVisitor sourceFieldVisitor = new FieldsVisitor(true);
            fieldReader.accept(docId, sourceFieldVisitor);
            BytesReference source = sourceFieldVisitor.source();
            if (source == null) {
                this.source = emptyMap();
                this.sourceContentType = null;
            } else {
                Tuple<XContentType, Map<String, Object>> tuple = sourceAsMapAndType(source);
                this.sourceContentType = tuple.v1();
                this.source = tuple.v2();
            }
        } catch (Exception e) {
            throw new ElasticsearchParseException("failed to parse / load source", e);
        }
        return this.source;
    }

    private static Tuple<XContentType, Map<String, Object>> sourceAsMapAndType(BytesReference source) throws ElasticsearchParseException {
        return XContentHelper.convertToMap(source, false);
    }

    /**
     * Get the source as a {@link Map} of java objects.
     * <p>
     * Important: This can lose precision on numbers with a decimal point. It
     * converts numbers like {@code "n": 1234.567} to a {@code double} which
     * only has 52 bits of precision in the mantissa. This will come up most
     * frequently when folks write nanosecond precision dates as a decimal
     * number.
     */
    public static Map<String, Object> sourceAsMap(BytesReference source) throws ElasticsearchParseException {
        return sourceAsMapAndType(source).v2();
    }

    public void setSegmentAndDocument(
        LeafReaderContext context,
        int docId
    ) {
        if (this.reader == context.reader() && this.docId == docId) {
            // if we are called with the same document, don't invalidate source
            return;
        }
        if (this.reader != context.reader()) {
            this.reader = context.reader();
            // only reset reader and fieldReader when reader changes
            if (context.reader() instanceof SequentialStoredFieldsLeafReader) {
                // All the docs to fetch are adjacent but Lucene stored fields are optimized
                // for random access and don't optimize for sequential access - except for merging.
                // So we do a little hack here and pretend we're going to do merges in order to
                // get better sequential access.
                SequentialStoredFieldsLeafReader lf = (SequentialStoredFieldsLeafReader) context.reader();
                fieldReader = lf.getSequentialStoredFieldsReader()::visitDocument;
            } else {
                fieldReader = context.reader()::document;
            }
        }
        this.source = null;
        this.sourceAsBytes = null;
        this.docId = docId;
    }

    public void setSource(BytesReference source) {
        this.sourceAsBytes = source;
    }

    public void setSourceContentType(XContentType sourceContentType) {
        this.sourceContentType = sourceContentType;
    }

    public void setSource(Map<String, Object> source) {
        this.source = source;
    }

    /**
     * Internal source representation, might be compressed....
     */
    public BytesReference internalSourceRef() {
        return sourceAsBytes;
    }

    /**
     * Returns the values associated with the path. Those are "low" level values, and it can
     * handle path expression where an array/list is navigated within.
     */
    public List<Object> extractRawValues(String path) {
        return XContentMapValues.extractRawValues(path, source());
    }

    /**
     * For the provided path, return its value in the source.
     *
     * Note that in contrast with {@link SourceLookup#extractRawValues}, array and object values
     * can be returned.
     *
     * @param path the value's path in the source.
     * @param nullValue a value to return if the path exists, but the value is 'null'. This helps
     *                  in distinguishing between a path that doesn't exist vs. a value of 'null'.
     *
     * @return the value associated with the path in the source or 'null' if the path does not exist.
     */
    public Object extractValue(String path, @Nullable Object nullValue) {
        return XContentMapValues.extractValue(path, source(), nullValue);
    }

    public Object filter(FetchSourceContext context) {
        return context.getFilter().apply(source());
    }

    @Override
    public Object get(Object key) {
        return source().get(key);
    }

    @Override
    public int size() {
        return source().size();
    }

    @Override
    public boolean isEmpty() {
        return source().isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return source().containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return source().containsValue(value);
    }

    @Override
    public Set<String> keySet() {
        return source().keySet();
    }

    @Override
    public Collection<Object> values() {
        return source().values();
    }

    @Override
    public Set<Map.Entry<String, Object>> entrySet() {
        return source().entrySet();
    }

    @Override
    public Object put(String key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends String, ? extends Object> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }
}
