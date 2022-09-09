/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.lookup;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.MemoizedSupplier;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class SourceLookup implements Map<String, Object> {
    private SourceProvider sourceProvider;

    private int docId = -1;

    public SourceLookup(SourceProvider sourceProvider) {
        this.sourceProvider = sourceProvider;
    }

    public XContentType sourceContentType() {
        return sourceProvider.sourceContentType();
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
        return sourceProvider.source();
    }

    public void setSourceProvider(SourceProvider sourceProvider) {
        this.sourceProvider = sourceProvider;
    }

    public void setSegmentAndDocument(LeafReaderContext context, int docId) {
        sourceProvider.setSegmentAndDocument(context, docId);
        this.docId = docId;
    }

    /**
     * Internal source representation, might be compressed....
     */
    public BytesReference internalSourceRef() {
        return sourceProvider.sourceAsBytes();
    }

    /**
     * Checks if the source has been deserialized as a {@link Map} of java objects.
     */
    public boolean hasSourceAsMap() {
        return sourceProvider.hasSourceAsMap();
    }

    /**
     * Returns the values associated with the path. Those are "low" level values, and it can
     * handle path expression where an array/list is navigated within.
     *
     * This method will:
     *
     *  - not cache source if it's not already parsed
     *  - will only extract the desired values from the compressed source instead of deserializing the whole object
     *
     * This is useful when the caller only wants a single value from source and does not care of source is fully parsed and cached
     * for later use.
     * @param path The path from which to extract the values from source
     * @return The list of found values or an empty list if none are found
     */
    public List<Object> extractRawValuesWithoutCaching(String path) {
        return sourceProvider.extractRawValuesWithoutCaching(path);
    }

    /**
     * For the provided path, return its value in the source.
     *
     * Both array and object values can be returned.
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
    @SuppressWarnings("rawtypes")
    public void putAll(Map m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    /**
     * SourceProvider describes how SourceLookup will access the source.
     */
    public interface SourceProvider {
        Map<String, Object> source();

        XContentType sourceContentType();

        BytesReference sourceAsBytes();

        List<Object> extractRawValuesWithoutCaching(String path);

        boolean hasSourceAsMap();

        void setSegmentAndDocument(LeafReaderContext context, int docId);
    }

    /**
     * Null source provider when we expect to be given a real source provider in the future.
     * Exceptions expected to be primarily seen by scripts attempting to access synthetic source.
     */
    public static class NullSourceProvider implements SourceProvider {
        @Override
        public Map<String, Object> source() {
            throw new IllegalArgumentException("no source available");
        }

        @Override
        public XContentType sourceContentType() {
            throw new IllegalArgumentException("no source available");
        }

        @Override
        public BytesReference sourceAsBytes() {
            throw new IllegalArgumentException("no source available");
        }

        @Override
        public List<Object> extractRawValuesWithoutCaching(String path) {
            throw new IllegalArgumentException("no source available");
        }

        @Override
        public boolean hasSourceAsMap() {
            return false;
        }

        @Override
        public void setSegmentAndDocument(LeafReaderContext context, int docId) {
            //
        }
    }

    /**
     * Provider for source using a given map and optional content type.
     */
    public static class MapSourceProvider implements SourceProvider {
        private Map<String, Object> source;
        private XContentType sourceContentType;

        public MapSourceProvider(Map<String, Object> source) {
            this.source = source;
        }

        public MapSourceProvider(Map<String, Object> source, @Nullable XContentType sourceContentType) {
            this.source = source;
            this.sourceContentType = sourceContentType;
        }

        @Override
        public Map<String, Object> source() {
            return source;
        }

        @Override
        public XContentType sourceContentType() {
            return sourceContentType;
        }

        @Override
        public BytesReference sourceAsBytes() {
            throw new UnsupportedOperationException("source as bytes unavailable - empty or already parsed to map");
        }

        @Override
        public List<Object> extractRawValuesWithoutCaching(String path) {
            return XContentMapValues.extractRawValues(path, source);
        }

        @Override
        public boolean hasSourceAsMap() {
            return true;
        }

        @Override
        public void setSegmentAndDocument(LeafReaderContext context, int docId) {
            //
        }
    }

    /**
     * Provider for source using given source bytes.
     */
    public static class BytesSourceProvider implements SourceProvider {
        private BytesReference sourceAsBytes;
        private Map<String, Object> source;
        private XContentType sourceContentType;

        public BytesSourceProvider(BytesReference sourceAsBytes) {
            this.sourceAsBytes = sourceAsBytes;
        }

        void parseSource() {
            Tuple<XContentType, Map<String, Object>> tuple = sourceAsMapAndType(sourceAsBytes);
            this.source = tuple.v2();
            this.sourceContentType = tuple.v1();
        }

        @Override
        public Map<String, Object> source() {
            if (source == null) {
                parseSource();
            }
            return source;
        }

        @Override
        public XContentType sourceContentType() {
            if (source == null) {
                parseSource();
            }
            return sourceContentType;
        }

        @Override
        public BytesReference sourceAsBytes() {
            return sourceAsBytes;
        }

        @Override
        public List<Object> extractRawValuesWithoutCaching(String path) {
            if (source != null) {
                return XContentMapValues.extractRawValues(path, source);
            }
            return XContentMapValues.extractRawValues(
                path,
                XContentHelper.convertToMap(sourceAsBytes, false, null, Set.of(path), null).v2()
            );
        }

        @Override
        public boolean hasSourceAsMap() {
            return source != null;
        }

        @Override
        public void setSegmentAndDocument(LeafReaderContext context, int docId) {
            //
        }
    }

    /**
     * Provider for source using a fields visitor.
     */
    public static class ReaderSourceProvider implements SourceProvider {
        private LeafReader reader;
        private CheckedBiConsumer<Integer, FieldsVisitor, IOException> fieldReader;
        private int docId = -1;

        private SourceProvider sourceProvider;

        public void setSegmentAndDocument(LeafReaderContext context, int docId) {
            // if we are called with the same document, don't invalidate source
            if (this.reader == context.reader() && this.docId == docId) {
                return;
            }

            // only reset reader and fieldReader when reader changes
            if (this.reader != context.reader()) {
                this.reader = context.reader();

                // All the docs to fetch are adjacent but Lucene stored fields are optimized
                // for random access and don't optimize for sequential access - except for merging.
                // So we do a little hack here and pretend we're going to do merges in order to
                // get better sequential access.
                if (context.reader()instanceof SequentialStoredFieldsLeafReader lf) {
                    // Avoid eagerly loading the stored fields reader, since this can be expensive
                    Supplier<StoredFieldsReader> supplier = new MemoizedSupplier<>(lf::getSequentialStoredFieldsReader);
                    fieldReader = (d, v) -> supplier.get().visitDocument(d, v);
                } else {
                    fieldReader = context.reader()::document;
                }
            }
            this.sourceProvider = null;
            this.docId = docId;
        }

        @Override
        public Map<String, Object> source() {
            if (sourceProvider == null) {
                readSourceBytes();
            }
            return sourceProvider.source();
        }

        private void readSourceBytes() {
            try {
                FieldsVisitor sourceFieldVisitor = new FieldsVisitor(true);
                fieldReader.accept(docId, sourceFieldVisitor);
                BytesReference sourceAsBytes = sourceFieldVisitor.source();
                if (sourceAsBytes == null) {
                    sourceProvider = new MapSourceProvider(Collections.emptyMap());
                } else {
                    sourceProvider = new BytesSourceProvider(sourceAsBytes);
                    ((BytesSourceProvider) sourceProvider).parseSource();
                }
            } catch (Exception e) {
                throw new ElasticsearchParseException("failed to parse / load source", e);
            }
        }

        @Override
        public XContentType sourceContentType() {
            if (sourceProvider == null) {
                readSourceBytes();
            }
            return sourceProvider.sourceContentType();
        }

        @Override
        public BytesReference sourceAsBytes() {
            if (sourceProvider == null) {
                readSourceBytes();
            }
            return sourceProvider.sourceAsBytes();
        }

        @Override
        public List<Object> extractRawValuesWithoutCaching(String path) {
            if (sourceProvider == null) {
                readSourceBytes();
            }
            return sourceProvider.extractRawValuesWithoutCaching(path);
        }

        @Override
        public boolean hasSourceAsMap() {
            return sourceProvider != null && sourceProvider.hasSourceAsMap();
        }
    }
}
