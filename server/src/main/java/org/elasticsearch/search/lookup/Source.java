/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.lookup;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The source of a document.  Note that, while objects returned from {@link #source()}
 * and {@link #internalSourceRef()} are immutable, objects implementing this interface
 * may not be immutable themselves.
 */
public interface Source {

    /**
     * The content type of the source, if stored as bytes
     */
    XContentType sourceContentType();

    /**
     * A map representation of the source
     * <p>
     * Important: This can lose precision on numbers with a decimal point. It
     * converts numbers like {@code "n": 1234.567} to a {@code double} which
     * only has 52 bits of precision in the mantissa. This will come up most
     * frequently when folks write nanosecond precision dates as a decimal
     * number.
     */
    Map<String, Object> source();

    /**
     * A byte representation of the source
     */
    BytesReference internalSourceRef();

    /**
     * Apply a filter to this source, returning a new Source
     */
    Source filter(SourceFilter sourceFilter);

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
    default Object extractValue(String path, @Nullable Object nullValue) {
        return XContentMapValues.extractValue(path, source(), nullValue);
    }

    /**
     * An empty Source, represented as an empty map
     */
    static Source empty(XContentType xContentType) {
        return Source.fromMap(Map.of(), xContentType == null ? XContentType.JSON : xContentType);
    }

    /**
     * Build a Source from a bytes representation with an unknown XContentType
     */
    static Source fromBytes(BytesReference bytes) {
        return fromBytes(bytes, null);
    }

    /**
     * Build a Source from a bytes representation with a known XContentType
     */
    @SuppressWarnings("deprecation")
    static Source fromBytes(BytesReference bytes, XContentType type) {
        if (bytes == null || bytes.length() == 0) {
            return empty(type);
        }
        assert type == null || type.xContent() == XContentHelper.xContentType(bytes).xContent()
            : "unexpected type " + type.xContent() + " expecting " + XContentHelper.xContentType(bytes).xContent();
        return new Source() {

            Map<String, Object> asMap = null;
            XContentType xContentType = type;

            private void parseBytes() {
                Tuple<XContentType, Map<String, Object>> t = XContentHelper.convertToMap(bytes, true);
                this.xContentType = t.v1();
                this.asMap = t.v2();
            }

            @Override
            public XContentType sourceContentType() {
                if (xContentType == null) {
                    xContentType = XContentHelper.xContentType(bytes);
                }
                return xContentType;
            }

            @Override
            public Map<String, Object> source() {
                if (asMap == null) {
                    parseBytes();
                }
                return asMap;
            }

            @Override
            public BytesReference internalSourceRef() {
                return bytes;
            }

            @Override
            public Source filter(SourceFilter sourceFilter) {
                // If we've already parsed to a map, then filter using that; but if we can
                // filter without reifying the bytes then that will perform better.
                if (asMap != null) {
                    return sourceFilter.filterMap(this);
                }
                return sourceFilter.filterBytes(this);
            }
        };
    }

    /**
     * Build a Source from a Map representation.
     *
     * Note that {@code null} is accepted as an input and interpreted as an empty map
     *
     * @param map           the java Map to use as a source
     * @param xContentType  the XContentType to serialize this source
     */
    static Source fromMap(Map<String, Object> map, XContentType xContentType) {
        Map<String, Object> sourceMap = map == null ? Map.of() : map;
        return new Source() {
            @Override
            public XContentType sourceContentType() {
                return xContentType;
            }

            @Override
            public Map<String, Object> source() {
                return sourceMap;
            }

            @Override
            public BytesReference internalSourceRef() {
                return mapToBytes(sourceMap, xContentType);
            }

            @Override
            public Source filter(SourceFilter sourceFilter) {
                return sourceFilter.filterMap(this);
            }

            private static BytesReference mapToBytes(Map<String, Object> value, XContentType xContentType) {
                BytesStreamOutput streamOutput = new BytesStreamOutput(1024);
                try {
                    XContentBuilder builder = new XContentBuilder(xContentType.xContent(), streamOutput);
                    builder.value(value);
                    return BytesReference.bytes(builder);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };
    }

    /**
     * Build a source lazily if one of its methods is called
     */
    static Source lazy(Supplier<Source> sourceSupplier) {
        return new Source() {

            Source inner = null;

            @Override
            public XContentType sourceContentType() {
                if (inner == null) {
                    inner = sourceSupplier.get();
                }
                return inner.sourceContentType();
            }

            @Override
            public Map<String, Object> source() {
                if (inner == null) {
                    inner = sourceSupplier.get();
                }
                return inner.source();
            }

            @Override
            public BytesReference internalSourceRef() {
                if (inner == null) {
                    inner = sourceSupplier.get();
                }
                return inner.internalSourceRef();
            }

            @Override
            public Source filter(SourceFilter sourceFilter) {
                if (inner == null) {
                    inner = sourceSupplier.get();
                }
                return inner.filter(sourceFilter);
            }
        };
    }

}
