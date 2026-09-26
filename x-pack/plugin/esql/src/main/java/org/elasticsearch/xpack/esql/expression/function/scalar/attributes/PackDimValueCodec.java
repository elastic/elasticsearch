/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.attributes;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Internal JSON leaf codec for packed dimensions. This encoding is not part of the consumer API.
 * Scalar types and array order are preserved; no key lookup, update or absence semantics belong here.
 */
final class PackDimValueCodec {
    private PackDimValueCodec() {}

    static Object decode(BytesRef bytes) throws IOException {
        try (
            var parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, bytes.bytes, bytes.offset, bytes.length)
        ) {
            parser.nextToken();
            return XContentParserUtils.parseFieldsValue(parser);
        }
    }

    static BytesRef encode(Object value) throws IOException {
        try (var builder = XContentFactory.jsonBuilder()) {
            builder.value(canonical(value));
            return BytesReference.bytes(builder).toBytesRef();
        }
    }

    private static Object canonical(Object value) {
        if (value instanceof Map<?, ?> map) {
            var sorted = new TreeMap<String, Object>();
            for (var entry : map.entrySet())
                sorted.put((String) entry.getKey(), canonical(entry.getValue()));
            return sorted;
        }
        if (value instanceof List<?> list) return list.stream().map(PackDimValueCodec::canonical).toList();
        return value;
    }

    /** Convert the engine's generic scalar/list view to JSON values without losing numeric or boolean types. */
    static Object jsonValue(Object value) {
        if (value instanceof BytesRef bytes) return bytes.utf8ToString();
        if (value instanceof List<?> list) return list.stream().map(PackDimValueCodec::jsonValue).toList();
        return value;
    }
}
