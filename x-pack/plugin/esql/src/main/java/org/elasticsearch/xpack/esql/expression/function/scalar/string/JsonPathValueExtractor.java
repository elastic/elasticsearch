/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

/**
 * Navigates an XContent (JSON/SMILE/CBOR/YAML) value using a {@link JsonPath}
 * and appends the result to a {@link BytesRefBlock.Builder}.
 */
final class JsonPathValueExtractor {

    private static final BytesRef TRUE_BYTES = new BytesRef("true");
    private static final BytesRef FALSE_BYTES = new BytesRef("false");

    private JsonPathValueExtractor() {}

    /**
     * Parses {@code str} as XContent, navigates to the value identified by
     * {@code path}, and appends the result to {@code builder}.
     *
     * @throws IllegalArgumentException on empty input, invalid JSON, missing
     *                                  path, or out-of-bounds array index
     */
    static void extract(BytesRefBlock.Builder builder, BytesRef str, JsonPath path) {
        XContentType type = XContentFactory.xContentType(str.bytes, str.offset, str.length);
        if (type == null) {
            type = XContentType.JSON;
        }
        try (XContentParser parser = type.xContent().createParser(XContentParserConfiguration.EMPTY, str.bytes, str.offset, str.length)) {
            if (parser.nextToken() == null) {
                throw new IllegalArgumentException("empty JSON input");
            }
            byte[] rawBytes = type == XContentType.JSON ? str.bytes : null;
            int rawOffset = type == XContentType.JSON ? str.offset : 0;
            extractValue(builder, parser, path.segments(), 0, path.originalPath(), rawBytes, rawOffset);
        } catch (IOException | XContentParseException e) {
            throw new IllegalArgumentException("invalid JSON input");
        }
    }

    private static void extractValue(
        BytesRefBlock.Builder builder,
        XContentParser parser,
        List<JsonPath.Segment> segments,
        int depth,
        String originalPath,
        byte[] rawBytes,
        int rawOffset
    ) throws IOException {
        if (depth == segments.size()) {
            extractCurrentValue(builder, parser, rawBytes, rawOffset);
            return;
        }

        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_OBJECT && segments.get(depth) instanceof JsonPath.Segment.Key key) {
            navigateToField(parser, key, originalPath);
            extractValue(builder, parser, segments, depth + 1, originalPath, rawBytes, rawOffset);
        } else if (token == XContentParser.Token.START_ARRAY && segments.get(depth) instanceof JsonPath.Segment.Index index) {
            navigateToIndex(parser, index);
            extractValue(builder, parser, segments, depth + 1, originalPath, rawBytes, rawOffset);
        } else {
            throw new IllegalArgumentException("path [" + originalPath + "] does not exist");
        }
    }

    private static void navigateToField(XContentParser parser, JsonPath.Segment.Key key, String originalPath) throws IOException {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String fieldName = parser.currentName();
                parser.nextToken();
                if (fieldName.equals(key.name())) {
                    return;
                }
                parser.skipChildren();
            }
        }
        throw new IllegalArgumentException("path [" + originalPath + "] does not exist");
    }

    private static void navigateToIndex(XContentParser parser, JsonPath.Segment.Index index) throws IOException {
        int currentIndex = 0;
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            if (currentIndex == index.index()) {
                return;
            }
            parser.skipChildren();
            currentIndex++;
        }
        throw new IllegalArgumentException("array index out of bounds");
    }

    private static void extractCurrentValue(BytesRefBlock.Builder builder, XContentParser parser, byte[] rawBytes, int rawOffset)
        throws IOException {
        XContentParser.Token token = parser.currentToken();

        switch (token) {
            case VALUE_STRING -> builder.appendBytesRef(new BytesRef(parser.text()));
            case VALUE_NUMBER -> {
                if (rawBytes != null) {
                    XContentLocation tokenLocation = parser.getTokenLocation();
                    XContentLocation currentLocation = parser.getCurrentLocation();
                    if (tokenLocation.hasValidByteOffset() && currentLocation.hasValidByteOffset()) {
                        int start = (int) tokenLocation.byteOffset() + rawOffset;
                        int end = (int) currentLocation.byteOffset() + rawOffset;
                        builder.appendBytesRef(new BytesRef(rawBytes, start, end - start));
                    } else {
                        builder.appendBytesRef(new BytesRef(parser.text()));
                    }
                } else {
                    builder.appendBytesRef(new BytesRef(parser.text()));
                }
            }
            case VALUE_BOOLEAN -> builder.appendBytesRef(parser.booleanValue() ? TRUE_BYTES : FALSE_BYTES);
            case VALUE_NULL -> builder.appendNull();
            case START_OBJECT, START_ARRAY -> {
                if (rawBytes != null) {
                    XContentLocation startLocation = parser.getTokenLocation();
                    parser.skipChildren();
                    XContentLocation endLocation = parser.getCurrentLocation();
                    if (startLocation.hasValidByteOffset() && endLocation.hasValidByteOffset()) {
                        int start = (int) startLocation.byteOffset() + rawOffset;
                        int end = (int) endLocation.byteOffset() + rawOffset;
                        builder.appendBytesRef(new BytesRef(rawBytes, start, end - start));
                    } else {
                        copyCurrentStructureFallback(builder, parser);
                    }
                } else {
                    copyCurrentStructureFallback(builder, parser);
                }
            }
            default -> throw new IllegalArgumentException("unexpected token: " + token);
        }
    }

    private static void copyCurrentStructureFallback(BytesRefBlock.Builder builder, XContentParser parser) throws IOException {
        try (XContentBuilder jsonBuilder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            jsonBuilder.copyCurrentStructure(parser);
            builder.appendBytesRef(BytesReference.bytes(jsonBuilder).toBytesRef());
        }
    }
}
