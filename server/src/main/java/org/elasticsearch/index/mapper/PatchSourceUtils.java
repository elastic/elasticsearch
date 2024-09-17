/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentGenerator;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.util.Set;

public class PatchSourceUtils {
    public interface CheckedTriConsumer<S, T, U> {
        void apply(S s, T t, U u) throws IOException;
    }

    /**
     * Parses the given {@code source} and returns a new version with all values referenced by the
     * provided {@code patchFullPaths} replaced using the {@code patchApply} consumer.
     */
    public static BytesReference patchSource(
        BytesReference source,
        XContent xContent,
        Set<String> patchFullPaths,
        CheckedTriConsumer<String, XContentParser, XContentGenerator> patchApply
    ) throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput();
        XContentBuilder builder = new XContentBuilder(xContent, streamOutput);
        try (XContentParser parser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, source, xContent.type())) {
            if ((parser.currentToken() == null) && (parser.nextToken() == null)) {
                return source;
            }
            parseAndPatchSource(builder.generator(), parser, "", patchFullPaths, patchApply);
            return BytesReference.bytes(builder);
        }
    }

    private static void parseAndPatchSource(
        XContentGenerator destination,
        XContentParser parser,
        String fullPath,
        Set<String> patchFullPaths,
        CheckedTriConsumer<String, XContentParser, XContentGenerator> patchApply
    ) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.FIELD_NAME) {
            String fieldName = parser.currentName();
            destination.writeFieldName(fieldName);
            token = parser.nextToken();
            fullPath = fullPath + (fullPath.isEmpty() ? "" : ".") + fieldName;
            if (patchFullPaths.contains(fullPath)) {
                patchApply.apply(fullPath, parser, destination);
                return;
            }
        }

        switch (token) {
            case START_ARRAY -> {
                destination.writeStartArray();
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    parseAndPatchSource(destination, parser, fullPath, patchFullPaths, patchApply);
                }
                destination.writeEndArray();
            }
            case START_OBJECT -> {
                destination.writeStartObject();
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    parseAndPatchSource(destination, parser, fullPath, patchFullPaths, patchApply);
                }
                destination.writeEndObject();
            }
            default -> // others are simple:
                destination.copyCurrentEvent(parser);
        }
    }
}
