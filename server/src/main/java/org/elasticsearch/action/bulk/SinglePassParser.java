/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.routing.IndexRouting.ExtractFromSource.ForIndexDimensions;
import org.elasticsearch.cluster.routing.IndexRouting.ExtractFromSource.ForRoutingPath;
import org.elasticsearch.cluster.routing.RoutingHashBuilder;
import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Parses a document's JSON in a single pass, simultaneously extracting routing fields
 * (for shard assignment) and flattening all fields into {@link FieldValue} entries
 * (for columnar batch encoding). This avoids the double-parse that occurs when
 * {@link IndexRouting.ExtractFromSource} routing and {@link DocumentBatchEncoder}
 * each independently parse the same JSON.
 */
final class SinglePassParser {

    private SinglePassParser() {}

    /**
     * Parse the given index request's source in a single pass, producing a {@link FlattenedDoc}
     * that contains both routing information and all flattened field data.
     *
     * @param request       the index request to parse
     * @param efs           the extract-from-source routing strategy
     * @param indexMetadata  the index metadata (used to get dimension fields for ForIndexDimensions)
     * @return a FlattenedDoc with routing hash, optional TSID, and all field values
     */
    static FlattenedDoc parse(IndexRequest request, IndexRouting.ExtractFromSource efs, IndexMetadata indexMetadata) throws IOException {
        RoutingConsumer consumer;
        if (efs instanceof ForRoutingPath frp) {
            consumer = new ForRoutingPathConsumer(frp.builder());
        } else if (efs instanceof ForIndexDimensions) {
            Set<String> dimensionPaths = Set.copyOf(indexMetadata.getTimeSeriesDimensions());
            consumer = new ForIndexDimensionsConsumer(dimensionPaths);
        } else {
            throw new IllegalArgumentException("Unsupported ExtractFromSource type: " + efs.getClass());
        }

        BytesReference source = request.source();
        XContentType xContentType = request.getContentType();
        if (xContentType == null) {
            xContentType = XContentType.JSON;
        }

        List<FieldValue> fields = new ArrayList<>();
        try (XContentParser parser = xContentType.xContent().createParser(XContentParserConfiguration.EMPTY, source.streamInput())) {
            parser.nextToken(); // START_OBJECT
            flattenAndRoute(parser, "", fields, consumer, xContentType);
        }

        return new FlattenedDoc(request, fields, consumer.buildHash(), consumer.tsid());
    }

    /**
     * Recursively flattens a JSON object, collecting field values and notifying the routing consumer.
     */
    private static void flattenAndRoute(
        XContentParser parser,
        String prefix,
        List<FieldValue> fields,
        RoutingConsumer consumer,
        XContentType xContentType
    ) throws IOException {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new IllegalStateException("Expected FIELD_NAME but got " + token);
            }
            String fieldName = parser.currentName();
            String fieldPath = prefix.isEmpty() ? fieldName : prefix + "." + fieldName;

            token = parser.nextToken(); // value token

            switch (token) {
                case START_OBJECT -> {
                    // Recurse into nested object — routing consumer will filter at scalar level
                    flattenAndRoute(parser, fieldPath, fields, consumer, xContentType);
                }
                case START_ARRAY -> {
                    // Capture raw bytes for batch encoding
                    BytesReference rawBytes = captureRawXContent(parser, xContentType);
                    fields.add(FieldValue.ofBinary(fieldPath, rawBytes));
                    // Process array for routing if needed (re-parses the small array payload)
                    consumer.onArray(fieldPath, rawBytes, xContentType);
                }
                case VALUE_STRING -> {
                    XContentString text = parser.optimizedText();
                    fields.add(FieldValue.ofString(fieldPath, text));
                    consumer.onString(fieldPath, text);
                }
                case VALUE_NUMBER -> {
                    XContentParser.NumberType numType = parser.numberType();
                    switch (numType) {
                        case INT -> {
                            int v = parser.intValue();
                            XContentString text = parser.optimizedText();
                            fields.add(FieldValue.ofInt(fieldPath, v));
                            consumer.onInt(fieldPath, v, text);
                        }
                        case LONG -> {
                            long v = parser.longValue();
                            XContentString text = parser.optimizedText();
                            fields.add(FieldValue.ofLong(fieldPath, v));
                            consumer.onLong(fieldPath, v, text);
                        }
                        case FLOAT -> {
                            float v = parser.floatValue();
                            XContentString text = parser.optimizedText();
                            fields.add(FieldValue.ofFloat(fieldPath, v));
                            consumer.onFloat(fieldPath, v, text);
                        }
                        case DOUBLE -> {
                            double v = parser.doubleValue();
                            XContentString text = parser.optimizedText();
                            fields.add(FieldValue.ofDouble(fieldPath, v));
                            consumer.onDouble(fieldPath, v, text);
                        }
                        default -> {
                            // BIG_INTEGER, BIG_DECIMAL -> store as string
                            XContentString text = parser.optimizedText();
                            fields.add(FieldValue.ofString(fieldPath, text));
                            consumer.onString(fieldPath, text);
                        }
                    }
                }
                case VALUE_BOOLEAN -> {
                    boolean v = parser.booleanValue();
                    XContentString text = parser.optimizedText();
                    fields.add(FieldValue.ofBoolean(fieldPath, v));
                    consumer.onBoolean(fieldPath, v, text);
                }
                case VALUE_NULL -> {
                    fields.add(FieldValue.ofNull(fieldPath));
                    // Routing consumers ignore nulls (matching existing behavior)
                }
                default -> throw new IllegalStateException("Unexpected token: " + token);
            }
        }
    }

    private static BytesReference captureRawXContent(XContentParser parser, XContentType xContentType) throws IOException {
        try (var builder = XContentFactory.jsonBuilder()) {
            builder.copyCurrentStructure(parser);
            return BytesReference.bytes(builder);
        }
    }

    // ----- Routing consumer abstraction -----

    /**
     * Strategy interface for processing routing-relevant data during single-pass parsing.
     */
    interface RoutingConsumer {
        void onString(String fieldPath, XContentString text) throws IOException;

        void onInt(String fieldPath, int value, XContentString text) throws IOException;

        void onLong(String fieldPath, long value, XContentString text) throws IOException;

        void onFloat(String fieldPath, float value, XContentString text) throws IOException;

        void onDouble(String fieldPath, double value, XContentString text) throws IOException;

        void onBoolean(String fieldPath, boolean value, XContentString text) throws IOException;

        void onArray(String fieldPath, BytesReference rawBytes, XContentType xContentType) throws IOException;

        int buildHash();

        @Nullable
        BytesRef tsid();
    }

    // ----- ForRoutingPath consumer -----

    /**
     * Routing consumer for {@link ForRoutingPath}. Delegates to {@link RoutingHashBuilder},
     * using {@code addMatching} which internally checks the routing path predicate.
     */
    static final class ForRoutingPathConsumer implements RoutingConsumer {
        private final RoutingHashBuilder builder;

        ForRoutingPathConsumer(RoutingHashBuilder builder) {
            this.builder = builder;
        }

        private void addMatchingText(String fieldPath, XContentString text) {
            XContentString.UTF8Bytes utf8 = text.bytes();
            builder.addMatching(fieldPath, new BytesRef(utf8.bytes(), utf8.offset(), utf8.length()));
        }

        @Override
        public void onString(String fieldPath, XContentString text) {
            addMatchingText(fieldPath, text);
        }

        @Override
        public void onInt(String fieldPath, int value, XContentString text) {
            addMatchingText(fieldPath, text);
        }

        @Override
        public void onLong(String fieldPath, long value, XContentString text) {
            addMatchingText(fieldPath, text);
        }

        @Override
        public void onFloat(String fieldPath, float value, XContentString text) {
            addMatchingText(fieldPath, text);
        }

        @Override
        public void onDouble(String fieldPath, double value, XContentString text) {
            addMatchingText(fieldPath, text);
        }

        @Override
        public void onBoolean(String fieldPath, boolean value, XContentString text) {
            addMatchingText(fieldPath, text);
        }

        @Override
        public void onArray(String fieldPath, BytesReference rawBytes, XContentType xContentType) throws IOException {
            // Re-parse the captured array bytes to extract routing hashes for matching fields.
            // RoutingHashBuilder.addMatching filters internally, so we process all elements.
            try (
                XContentParser arrayParser = XContentType.JSON.xContent()
                    .createParser(XContentParserConfiguration.EMPTY, rawBytes.streamInput())
            ) {
                arrayParser.nextToken(); // START_ARRAY
                walkArrayForRoutingPath(fieldPath, arrayParser);
            }
        }

        private void walkArrayForRoutingPath(String fieldPath, XContentParser parser) throws IOException {
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                switch (token) {
                    case VALUE_STRING, VALUE_NUMBER, VALUE_BOOLEAN -> {
                        XContentString.UTF8Bytes utf8 = parser.optimizedText().bytes();
                        builder.addMatching(fieldPath, new BytesRef(utf8.bytes(), utf8.offset(), utf8.length()));
                    }
                    case START_OBJECT -> {
                        parser.nextToken();
                        walkObjectForRoutingPath(fieldPath, parser);
                    }
                    case START_ARRAY -> walkArrayForRoutingPath(fieldPath, parser);
                    case VALUE_NULL -> {
                    }
                    default -> throw new ParsingException(parser.getTokenLocation(), "Unexpected token in array for routing: [{}]", token);
                }
            }
        }

        private void walkObjectForRoutingPath(String parentPath, XContentParser parser) throws IOException {
            while (parser.currentToken() != XContentParser.Token.END_OBJECT) {
                if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
                    throw new IllegalStateException("Expected FIELD_NAME but got " + parser.currentToken());
                }
                String fieldName = parser.currentName();
                String subPath = parentPath == null ? fieldName : parentPath + "." + fieldName;
                XContentParser.Token token = parser.nextToken();
                switch (token) {
                    case VALUE_STRING, VALUE_NUMBER, VALUE_BOOLEAN -> {
                        XContentString.UTF8Bytes utf8 = parser.optimizedText().bytes();
                        builder.addMatching(subPath, new BytesRef(utf8.bytes(), utf8.offset(), utf8.length()));
                        parser.nextToken();
                    }
                    case START_OBJECT -> {
                        parser.nextToken();
                        walkObjectForRoutingPath(subPath, parser);
                        parser.nextToken();
                    }
                    case START_ARRAY -> {
                        walkArrayForRoutingPath(subPath, parser);
                        parser.nextToken();
                    }
                    case VALUE_NULL -> parser.nextToken();
                    default -> throw new ParsingException(parser.getTokenLocation(), "Unexpected token for routing: [{}]", token);
                }
            }
        }

        @Override
        public int buildHash() {
            return builder.buildHash(
                () -> { throw new IllegalArgumentException("Error extracting routing: source didn't contain any routing fields"); }
            );
        }

        @Override
        @Nullable
        public BytesRef tsid() {
            return null;
        }
    }

    // ----- ForIndexDimensions consumer -----

    /**
     * Routing consumer for {@link ForIndexDimensions}. Delegates to {@link TsidBuilder},
     * adding dimension values only for fields that are in the dimension set.
     * The existing code uses a filtered parser that only exposes dimension fields;
     * in single-pass mode we parse all fields and filter here instead.
     */
    static final class ForIndexDimensionsConsumer implements RoutingConsumer {
        private final TsidBuilder tsidBuilder = new TsidBuilder();
        private final Set<String> dimensionPaths;
        private BytesRef builtTsid;

        ForIndexDimensionsConsumer(Set<String> dimensionPaths) {
            this.dimensionPaths = dimensionPaths;
        }

        private boolean isDimensionField(String fieldPath) {
            if (dimensionPaths.contains(fieldPath)) {
                return true;
            }
            // Check if fieldPath is a descendant of a dimension path (for recursive matching)
            for (String dim : dimensionPaths) {
                if (fieldPath.startsWith(dim + ".")) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void onString(String fieldPath, XContentString text) {
            if (isDimensionField(fieldPath)) {
                tsidBuilder.addStringDimension(fieldPath, text.bytes());
            }
        }

        @Override
        public void onInt(String fieldPath, int value, XContentString text) {
            if (isDimensionField(fieldPath)) {
                tsidBuilder.addIntDimension(fieldPath, value);
            }
        }

        @Override
        public void onLong(String fieldPath, long value, XContentString text) {
            if (isDimensionField(fieldPath)) {
                tsidBuilder.addLongDimension(fieldPath, value);
            }
        }

        @Override
        public void onFloat(String fieldPath, float value, XContentString text) {
            if (isDimensionField(fieldPath)) {
                tsidBuilder.addDoubleDimension(fieldPath, value);
            }
        }

        @Override
        public void onDouble(String fieldPath, double value, XContentString text) {
            if (isDimensionField(fieldPath)) {
                tsidBuilder.addDoubleDimension(fieldPath, value);
            }
        }

        @Override
        public void onBoolean(String fieldPath, boolean value, XContentString text) {
            if (isDimensionField(fieldPath)) {
                tsidBuilder.addBooleanDimension(fieldPath, value);
            }
        }

        @Override
        public void onArray(String fieldPath, BytesReference rawBytes, XContentType xContentType) throws IOException {
            if (isDimensionField(fieldPath) == false) {
                return;
            }
            // Re-parse the captured array bytes to extract dimension values.
            try (
                XContentParser arrayParser = XContentType.JSON.xContent()
                    .createParser(XContentParserConfiguration.EMPTY, rawBytes.streamInput())
            ) {
                arrayParser.nextToken(); // START_ARRAY
                walkArrayForDimensions(fieldPath, arrayParser);
            }
        }

        private void walkArrayForDimensions(String fieldPath, XContentParser parser) throws IOException {
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                switch (token) {
                    case VALUE_NUMBER -> {
                        switch (parser.numberType()) {
                            case INT -> tsidBuilder.addIntDimension(fieldPath, parser.intValue());
                            case LONG -> tsidBuilder.addLongDimension(fieldPath, parser.longValue());
                            case FLOAT -> tsidBuilder.addDoubleDimension(fieldPath, parser.floatValue());
                            case DOUBLE -> tsidBuilder.addDoubleDimension(fieldPath, parser.doubleValue());
                            case BIG_DECIMAL, BIG_INTEGER -> tsidBuilder.addStringDimension(fieldPath, parser.optimizedText().bytes());
                        }
                    }
                    case VALUE_BOOLEAN -> tsidBuilder.addBooleanDimension(fieldPath, parser.booleanValue());
                    case VALUE_STRING -> tsidBuilder.addStringDimension(fieldPath, parser.optimizedText().bytes());
                    case START_OBJECT -> {
                        parser.nextToken();
                        walkObjectForDimensions(fieldPath, parser);
                    }
                    case START_ARRAY -> walkArrayForDimensions(fieldPath, parser);
                    case VALUE_NULL -> {
                    }
                    default -> throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unexpected token in array for dimensions: [{}]",
                        token
                    );
                }
            }
        }

        private void walkObjectForDimensions(String parentPath, XContentParser parser) throws IOException {
            while (parser.currentToken() != XContentParser.Token.END_OBJECT) {
                if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
                    throw new IllegalStateException("Expected FIELD_NAME but got " + parser.currentToken());
                }
                String fieldName = parser.currentName();
                String subPath = parentPath == null ? fieldName : parentPath + "." + fieldName;
                XContentParser.Token token = parser.nextToken();
                switch (token) {
                    case VALUE_NUMBER -> {
                        switch (parser.numberType()) {
                            case INT -> tsidBuilder.addIntDimension(subPath, parser.intValue());
                            case LONG -> tsidBuilder.addLongDimension(subPath, parser.longValue());
                            case FLOAT -> tsidBuilder.addDoubleDimension(subPath, parser.floatValue());
                            case DOUBLE -> tsidBuilder.addDoubleDimension(subPath, parser.doubleValue());
                            case BIG_DECIMAL, BIG_INTEGER -> tsidBuilder.addStringDimension(subPath, parser.optimizedText().bytes());
                        }
                        parser.nextToken();
                    }
                    case VALUE_BOOLEAN -> {
                        tsidBuilder.addBooleanDimension(subPath, parser.booleanValue());
                        parser.nextToken();
                    }
                    case VALUE_STRING -> {
                        tsidBuilder.addStringDimension(subPath, parser.optimizedText().bytes());
                        parser.nextToken();
                    }
                    case START_OBJECT -> {
                        parser.nextToken();
                        walkObjectForDimensions(subPath, parser);
                        parser.nextToken();
                    }
                    case START_ARRAY -> {
                        walkArrayForDimensions(subPath, parser);
                        parser.nextToken();
                    }
                    case VALUE_NULL -> parser.nextToken();
                    default -> throw new ParsingException(parser.getTokenLocation(), "Unexpected token for dimensions: [{}]", token);
                }
            }
        }

        @Override
        public int buildHash() {
            builtTsid = tsidBuilder.buildTsid();
            return IndexRouting.ExtractFromSource.hash(builtTsid);
        }

        @Override
        @Nullable
        public BytesRef tsid() {
            return builtTsid;
        }
    }
}
