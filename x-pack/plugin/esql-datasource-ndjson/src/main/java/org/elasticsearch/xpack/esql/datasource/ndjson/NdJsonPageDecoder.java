/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ConstantNullBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NdJsonPageDecoder implements Closeable {

    private static final Logger LOGGER = LogManager.getLogger(NdJsonPageDecoder.class);

    private InputStream input;
    private final BlockDecoder decoder;
    private final int batchSize;
    private final BlockFactory blockFactory;
    private JsonParser parser;
    private final List<Attribute> projectedAttributes;

    // What blocks got a value on the current line? Needed because Block.Builder doesn't provide
    // the number of positions that were added.
    private final BitSet blockTracker;

    NdJsonPageDecoder(
        InputStream input,
        List<Attribute> attributes,
        List<String> projectedColumns,
        int batchSize,
        BlockFactory blockFactory
    ) throws IOException {
        this.input = input;

        var projectedAttributes = attributes;
        if (projectedColumns.isEmpty() == false) {
            // Keep projected columns in order, adding NULL for missing columns
            projectedAttributes = projectedColumns.stream()
                .map(
                    col -> attributes.stream()
                        .filter(a -> a.name().equals(col))
                        .findFirst()
                        .orElseGet(() -> NdJsonSchemaInferrer.attribute(col, DataType.NULL, false))
                )
                .toList();
        }

        this.decoder = prepareSchema(projectedAttributes);
        this.batchSize = batchSize;
        this.blockFactory = blockFactory;
        this.projectedAttributes = projectedAttributes;
        this.blockTracker = new BitSet(projectedAttributes.size());

        this.parser = NdJsonUtils.JSON_FACTORY.createParser(input);
    }

    Page decodePage() throws IOException {
        var blockBuilders = new Block.Builder[this.projectedAttributes.size()];
        // Setting up builders may trip the circuit breaker. Make sure they're all always closed
        try {
            this.decoder.setupBuilders(blockBuilders);

            int lineCount = 0;
            while (lineCount < batchSize) {
                try {
                    if (parser.nextToken() == null) {
                        break; // End of stream
                    }
                } catch (JsonParseException e) {
                    LOGGER.warn("Malformed NDJSON at line {}: {}", lineCount, e);
                    this.input = NdJsonUtils.moveToNextLine(parser, this.input);
                    parser = NdJsonUtils.JSON_FACTORY.createParser(this.input);
                    continue;
                }

                lineCount++;
                this.blockTracker.clear();

                try {
                    decoder.decodeObject(parser);
                } catch (JsonParseException e) {
                    LOGGER.warn("Malformed NDJSON at line {}: {}", lineCount, e);
                    this.input = NdJsonUtils.moveToNextLine(parser, this.input);
                    parser = NdJsonUtils.JSON_FACTORY.createParser(this.input);
                }

                // Make sure every block got moved forward
                for (int i = 0; i < blockBuilders.length; i++) {
                    if (blockTracker.get(i) == false) {
                        blockBuilders[i].appendNull();
                    }
                }
            }

            if (lineCount == 0) {
                // Done
                return null;
            }

            var blocks = new Block[this.projectedAttributes.size()];
            var success = false;
            try {
                for (int i = 0; i < blockBuilders.length; i++) {
                    blocks[i] = blockBuilders[i].build();
                }
                success = true;
            } finally {
                if (success == false) {
                    // Some blocks may have been created
                    Releasables.close(blocks);
                }
            }
            return new Page(blocks);

        } finally {
            Releasables.close(blockBuilders);
        }
    }

    // Prepare the tree of property decoders and return the root decoder.
    private BlockDecoder prepareSchema(List<Attribute> attributes) {
        BlockDecoder root = new BlockDecoder();
        int idx = 0;
        for (var attribute : attributes) {
            var decoder = root;
            var path = attribute.name().split("\\.");
            for (var part : path) {
                if (decoder.children == null) {
                    decoder.children = new HashMap<>();
                }
                decoder = decoder.children.computeIfAbsent(part, k -> new BlockDecoder());
            }
            decoder.setAttribute(attribute, idx);
            idx++;
        }
        return root;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(input);
        input = null;
    }

    // ---------------------------------------------------------------------------------------------
    // A tree of decoders. Avoids path reconstruction when traversing nested objects.
    private class BlockDecoder {
        @Nullable
        Attribute attribute;
        Block.Builder blockBuilder;
        int blockIdx;
        Map<String, BlockDecoder> children;

        void setAttribute(Attribute attribute, int blockIdx) {
            this.attribute = attribute;
            this.blockIdx = blockIdx;
        }

        // Builders setup independently as we need to create new ones for each page.
        void setupBuilders(Block.Builder[] blockBuilders) {
            if (attribute != null) {
                blockBuilder = switch (attribute.dataType()) {
                    // Keep in sync with NdJsonSchemaInferrer.inferValueSchema
                    case BOOLEAN -> blockFactory.newBooleanBlockBuilder(batchSize);
                    case NULL -> new ConstantNullBlock.Builder(blockFactory);
                    case INTEGER -> blockFactory.newIntBlockBuilder(batchSize);
                    case LONG -> blockFactory.newLongBlockBuilder(batchSize);
                    case DOUBLE -> blockFactory.newDoubleBlockBuilder(batchSize);
                    case KEYWORD -> blockFactory.newBytesRefBlockBuilder(batchSize);
                    case DATETIME -> blockFactory.newLongBlockBuilder(batchSize); // milliseconds since epoch
                    default -> throw new IllegalArgumentException("Unsupported data type: " + attribute.dataType());
                };
                blockBuilders[blockIdx] = blockBuilder;
            }

            if (children != null) {
                for (var child : children.values()) {
                    child.setupBuilders(blockBuilders);
                }
            }
        }

        private void decodeObject(JsonParser parser) throws IOException {
            JsonToken token = parser.currentToken();
            if (token != JsonToken.START_OBJECT) {
                throw new NdJsonParseException(parser, "Expected JSON object");
            }
            while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
                if (token != JsonToken.FIELD_NAME) {
                    throw new NdJsonParseException(parser, "Expected field name in object");
                }
                var childDecoder = this.children == null ? null : this.children.get(parser.currentName());
                parser.nextToken();
                if (childDecoder == null) {
                    // Unknown field, skip it
                    parser.skipChildren();
                } else {
                    childDecoder.decodeValue(parser);
                }
            }
        }

        private void decodeValue(JsonParser parser) throws IOException {
            JsonToken token = parser.currentToken();
            blockTracker.set(blockIdx);
            if (token == JsonToken.START_ARRAY) {
                this.blockBuilder.beginPositionEntry();
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    decodeValue(parser);
                }
                this.blockBuilder.endPositionEntry();
                return;
            }

            if (token == JsonToken.VALUE_NULL) {
                blockBuilder.appendNull();
                return;
            }

            switch (attribute.dataType()) {
                case BOOLEAN -> {
                    if (token == JsonToken.VALUE_TRUE) {
                        ((BooleanBlock.Builder) blockBuilder).appendBoolean(true);
                    } else if (token == JsonToken.VALUE_FALSE) {
                        ((BooleanBlock.Builder) blockBuilder).appendBoolean(false);
                    } else {
                        unexpectedValue(parser);
                    }
                }
                case NULL -> {
                    // NULL handled above
                    unexpectedValue(parser);
                }
                case INTEGER -> {
                    if (token == JsonToken.VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT) {
                        ((IntBlock.Builder) blockBuilder).appendInt(parser.getIntValue());
                    } else {
                        unexpectedValue(parser);
                    }
                }
                case LONG -> {
                    if (token == JsonToken.VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT) {
                        ((LongBlock.Builder) blockBuilder).appendLong(parser.getLongValue());
                    } else {
                        unexpectedValue(parser);
                    }
                }
                case DOUBLE -> {
                    if (token == JsonToken.VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT) {
                        ((DoubleBlock.Builder) blockBuilder).appendDouble(parser.getDoubleValue());
                    } else {
                        unexpectedValue(parser);
                    }
                }
                case DATETIME -> {
                    try {
                        var millis = Instant.parse(parser.getValueAsString()).toEpochMilli();
                        ((LongBlock.Builder) blockBuilder).appendLong(millis);
                    } catch (Exception e) {
                        unexpectedValue(parser);
                    }
                }
                case KEYWORD -> {
                    // Be lenient, this is a catch-all type
                    var str = parser.getValueAsString();
                    if (str != null) {
                        ((BytesRefBlock.Builder) blockBuilder).appendBytesRef(new BytesRef(str));
                    } else {
                        unexpectedValue(parser);
                    }
                }
                default -> throw new IllegalArgumentException("Unsupported data type: " + attribute.dataType());
            }
        }

        private void unexpectedValue(JsonParser parser) throws IOException {
            LOGGER.warn(
                "Unexpected token type: {} for attribute: {} at {}",
                parser.currentToken(),
                attribute.name(),
                parser.getTokenLocation()
            );
            // Ignore any children to keep reading other values
            parser.skipChildren();
        }
    }
}
