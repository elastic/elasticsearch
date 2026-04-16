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
import com.fasterxml.jackson.core.exc.InputCoercionException;
import com.fasterxml.jackson.core.io.JsonEOFException;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.LoggerMessageFormat;
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
import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Parses NDJSON into {@link Page}s for a single input stream.
 * <p>
 * <strong>Not thread-safe:</strong> each instance is intended for use by a single consumer (one
 * {@link NdJsonPageIterator}); do not call {@link #decodePage()} concurrently from multiple threads.
 */
public class NdJsonPageDecoder implements Closeable {

    private static final Logger logger = LogManager.getLogger(NdJsonPageDecoder.class);

    private InputStream input;
    private final BlockDecoder decoder;
    private final int batchSize;
    private final BlockFactory blockFactory;
    private JsonParser parser;
    private final List<Attribute> projectedAttributes;

    // What blocks got a value on the current line? Needed because Block.Builder doesn't provide
    // the number of positions that were added.
    private final BitSet blockTracker;
    private final ErrorPolicy errorPolicy;
    private long totalRowCount;
    private long errorCount;

    /**
     * Lazily allocated for {@link #decodePageLenient} only; reused across rows within this decoder
     * (avoids per-row {@code new Block.Builder[n]}).
     */
    @Nullable
    private Block.Builder[] lenientScratchBuilders;

    /**
     * Reused buffer for {@link #appendDecodedScratchRow}; paired with {@link #lenientScratchBuilders}.
     */
    @Nullable
    private Block[] lenientScratchRowBlocks;

    NdJsonPageDecoder(
        InputStream input,
        List<Attribute> attributes,
        List<String> projectedColumns,
        int batchSize,
        BlockFactory blockFactory,
        ErrorPolicy errorPolicy
    ) throws IOException {
        this.input = input;
        this.errorPolicy = errorPolicy != null ? errorPolicy : ErrorPolicy.STRICT;

        var projectedAttributes = attributes;
        if (projectedColumns != null && projectedColumns.isEmpty() == false) {
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

    private void recoverFromParseException(JsonParser failedParser) throws IOException {
        this.input = NdJsonUtils.moveToNextLine(failedParser, this.input);
        this.parser = NdJsonUtils.JSON_FACTORY.createParser(this.input);
    }

    /**
     * Whole-line JSON failures always drop the line. {@link ErrorPolicy.Mode#NULL_FIELD} is treated
     * like {@link ErrorPolicy.Mode#SKIP_ROW} here; per-field null-fill would require partial decode support.
     */
    private void onNdjsonLineParseError(JsonParseException e, long logicalRowIndex, String phaseLabel) {
        if (errorPolicy.isStrict()) {
            throw new EsqlIllegalArgumentException(e, "Malformed NDJSON [{}]: {}", phaseLabel, e.getOriginalMessage());
        }
        errorCount++;
        if (errorPolicy.isBudgetExceeded(errorCount, totalRowCount)) {
            throw new EsqlIllegalArgumentException(
                "NDJSON error budget exceeded: [{}] errors in [{}] rows, maximum allowed is [{}] errors or [{}] ratio",
                errorCount,
                totalRowCount,
                errorPolicy.maxErrors(),
                errorPolicy.maxErrorRatio()
            );
        }
        logger.log(
            errorPolicy.logErrors() ? Level.INFO : Level.DEBUG,
            LoggerMessageFormat.format(
                "{} NDJSON at logical row [{}] ({}): {}",
                e instanceof JsonEOFException ? "Truncated" : "Malformed",
                logicalRowIndex,
                phaseLabel,
                e.getOriginalMessage()
            )
        );
    }

    Page decodePage() throws IOException {
        var blockBuilders = new Block.Builder[projectedAttributes.size()];
        // Setting up builders may trip the circuit breaker. Make sure they're all always closed
        try {
            decoder.setupBuilders(blockBuilders);
            return errorPolicy.isStrict() ? decodePageFailFast(blockBuilders) : decodePageLenient(blockBuilders);
        } finally {
            Releasables.close(blockBuilders);
        }
    }

    /**
     * {@link ErrorPolicy.Mode#FAIL_FAST}: abort on the first {@link JsonParseException} on a line
     * (no recovery, no scratch-row path).
     */
    private Page decodePageFailFast(Block.Builder[] blockBuilders) throws IOException {
        int lineCount = 0;
        while (lineCount < batchSize) {
            try {
                if (parser.nextToken() == null) {
                    break; // End of stream
                }
            } catch (JsonParseException e) {
                totalRowCount++;
                onNdjsonLineParseError(e, totalRowCount, "nextToken");
            }

            totalRowCount++;
            this.blockTracker.clear();

            try {
                decoder.decodeObject(parser, false);
            } catch (JsonParseException e) {
                onNdjsonLineParseError(e, totalRowCount, "decodeObject");
            }

            lineCount++;
            for (int i = 0; i < blockBuilders.length; i++) {
                if (blockTracker.get(i) == false) {
                    blockBuilders[i].appendNull();
                }
            }
        }
        return buildPageFromBuildersOrNull(blockBuilders, lineCount);
    }

    /**
     * Lenient modes: skip bad lines up to the error budget, using scratch builders so partial rows
     * are never committed to the page.
     */
    private Page decodePageLenient(Block.Builder[] blockBuilders) throws IOException {
        ensureLenientScratchBuffers();
        final Block.Builder[] rowScratch = Objects.requireNonNull(lenientScratchBuilders);

        int lineCount = 0;
        while (lineCount < batchSize) {
            try {
                if (parser.nextToken() == null) {
                    break; // End of stream
                }
            } catch (JsonParseException e) {
                totalRowCount++;
                onNdjsonLineParseError(e, totalRowCount, "nextToken");
                recoverFromParseException(parser);
                continue;
            }

            totalRowCount++;
            this.blockTracker.clear();

            try {
                decoder.setupBuilders(rowScratch);
                try {
                    decoder.decodeObject(parser, false);
                } catch (JsonParseException e) {
                    onNdjsonLineParseError(e, totalRowCount, "decodeObject");
                    recoverFromParseException(parser);
                    continue;
                }
                for (int i = 0; i < rowScratch.length; i++) {
                    if (blockTracker.get(i) == false) {
                        rowScratch[i].appendNull();
                    }
                }
                appendDecodedScratchRow(blockBuilders, rowScratch);
            } finally {
                Releasables.close(rowScratch);
            }

            lineCount++;
        }
        return buildPageFromBuildersOrNull(blockBuilders, lineCount);
    }

    private void ensureLenientScratchBuffers() {
        if (lenientScratchBuilders == null) {
            lenientScratchBuilders = new Block.Builder[projectedAttributes.size()];
            lenientScratchRowBlocks = new Block[projectedAttributes.size()];
        }
    }

    private Page buildPageFromBuildersOrNull(Block.Builder[] blockBuilders, int lineCount) {
        if (lineCount == 0) {
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
                Releasables.close(blocks);
            }
        }
        return new Page(blocks);
    }

    /**
     * Copies one fully decoded logical row from per-line scratch builders into the page builders.
     * Scratch builders are {@link Block.Builder#build() built} and released; callers still close
     * any non-built scratch builders via {@link Releasables#close}.
     */
    private void appendDecodedScratchRow(Block.Builder[] pageBuilders, Block.Builder[] scratchBuilders) {
        final int columns = scratchBuilders.length;
        Block[] rowBlocks = Objects.requireNonNull(lenientScratchRowBlocks);
        try {
            for (int i = 0; i < columns; i++) {
                rowBlocks[i] = scratchBuilders[i].build();
            }
            for (int i = 0; i < columns; i++) {
                pageBuilders[i].copyFrom(rowBlocks[i], 0, 1);
            }
        } finally {
            for (int i = 0; i < columns; i++) {
                Releasables.close(rowBlocks[i]);
                rowBlocks[i] = null;
            }
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
        DataType dataType;
        String name;
        int blockIdx;
        Block.Builder blockBuilder;
        Map<String, BlockDecoder> children;

        void setAttribute(Attribute attribute, int blockIdx) {
            this.dataType = attribute.dataType();
            this.name = attribute.name();
            this.blockIdx = blockIdx;
        }

        // Builders setup independently as we need to create new ones for each page.
        void setupBuilders(Block.Builder[] blockBuilders) {
            if (dataType != null) {
                blockBuilder = switch (dataType) {
                    // Keep in sync with NdJsonSchemaInferrer.inferValueSchema
                    case BOOLEAN -> blockFactory.newBooleanBlockBuilder(batchSize);
                    case NULL -> new ConstantNullBlock.Builder(blockFactory);
                    case INTEGER -> blockFactory.newIntBlockBuilder(batchSize);
                    case LONG -> blockFactory.newLongBlockBuilder(batchSize);
                    case DOUBLE -> blockFactory.newDoubleBlockBuilder(batchSize);
                    case KEYWORD -> blockFactory.newBytesRefBlockBuilder(batchSize);
                    case DATETIME -> blockFactory.newLongBlockBuilder(batchSize); // milliseconds since epoch
                    default -> throw new IllegalArgumentException("Unsupported data type: " + dataType);
                };
                blockBuilders[blockIdx] = blockBuilder;
            }

            if (children != null) {
                for (var child : children.values()) {
                    child.setupBuilders(blockBuilders);
                }
            }
        }

        private void decodeObject(JsonParser parser, boolean inArray) throws IOException {
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
                    childDecoder.decodeValue(parser, inArray);
                }
            }
        }

        /**
         * @param includeChildren when {@code true}, also begins MV entries on child decoders. Use this only for JSON
         *        arrays of objects (e.g. {@code [{"a":1},{"a":2}]}) where every child column shares one MV slot per
         *        element. For arrays of primitives (e.g. {@code salary_change} as doubles while {@code salary_change.int}
         *        is a separate top-level field), {@code false} so children are not opened for values they will never
         *        receive from this array.
         */
        private void beginPositionEntry(boolean includeChildren) {
            // We may have DataType.NULL for unknown columns. And NullBlock.Builder throws on beginPositionEntry()
            if (blockBuilder != null && dataType != DataType.NULL) {
                blockBuilder.beginPositionEntry();
            }
            if (includeChildren && children != null) {
                for (var child : children.values()) {
                    child.beginPositionEntry(includeChildren);
                }
            }
        }

        private void endPositionEntry(boolean includeChildren) {
            if (blockBuilder != null && dataType != DataType.NULL) {
                blockBuilder.endPositionEntry();
            }
            if (includeChildren && children != null) {
                for (var child : children.values()) {
                    child.endPositionEntry(includeChildren);
                }
            }
        }

        /**
         * An empty JSON array {@code []} must not run {@link Block.Builder#beginPositionEntry()} with no values:
         * {@link org.elasticsearch.compute.data.AbstractBlockBuilder#endPositionEntry()} asserts on empty multi-value
         * slots. Treat {@code []} like a missing field for every leaf column under this decoder subtree.
         */
        private void appendNullsForEmptyArray() {
            if (blockBuilder != null) {
                if (dataType != DataType.NULL) {
                    blockTracker.set(blockIdx);
                    blockBuilder.appendNull();
                }
            } else if (children != null) {
                for (var child : children.values()) {
                    child.appendNullsForEmptyArray();
                }
            }
        }

        private void decodeValue(JsonParser parser, boolean inArray) throws IOException {
            JsonToken token = parser.currentToken();

            if (dataType == DataType.NULL) {
                // Don't do anything. We must do a single appendNull() on null blocks, this will be done
                // at the end of decodePage() when we check that all blocks have moved forward.
                parser.skipChildren();
                return;
            }

            if (token == JsonToken.START_ARRAY) {
                // Start a multi-value entry on this decoder and all its children (nested arrays are flattened).
                // Note: the `inArray` flag is needed because blockBuilder.beginPositionEntry() is not idempotent.
                // Calling it twice implicitly calls endPositionEntry().
                if (!inArray) {
                    JsonToken first = parser.nextToken();
                    if (first == JsonToken.END_ARRAY) {
                        appendNullsForEmptyArray();
                        return;
                    }
                    boolean includeChildren = first == JsonToken.START_OBJECT;
                    beginPositionEntry(includeChildren);
                    decodeValue(parser, true);
                    while (parser.nextToken() != JsonToken.END_ARRAY) {
                        decodeValue(parser, true);
                    }
                    endPositionEntry(includeChildren);
                    return;
                }
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    decodeValue(parser, true);
                }
                return;
            }

            if (token == JsonToken.START_OBJECT) {
                decodeObject(parser, inArray);
                return;
            }

            blockTracker.set(blockIdx);
            if (token == JsonToken.VALUE_NULL && inArray == false) {
                // Nulls in arrays aren't supported. Furthermore, appendNull will implicitly call endPositionEntry()
                blockBuilder.appendNull();
                return;
            }

            switch (dataType) {
                case BOOLEAN -> {
                    if (token == JsonToken.VALUE_TRUE) {
                        ((BooleanBlock.Builder) blockBuilder).appendBoolean(true);
                    } else if (token == JsonToken.VALUE_FALSE) {
                        ((BooleanBlock.Builder) blockBuilder).appendBoolean(false);
                    } else {
                        unexpectedValue(blockBuilder, parser, inArray);
                    }
                }
                case NULL -> {
                    // NULL handled above
                    unexpectedValue(blockBuilder, parser, inArray);
                }
                case INTEGER -> {
                    if (token == JsonToken.VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT) {
                        try {
                            ((IntBlock.Builder) blockBuilder).appendInt(parser.getIntValue());
                        } catch (InputCoercionException e) {
                            unexpectedValue(blockBuilder, parser, inArray);
                        }
                    } else {
                        unexpectedValue(blockBuilder, parser, inArray);
                    }
                }
                case LONG -> {
                    if (token == JsonToken.VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT) {
                        try {
                            ((LongBlock.Builder) blockBuilder).appendLong(parser.getLongValue());
                        } catch (InputCoercionException e) {
                            unexpectedValue(blockBuilder, parser, inArray);
                        }
                    } else {
                        unexpectedValue(blockBuilder, parser, inArray);
                    }
                }
                case DOUBLE -> {
                    if (token == JsonToken.VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT) {
                        try {
                            ((DoubleBlock.Builder) blockBuilder).appendDouble(parser.getDoubleValue());
                        } catch (InputCoercionException e) {
                            unexpectedValue(blockBuilder, parser, inArray);
                        }
                    } else {
                        unexpectedValue(blockBuilder, parser, inArray);
                    }
                }
                case DATETIME -> {
                    try {
                        var millis = NdJsonSchemaInferrer.DATE_FORMATTER.parseMillis(parser.getValueAsString());
                        ((LongBlock.Builder) blockBuilder).appendLong(millis);
                    } catch (Exception e) {
                        unexpectedValue(blockBuilder, parser, inArray);
                    }
                }
                case KEYWORD -> {
                    // Be lenient, this is a catch-all type
                    var str = parser.getValueAsString();
                    if (str != null) {
                        ((BytesRefBlock.Builder) blockBuilder).appendBytesRef(new BytesRef(str));
                    } else {
                        unexpectedValue(blockBuilder, parser, inArray);
                    }
                }
                default -> throw new IllegalArgumentException("Unsupported data type: " + dataType);
            }
        }

        private void unexpectedValue(Block.Builder builder, JsonParser parser, boolean inArray) throws IOException {
            // Append a null and log the problem
            if (inArray == false) {
                // See previous comment about nulls and arrays
                builder.appendNull();
            }

            logger.debug("Unexpected token type: {} for attribute: {} at {}", parser.currentToken(), name, parser.getTokenLocation());
            // Ignore any children to keep reading other values
            parser.skipChildren();
        }
    }
}
