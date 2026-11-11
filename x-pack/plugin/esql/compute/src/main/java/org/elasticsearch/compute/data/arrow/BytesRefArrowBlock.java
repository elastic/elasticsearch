/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Utf8Sanitizer;

/**
 * Converts Arrow VARCHAR/VARBINARY vectors to ESQL BytesRefBlocks.
 * Flat vectors delegate to zero-copy {@link BytesRefArrowBufBlock}.
 * {@link ListVector} (multi-valued) inputs are converted by copying values into block builders,
 * because {@link BytesRefArrowBufBlock#expand()} does not handle the variable-width value offsets
 * needed by downstream operators like MvExpand.
 *
 * See {@link ArrowListSupport} for the Arrow list -> ESQL multi-value mapping rules
 * (null lists, empty lists, and null children are all collapsed to an ESQL null
 * position; mixed lists drop their null elements).
 */
public final class BytesRefArrowBlock {

    private BytesRefArrowBlock() {}

    public static Block of(ValueVector vector, BlockFactory blockFactory) {
        if (vector instanceof ListVector listVector) {
            return ofList(listVector, blockFactory);
        }
        return BytesRefArrowBufBlock.of(vector, blockFactory);
    }

    /**
     * Converts an Arrow {@code VARCHAR} vector to a KEYWORD {@link BytesRefBlock}, repairing malformed
     * UTF-8 to {@code U+FFFD} so downstream {@code KEYWORD} operations (e.g. the TopN Utf8 encoders)
     * stay total. Arrow {@code VARCHAR} is nominally UTF-8, but external producers (e.g. Spark) can
     * still emit ill-formed bytes.
     * <p>
     * The common, well-formed case is validated in a single pass and then returned as the zero-copy
     * {@link BytesRefArrowBufBlock}; only when a malformed value is found does the block get rebuilt
     * with sanitized values. UTF-8 well-formedness is checked per value because a multi-byte sequence
     * can never span two logical Arrow values.
     */
    public static Block ofSanitizedUtf8(ValueVector vector, BlockFactory blockFactory) {
        if (vector instanceof ListVector listVector) {
            // Lists always go through the copying builder path (like ofList): it is the canonical
            // ArrowListSupport multi-value mapping and lets each emitted value be sanitized.
            return ofSanitizedList(listVector, blockFactory);
        }
        if (vector instanceof BaseVariableWidthVector base && allWellFormed(base) == false) {
            return ofSanitizedFlat(base, blockFactory);
        }
        return BytesRefArrowBufBlock.of(vector, blockFactory);
    }

    private static boolean allWellFormed(BaseVariableWidthVector base) {
        int valueCount = base.getValueCount();
        ArrowBuf offsets = base.getOffsetBuffer();
        ArrowBuf data = base.getDataBuffer();
        byte[] scratch = null;
        for (int i = 0; i < valueCount; i++) {
            if (base.isNull(i)) {
                continue;
            }
            int start = offsets.getInt((long) i * Integer.BYTES);
            int end = offsets.getInt((long) (i + 1) * Integer.BYTES);
            int len = end - start;
            if (len == 0) {
                continue;
            }
            if (scratch == null || scratch.length < len) {
                scratch = new byte[len];
            }
            data.getBytes(start, scratch, 0, len);
            if (Utf8Sanitizer.isWellFormed(scratch, 0, len) == false) {
                return false;
            }
        }
        return true;
    }

    private static Block ofSanitizedFlat(BaseVariableWidthVector base, BlockFactory blockFactory) {
        int valueCount = base.getValueCount();
        ArrowBuf offsets = base.getOffsetBuffer();
        ArrowBuf data = base.getDataBuffer();
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(valueCount)) {
            for (int i = 0; i < valueCount; i++) {
                if (base.isNull(i)) {
                    builder.appendNull();
                    continue;
                }
                int start = offsets.getInt((long) i * Integer.BYTES);
                int end = offsets.getInt((long) (i + 1) * Integer.BYTES);
                builder.appendBytesRef(Utf8Sanitizer.sanitize(copyRange(data, start, end)));
            }
            return builder.build();
        }
    }

    /**
     * Sanitizing rebuild of a multi-valued {@code VARCHAR} list. Mirrors {@link #ofList} exactly (the
     * {@link ArrowListSupport} mapping: null/empty lists and all-null children collapse to a null
     * position, mixed lists drop their null children), sanitizing each emitted value to well-formed UTF-8.
     */
    private static Block ofSanitizedList(ListVector listVector, BlockFactory blockFactory) {
        int rowCount = listVector.getValueCount();
        BaseVariableWidthVector child = (BaseVariableWidthVector) listVector.getDataVector();
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(rowCount)) {
            for (int i = 0; i < rowCount; i++) {
                if (listVector.isNull(i)) {
                    builder.appendNull();
                    continue;
                }
                int start = listVector.getElementStartIndex(i);
                int end = listVector.getElementEndIndex(i);
                int nonNullCount = ArrowListSupport.countNonNull(child, start, end);
                if (nonNullCount == 0) {
                    builder.appendNull();
                } else if (nonNullCount == 1) {
                    for (int j = start; j < end; j++) {
                        if (child.isNull(j) == false) {
                            builder.appendBytesRef(Utf8Sanitizer.sanitize(new BytesRef(child.get(j))));
                            break;
                        }
                    }
                } else {
                    builder.beginPositionEntry();
                    for (int j = start; j < end; j++) {
                        if (child.isNull(j) == false) {
                            builder.appendBytesRef(Utf8Sanitizer.sanitize(new BytesRef(child.get(j))));
                        }
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    private static BytesRef copyRange(ArrowBuf data, int start, int end) {
        int len = end - start;
        byte[] bytes = new byte[len];
        if (len > 0) {
            data.getBytes(start, bytes, 0, len);
        }
        return new BytesRef(bytes, 0, len);
    }

    private static Block ofList(ListVector listVector, BlockFactory blockFactory) {
        int rowCount = listVector.getValueCount();
        BaseVariableWidthVector child = (BaseVariableWidthVector) listVector.getDataVector();
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(rowCount)) {
            for (int i = 0; i < rowCount; i++) {
                if (listVector.isNull(i)) {
                    builder.appendNull();
                    continue;
                }
                int start = listVector.getElementStartIndex(i);
                int end = listVector.getElementEndIndex(i);
                int nonNullCount = ArrowListSupport.countNonNull(child, start, end);
                if (nonNullCount == 0) {
                    builder.appendNull();
                } else if (nonNullCount == 1) {
                    for (int j = start; j < end; j++) {
                        if (child.isNull(j) == false) {
                            builder.appendBytesRef(new BytesRef(child.get(j)));
                            break;
                        }
                    }
                } else {
                    boolean hasNulls = nonNullCount != (end - start);
                    builder.beginPositionEntry();
                    if (hasNulls) {
                        for (int j = start; j < end; j++) {
                            if (child.isNull(j) == false) {
                                builder.appendBytesRef(new BytesRef(child.get(j)));
                            }
                        }
                    } else {
                        for (int j = start; j < end; j++) {
                            builder.appendBytesRef(new BytesRef(child.get(j)));
                        }
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }
}
