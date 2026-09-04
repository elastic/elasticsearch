/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.elasticsearch.xcontent.XContentString;

/**
 * A per-leaf callback a {@link SourceBatchEncoder} fires while parsing a document, so the caller can
 * observe primitive leaf values as they are encoded (used for shard-routing extraction). The encoder
 * still encodes every value regardless of what the sink does; the sink may throw to abandon encoding.
 */
public interface LeafSink {

    LeafSink NO_OP = () -> false;

    /**
     * Returns true if this sink wants the parser's UTF-8 text bytes for every primitive leaf
     * (via {@link #onTextPrimitive}), false if it wants typed values (via {@link #onLongPrimitive} /
     * {@link #onDoublePrimitive} / {@link #onBooleanPrimitive}) for numeric and boolean leaves. The
     * encoder reads this once per document.
     */
    boolean passRawText();

    /**
     * Called in raw-text mode ({@link #passRawText()} = {@code true}) for every primitive leaf, and in
     * typed mode for string leaves and unrecognized number types (BIG_DECIMAL / BIG_INTEGER) that the
     * encoder narrows to a string representation.
     *
     * @param columnIndex schema leaf index (stable across documents in this encoder)
     * @param dottedPath cached dotted path for the column
     * @param type the type byte assigned to the value
     * @param textBytes UTF-8 byte slice of the parser token's textual form
     */
    default void onTextPrimitive(int columnIndex, String dottedPath, byte type, XContentString.UTF8Bytes textBytes) {}

    /**
     * Called in typed mode ({@link #passRawText()} = {@code false}) for {@code INT} and {@code LONG}
     * primitives. The encoder narrows numerics that fit in the int range to the int type; subclasses
     * can dispatch further on {@code type}.
     */
    default void onLongPrimitive(int columnIndex, String dottedPath, byte type, long value) {}

    /**
     * Called in typed mode for {@code FLOAT} and {@code DOUBLE} primitives. The value passed is the
     * actual {@code double} (already reconstructed for narrowed float columns), not raw bits.
     */
    default void onDoublePrimitive(int columnIndex, String dottedPath, byte type, double value) {}

    /** Called in typed mode for boolean primitives. */
    default void onBooleanPrimitive(int columnIndex, String dottedPath, boolean value) {}

    /**
     * Invoked once per array encountered as a direct leaf value under an object. The encoder still
     * encodes the array; this hook simply tells the caller that a complex value was seen at that
     * column so it can decide how to react.
     */
    default void onArrayLeaf(int columnIndex, String dottedPath) {}
}
