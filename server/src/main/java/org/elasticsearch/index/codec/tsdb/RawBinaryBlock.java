/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.IndexInput;

/**
 * A compressed binary doc-values block handed from a source segment's decoder to the merge writer
 * so it can be copied verbatim into the target segment, bypassing decompression and re-compression.
 * Only produced for blocks holding exactly one document.
 *
 * <p>{@code payload} is an {@link IndexInput} positioned at the first byte the block's decompressor
 * would read — i.e. immediately after the encoded doc offsets. It is owned by the producing decoder
 * and is valid only until the next call on that decoder; the consumer must read exactly
 * {@code payloadLength} bytes from it before doing anything else, and must never close it.
 *
 * @param compression        the compression mode recorded for the source field entry; used by the
 *                           consumer to verify it matches the target's algorithm before copying
 * @param compressed         the source block's own per-block header bit; the format allows
 *                           heterogeneous per-block flags, so this may differ from the field-level
 *                           compression mode
 * @param uncompressedLength the block's uncompressed byte length (equals the single value's length)
 * @param payload            input positioned at the payload; caller reads exactly payloadLength bytes
 * @param payloadLength      number of bytes to read from payload
 */
record RawBinaryBlock(
    BinaryDVCompressionMode compression,
    boolean compressed,
    int uncompressedLength,
    IndexInput payload,
    long payloadLength
) {}
