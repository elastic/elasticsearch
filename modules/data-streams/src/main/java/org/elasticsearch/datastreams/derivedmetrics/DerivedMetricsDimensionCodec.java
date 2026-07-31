/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;

import java.nio.charset.StandardCharsets;

/**
 * Encodes a series' dimension values into one byte sequence, and decodes them back.
 *
 * <p>The encoding exists so that a series can be identified by a single {@link BytesRef} rather than by a list of strings, which is what
 * lets the write path intern it to an ordinal without allocating.
 *
 * <p>Layout: a presence bitmap of {@code ceil(dimensions / 8)} bytes, then each present value as a four byte length followed by its
 * UTF-8 bytes, in configuration order. The bitmap is needed because a document missing a dimension forms its own series rather than
 * sharing an artificial "missing" value, so which dimensions were present is part of the identity. The number of dimensions is a property
 * of the metric, so the decoder is told it rather than having to recover it.
 *
 * <p>Encoding writes into a caller-owned scratch buffer reused across documents, so recording against a series that already exists
 * allocates nothing.
 */
public final class DerivedMetricsDimensionCodec {

    private static final int LENGTH_BYTES = 4;

    private DerivedMetricsDimensionCodec() {}

    /**
     * A reusable encoding buffer. One per thread on the write path; never shared.
     */
    public static final class Scratch {
        private byte[] bytes = new byte[128];
        private final BytesRef ref = new BytesRef();

        private void ensure(int capacity) {
            if (bytes.length < capacity) {
                int size = bytes.length;
                while (size < capacity) {
                    size <<= 1;
                }
                byte[] grown = new byte[size];
                System.arraycopy(bytes, 0, grown, 0, bytes.length);
                bytes = grown;
            }
        }
    }

    public static int bitmapLength(int dimensionCount) {
        return (dimensionCount + 7) / 8;
    }

    /**
     * Encodes the values the document actually had into the scratch buffer.
     *
     * @param values one entry per configured dimension, null where the document did not have that dimension
     * @return a {@link BytesRef} into the scratch buffer, valid only until the next encode on that scratch
     */
    public static BytesRef encode(String[] values, int dimensionCount, Scratch scratch) {
        int bitmap = bitmapLength(dimensionCount);
        scratch.ensure(Math.max(bitmap, 128));
        for (int i = 0; i < bitmap; i++) {
            scratch.bytes[i] = 0;
        }
        int offset = bitmap;
        for (int i = 0; i < dimensionCount; i++) {
            String value = values[i];
            if (value == null) {
                continue;
            }
            scratch.bytes[i >>> 3] |= (byte) (1 << (i & 7));
            scratch.ensure(offset + LENGTH_BYTES + value.length() * UnicodeUtil.MAX_UTF8_BYTES_PER_CHAR);
            // UTF16toUTF8 returns where it stopped writing, not how much it wrote
            int end = UnicodeUtil.UTF16toUTF8(value, 0, value.length(), scratch.bytes, offset + LENGTH_BYTES);
            int length = end - (offset + LENGTH_BYTES);
            scratch.bytes[offset] = (byte) (length >>> 24);
            scratch.bytes[offset + 1] = (byte) (length >>> 16);
            scratch.bytes[offset + 2] = (byte) (length >>> 8);
            scratch.bytes[offset + 3] = (byte) length;
            offset += LENGTH_BYTES + length;
        }
        scratch.ref.bytes = scratch.bytes;
        scratch.ref.offset = 0;
        scratch.ref.length = offset;
        return scratch.ref;
    }

    /**
     * Decodes an encoded tuple back into one entry per configured dimension, null where the dimension was absent. Only called at flush,
     * so allocating here is fine.
     */
    public static String[] decode(BytesRef encoded, int dimensionCount) {
        String[] values = new String[dimensionCount];
        byte[] bytes = encoded.bytes;
        int offset = encoded.offset + bitmapLength(dimensionCount);
        for (int i = 0; i < dimensionCount; i++) {
            boolean present = (bytes[encoded.offset + (i >>> 3)] & (1 << (i & 7))) != 0;
            if (present == false) {
                continue;
            }
            int length = ((bytes[offset] & 0xFF) << 24) | ((bytes[offset + 1] & 0xFF) << 16) | ((bytes[offset + 2] & 0xFF) << 8)
                | (bytes[offset + 3] & 0xFF);
            offset += LENGTH_BYTES;
            values[i] = new String(bytes, offset, length, StandardCharsets.UTF_8);
            offset += length;
        }
        return values;
    }
}
