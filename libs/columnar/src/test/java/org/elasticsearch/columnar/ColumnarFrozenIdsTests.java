/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.elasticsearch.columnar.numeric.SkipIndexCodec;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.test.ESTestCase;

/**
 * Pins every allocated frozen byte id for the public extensibility axes: field types, block-byte
 * codecs, and skip-index codecs. Any renumber, reuse, or accidental removal of an id will fail
 * this suite. Ids may only be added; they can never be reused, renumbered, or removed once
 * shipped, because old segments record them and must decode.
 *
 * <p>Numeric transform and terminal ids are package-private by design and are covered by
 * {@code org.elasticsearch.columnar.numeric.NumericFrozenIdsTests}, which lives in the same
 * package and references the owner constants directly.
 */
public class ColumnarFrozenIdsTests extends ESTestCase {

    public void testColumnarFieldTypeIds() {
        assertEquals((byte) 0, ColumnarFieldType.LONG.id());
        assertEquals((byte) 1, ColumnarFieldType.DOUBLE.id());
        assertEquals((byte) 2, ColumnarFieldType.STRING.id());
    }

    public void testBlockBytesCodecIdentityIdIsStable() {
        assertEquals((byte) 0, BlockBytesCodec.IDENTITY_ID);
        assertNotNull(BlockBytesCodec.forId(BlockBytesCodec.IDENTITY_ID));
    }

    public void testSkipIndexCodecMultiLevelIdIsStable() {
        assertEquals((byte) 0, SkipIndexCodec.MULTI_LEVEL_ID);
        assertNotNull(SkipIndexCodec.forId(SkipIndexCodec.MULTI_LEVEL_ID));
    }

    public void testBlockBytesCodecRejectsUnknownId() {
        final byte unknownId = 127;
        final IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> BlockBytesCodec.forId(unknownId));
        assertTrue(ex.getMessage().contains(String.valueOf(unknownId)));
    }

    public void testSkipIndexCodecRejectsUnknownId() {
        final byte unknownId = 127;
        final IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> SkipIndexCodec.forId(unknownId));
        assertTrue(ex.getMessage().contains(String.valueOf(unknownId)));
    }
}
