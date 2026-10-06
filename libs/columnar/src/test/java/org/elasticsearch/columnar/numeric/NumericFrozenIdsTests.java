/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.elasticsearch.test.ESTestCase;

/**
 * Pins the frozen wire ids for numeric transforms and the FOR terminal. Two guarantees:
 * literal assertions ensure the wire values cannot change silently; the rebuild test
 * exercises the registry through the owner constants so the registry cannot drift from
 * the owning classes.
 *
 * <p>Same-package placement gives access to package-private {@code ID} constants without
 * making them public API.
 */
public class NumericFrozenIdsTests extends ESTestCase {

    public void testTransformIdsAreStable() {
        assertEquals((byte) 0, DeltaTransform.ID);
        assertEquals((byte) 1, OffsetTransform.ID);
        assertEquals((byte) 2, GcdTransform.ID);
        assertEquals((byte) 3, SplitDeltaTransform.ID);
        assertEquals((byte) 4, AlpDoubleTransform.ID);
    }

    public void testTerminalIdIsStable() {
        assertEquals((byte) 0x40, ForTerminal.ID);
    }

    public void testRegisteredIdsRebuild() {
        final byte[] allTransformIds = {
            DeltaTransform.ID,
            OffsetTransform.ID,
            GcdTransform.ID,
            SplitDeltaTransform.ID,
            AlpDoubleTransform.ID };
        final NumericPipeline pipeline = NumericPipeline.Registry.rebuild(ForTerminal.ID, allTransformIds, 128);
        assertArrayEquals(allTransformIds, pipeline.transformIds());
        assertEquals(ForTerminal.ID, pipeline.terminalId());
    }

    public void testRegistryRejectsUnknownTransformId() {
        final byte unknownId = 127;
        final IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> NumericPipeline.Registry.rebuild(ForTerminal.ID, new byte[] { unknownId }, 128)
        );
        assertTrue(ex.getMessage().contains(String.valueOf(unknownId)));
    }

    public void testRegistryRejectsUnknownTerminalId() {
        final byte unknownId = 127;
        final IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> NumericPipeline.Registry.rebuild(unknownId, new byte[0], 128)
        );
        assertTrue(ex.getMessage().contains(String.valueOf(unknownId)));
    }
}
