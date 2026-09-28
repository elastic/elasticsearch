/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.elasticsearch.test.ESTestCase;

/**
 * Pins the frozen wire ids for string column layouts, the way
 * {@code org.elasticsearch.columnar.numeric.NumericFrozenIdsTests} does for numeric transforms. A layout id is
 * recorded in every string column's metadata, so renumbering, reusing, or removing one would make already
 * written segments decode as the wrong layout. Ids may only be added.
 */
public class StringFrozenIdsTests extends ESTestCase {

    public void testLayoutIdsAreStable() {
        assertEquals((byte) 0, StringColumnLayout.PLAIN.id());
        assertEquals((byte) 1, StringColumnLayout.DICTIONARY.id());
    }

    public void testLayoutRoundTripsThroughItsId() {
        for (StringColumnLayout layout : StringColumnLayout.values()) {
            assertEquals(layout, StringColumnLayout.fromId(layout.id()));
        }
    }

    public void testLayoutRejectsUnknownId() {
        final byte unknownId = 127;
        final IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> StringColumnLayout.fromId(unknownId));
        assertTrue(ex.getMessage().contains(String.valueOf(unknownId)));
    }
}
