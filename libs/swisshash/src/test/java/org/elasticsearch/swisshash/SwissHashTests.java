/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.swisshash;

import org.elasticsearch.test.ESTestCase;

public class SwissHashTests extends ESTestCase {

    public void testPageCountsAtLargeCapacities() {
        assertEquals(229_376, SwissHash.pagesNeeded(469_762_048, Long.BYTES));
        assertEquals(131_072, SwissHash.pagesNeeded(536_870_912, Integer.BYTES));
        assertEquals(262_144, SwissHash.pagesNeeded(536_870_912, Long.BYTES));
        assertEquals(524_288, SwissHash.pagesNeeded(536_870_912, Long.BYTES * 2));

        assertEquals(458_752, SwissHash.pagesNeeded(939_524_096, Long.BYTES));
        assertEquals(262_144, SwissHash.pagesNeeded(1_073_741_824, Integer.BYTES));
        assertEquals(524_288, SwissHash.pagesNeeded(1_073_741_824, Long.BYTES));
        assertEquals(917_504, SwissHash.pagesNeeded(939_524_096, Long.BYTES * 2));
    }

    public void testPartialPages() {
        assertEquals(1, SwissHash.pagesNeeded(1, Long.BYTES));
        assertEquals(1, SwissHash.pagesNeeded(2048, Long.BYTES));
        assertEquals(2, SwissHash.pagesNeeded(2049, Long.BYTES));
        assertEquals(1, SwissHash.pagesNeeded(4096, Integer.BYTES));
        assertEquals(2, SwissHash.pagesNeeded(4097, Integer.BYTES));
    }

    public void testMaximumCapacity() {
        assertEquals(1_073_741_824, SwissHash.nextCapacity(536_870_912));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> SwissHash.nextCapacity(1_073_741_824));
        assertEquals("maximum hash capacity [1073741824] exceeded", e.getMessage());
    }
}
