/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.test.ESTestCase;

/**
 * Pins the {@link SurvivorOffsets} primitive the A1/F1 stripe-offset fix rests on: the survivor offsets must be
 * page-aligned (the accepted rows' offsets, trimmed to the accepted count), and a no-tracking source must yield a
 * {@code null} finish (a legitimate safe-miss), never a bogus array.
 */
public class SurvivorOffsetsTests extends ESTestCase {

    public void testNotTrackingReturnsNull() {
        SurvivorOffsets s = SurvivorOffsets.of(null, 5);
        s.accept(0);
        s.accept(2);
        assertNull("no source offsets -> no survivor offsets (safe-miss, not a fabricated array)", s.finish());
    }

    public void testAcceptedSubsetIsPageAligned() {
        long[] source = { 10L, 20L, 30L, 40L, 50L };
        SurvivorOffsets s = SurvivorOffsets.of(source, source.length);
        // Parsed rows 1 and 3 dropped; the page carries rows 0, 2, 4.
        s.accept(0);
        s.accept(2);
        s.accept(4);
        assertArrayEquals(
            "survivor offsets are the accepted rows' offsets, trimmed to the accepted count",
            new long[] { 10L, 30L, 50L },
            s.finish()
        );
    }

    public void testAllAcceptedIsIdentity() {
        long[] source = { 7L, 8L, 9L };
        SurvivorOffsets s = SurvivorOffsets.of(source, source.length);
        for (int i = 0; i < source.length; i++) {
            s.accept(i);
        }
        assertArrayEquals(source, s.finish());
    }

    public void testNoneAcceptedIsEmptyNotNull() {
        SurvivorOffsets s = SurvivorOffsets.of(new long[] { 1L, 2L }, 2);
        assertArrayEquals("tracking on but nothing accepted -> empty array, not null", new long[0], s.finish());
    }
}
