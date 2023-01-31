/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiler;

import org.elasticsearch.test.ESTestCase;

public class EventsIndexTests extends ESTestCase {
    public void testFullIndex() {
        EventsIndex idx = EventsIndex.FULL_INDEX;
        assertEquals("profiling-events-all", idx.getName());
        assertEquals(1.0d, idx.getSampleRate(), 1e-3);
    }

    public void testResampledIndexSameSize() {
        EventsIndex resampledIndex = EventsIndex.MEDIUM_DOWNSAMPLED.getResampledIndex(100, 100);
        assertEquals("profiling-events-5pow06", resampledIndex.getName());
        assertEquals(Math.pow(1.0d / 5.0d, 6.0d), resampledIndex.getSampleRate(), 1e-9);
    }

    public void testResampledIndexDifferentSizes() {
        assertResampledIndex("profiling-events-5pow01", Math.pow(5.0d, 5));
        assertResampledIndex("profiling-events-5pow02", Math.pow(5.0d, 4));
        assertResampledIndex("profiling-events-5pow03", Math.pow(5.0d, 3));

        assertResampledIndex("profiling-events-5pow04", Math.pow(5.0d, 2));
        assertResampledIndex("profiling-events-5pow05", Math.pow(5.0d, 1));

        assertResampledIndex("profiling-events-5pow06", Math.pow(5.0d, 0));
        assertResampledIndex("profiling-events-5pow07", Math.pow(5.0d, -1));
        assertResampledIndex("profiling-events-5pow08", Math.pow(5.0d, -2));

        assertResampledIndex("profiling-events-5pow09", Math.pow(5.0d, -3));
        assertResampledIndex("profiling-events-5pow10", Math.pow(5.0d, -4));
        assertResampledIndex("profiling-events-5pow11", Math.pow(5.0d, -5));
    }

    private void assertResampledIndex(String expectedName, double ratio) {
        long currentSampleSize = 10_000_000L;
        long targetSampleSize = (long) (currentSampleSize * ratio);
        EventsIndex e = EventsIndex.MEDIUM_DOWNSAMPLED;
        assertEquals(expectedName, e.getResampledIndex(targetSampleSize, currentSampleSize).getName());
    }
}
