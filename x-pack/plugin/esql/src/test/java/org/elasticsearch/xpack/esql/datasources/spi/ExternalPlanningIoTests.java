/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class ExternalPlanningIoTests extends ESTestCase {

    public void testInactiveHolderIgnoresEvents() {
        StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();
        counters.addRequest(1_000, 0);
        counters.addBytes(64);
        ExternalPlanningIo.addMetadataGet(8);
        assertNull(ExternalPlanningIo.current());
    }

    public void testCountersAndMetadataGetFoldIntoActiveHolder() {
        ExternalPlanningIo io = new ExternalPlanningIo();
        try (var ignored = ExternalPlanningIo.activate(io)) {
            StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();
            counters.addRequest(500, 0);
            counters.addBytes(17);
            counters.addRequest(250, 4);
            ExternalPlanningIo.addMetadataGet(1);
            assertEquals(22L, io.bytesRead());
            assertEquals(3L, io.requestCount());
        }
        assertNull(ExternalPlanningIo.current());
    }

    public void testMeteredProbeCountsReceivedBytesNotWindowLength() throws IOException {
        byte[] payload = new byte[1024];
        random().nextBytes(payload);
        ExternalPlanningIo io = new ExternalPlanningIo();
        StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();
        try (var ignored = ExternalPlanningIo.activate(io)) {
            counters.addRequest(1, 0);
            try (MeteredInputStream in = new MeteredInputStream(new ByteArrayInputStream(payload), counters)) {
                byte[] buf = new byte[32];
                assertEquals(32, in.read(buf));
            }
        }
        assertEquals("received bytes, not the 1024-byte advertised window", 32L, io.bytesRead());
        assertEquals(1L, io.requestCount());
        assertEquals(32L, counters.snapshot().bytesRead());
        assertEquals(1L, counters.snapshot().requestCount());
    }

    public void testSnapshotAndResetClearsHolder() {
        ExternalPlanningIo io = new ExternalPlanningIo();
        try (var ignored = ExternalPlanningIo.activate(io)) {
            ExternalPlanningIo.addMetadataGet(9);
            ExternalPlanningIo.addStreamBytes(3);
        }
        long[] snap = io.snapshotAndReset();
        assertEquals(12L, snap[0]);
        assertEquals(1L, snap[1]);
        assertEquals(0L, io.bytesRead());
        assertEquals(0L, io.requestCount());
    }

    public void testStickyCaptureSurvivesActivateRestore() {
        ExternalPlanningIo io = new ExternalPlanningIo();
        StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();
        try (var ignored = ExternalPlanningIo.activate(io)) {
            counters.bindPlanningIo();
        }
        assertNull(ExternalPlanningIo.current());
        counters.addRequest(1, 0);
        counters.addBytes(11);
        assertEquals(11L, io.bytesRead());
        assertEquals(1L, io.requestCount());
    }

    public void testAttachStopsPlanningRebind() {
        ExternalPlanningIo io = new ExternalPlanningIo();
        StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();
        try (var ignored = ExternalPlanningIo.activate(io)) {
            counters.bindPlanningIo();
        }
        counters.attach(ExternalSourceMetrics.NOOP, "s3");
        try (var ignored = ExternalPlanningIo.activate(io)) {
            counters.bindPlanningIo();
            counters.addRequest(1, 0);
            counters.addBytes(99);
        }
        assertEquals("execution attach must not leak into planning totals", 0L, io.bytesRead());
        assertEquals(0L, io.requestCount());
        assertEquals(99L, counters.snapshot().bytesRead());
        assertEquals(1L, counters.snapshot().requestCount());
    }

    public void testNestedActivateRestoresPrevious() {
        ExternalPlanningIo outer = new ExternalPlanningIo();
        ExternalPlanningIo inner = new ExternalPlanningIo();
        try (var outerScope = ExternalPlanningIo.activate(outer)) {
            ExternalPlanningIo.addMetadataGet(0);
            try (var innerScope = ExternalPlanningIo.activate(inner)) {
                ExternalPlanningIo.addMetadataGet(0);
                assertSame(inner, ExternalPlanningIo.current());
            }
            assertSame(outer, ExternalPlanningIo.current());
        }
        assertEquals(1L, outer.requestCount());
        assertEquals(1L, inner.requestCount());
        assertNull(ExternalPlanningIo.current());
    }
}
