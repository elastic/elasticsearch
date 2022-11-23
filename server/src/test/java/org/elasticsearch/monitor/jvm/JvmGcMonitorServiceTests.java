/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor.jvm;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class JvmGcMonitorServiceTests extends ESTestCase {

    public void testSlowGcLogging() {
        final Logger logger = mock(Logger.class);
        when(logger.isWarnEnabled()).thenReturn(true);
        when(logger.isInfoEnabled()).thenReturn(true);
        when(logger.isDebugEnabled()).thenReturn(true);
        final JvmGcMonitorService.JvmMonitor.Threshold threshold = randomFrom(JvmGcMonitorService.JvmMonitor.Threshold.values());
        final String name = randomAlphaOfLength(16);
        final long seq = randomIntBetween(1, 1 << 30);
        final int elapsedValue = randomIntBetween(1, 1 << 10);
        final long totalCollectionCount = randomIntBetween(1, 16);
        final long currentCollectionCount = randomIntBetween(1, 16);
        final TimeValue totalCollectionTime = TimeValue.timeValueMillis(randomIntBetween(1, elapsedValue));
        final TimeValue currentCollectionTime = TimeValue.timeValueMillis(randomIntBetween(1, elapsedValue));

        final ByteSizeValue lastHeapUsed = ByteSizeValue.ofBytes(randomIntBetween(1, 1 << 10));
        JvmStats lastJvmStats = mock(JvmStats.class);
        JvmStats.Mem lastMem = mock(JvmStats.Mem.class);
        when(lastMem.getHeapUsed()).thenReturn(lastHeapUsed);
        when(lastJvmStats.getMem()).thenReturn(lastMem);
        when(lastJvmStats.toString()).thenReturn("last");

        final ByteSizeValue currentHeapUsed = ByteSizeValue.ofBytes(randomIntBetween(1, 1 << 10));
        JvmStats currentJvmStats = mock(JvmStats.class);
        JvmStats.Mem currentMem = mock(JvmStats.Mem.class);
        when(currentMem.getHeapUsed()).thenReturn(currentHeapUsed);
        when(currentJvmStats.getMem()).thenReturn(currentMem);
        when(currentJvmStats.toString()).thenReturn("current");

        JvmStats.GarbageCollector gc = mock(JvmStats.GarbageCollector.class);
        when(gc.getName()).thenReturn(name);
        when(gc.getCollectionCount()).thenReturn(totalCollectionCount);
        when(gc.getCollectionTime()).thenReturn(totalCollectionTime);

        final ByteSizeValue maxHeapUsed = ByteSizeValue.ofBytes(Math.max(lastHeapUsed.getBytes(), currentHeapUsed.getBytes()) + 1 << 10);

        JvmGcMonitorService.JvmMonitor.SlowGcEvent slowGcEvent = new JvmGcMonitorService.JvmMonitor.SlowGcEvent(
            gc,
            currentCollectionCount,
            currentCollectionTime,
            elapsedValue,
            lastJvmStats,
            currentJvmStats,
            maxHeapUsed
        );

        JvmGcMonitorService.logSlowGc(logger, threshold, seq, slowGcEvent, (l, c) -> l.toString() + ", " + c.toString());

        switch (threshold) {
            case WARN -> {
                verify(logger).isWarnEnabled();
                verify(logger).warn(
                    "[gc][{}][{}][{}] duration [{}], collections [{}]/[{}], total [{}]/[{}], memory [{}]->[{}]/[{}], all_pools {}",
                    name,
                    seq,
                    totalCollectionCount,
                    currentCollectionTime,
                    currentCollectionCount,
                    TimeValue.timeValueMillis(elapsedValue),
                    currentCollectionTime,
                    totalCollectionTime,
                    lastHeapUsed,
                    currentHeapUsed,
                    maxHeapUsed,
                    "last, current"
                );
            }
            case INFO -> {
                verify(logger).isInfoEnabled();
                verify(logger).info(
                    "[gc][{}][{}][{}] duration [{}], collections [{}]/[{}], total [{}]/[{}], memory [{}]->[{}]/[{}], all_pools {}",
                    name,
                    seq,
                    totalCollectionCount,
                    currentCollectionTime,
                    currentCollectionCount,
                    TimeValue.timeValueMillis(elapsedValue),
                    currentCollectionTime,
                    totalCollectionTime,
                    lastHeapUsed,
                    currentHeapUsed,
                    maxHeapUsed,
                    "last, current"
                );
            }
            case DEBUG -> {
                verify(logger).isDebugEnabled();
                verify(logger).debug(
                    "[gc][{}][{}][{}] duration [{}], collections [{}]/[{}], total [{}]/[{}], memory [{}]->[{}]/[{}], all_pools {}",
                    name,
                    seq,
                    totalCollectionCount,
                    currentCollectionTime,
                    currentCollectionCount,
                    TimeValue.timeValueMillis(elapsedValue),
                    currentCollectionTime,
                    totalCollectionTime,
                    lastHeapUsed,
                    currentHeapUsed,
                    maxHeapUsed,
                    "last, current"
                );
            }
        }
        verifyNoMoreInteractions(logger);
    }

    public void testGcOverheadLogging() {
        final JvmGcMonitorService.JvmMonitor.Threshold threshold = randomFrom(JvmGcMonitorService.JvmMonitor.Threshold.values());
        final int current = randomIntBetween(1, Integer.MAX_VALUE);
        final long elapsed = randomIntBetween(current, Integer.MAX_VALUE);
        final long seq = randomIntBetween(1, Integer.MAX_VALUE);
        final Logger logger = mock(Logger.class);
        when(logger.isWarnEnabled()).thenReturn(true);
        when(logger.isInfoEnabled()).thenReturn(true);
        when(logger.isDebugEnabled()).thenReturn(true);
        JvmGcMonitorService.logGcOverhead(logger, threshold, current, elapsed, seq);
        switch (threshold) {
            case WARN -> {
                verify(logger).isWarnEnabled();
                verify(logger).warn(
                    "[gc][{}] overhead, spent [{}] collecting in the last [{}]",
                    seq,
                    TimeValue.timeValueMillis(current),
                    TimeValue.timeValueMillis(elapsed)
                );
            }
            case INFO -> {
                verify(logger).isInfoEnabled();
                verify(logger).info(
                    "[gc][{}] overhead, spent [{}] collecting in the last [{}]",
                    seq,
                    TimeValue.timeValueMillis(current),
                    TimeValue.timeValueMillis(elapsed)
                );
            }
            case DEBUG -> {
                verify(logger).isDebugEnabled();
                verify(logger).debug(
                    "[gc][{}] overhead, spent [{}] collecting in the last [{}]",
                    seq,
                    TimeValue.timeValueMillis(current),
                    TimeValue.timeValueMillis(elapsed)
                );
            }
        }
        verifyNoMoreInteractions(logger);
    }

}
