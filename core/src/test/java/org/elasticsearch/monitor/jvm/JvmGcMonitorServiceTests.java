/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.monitor.jvm;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
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

        final ByteSizeValue lastHeapUsed = new ByteSizeValue(randomIntBetween(1, 1 << 10));
        JvmStats lastJvmStats = mock(JvmStats.class);
        JvmStats.Mem lastMem = mock(JvmStats.Mem.class);
        when(lastMem.getHeapUsed()).thenReturn(lastHeapUsed);
        when(lastJvmStats.getMem()).thenReturn(lastMem);
        when(lastJvmStats.toString()).thenReturn("last");

        final ByteSizeValue currentHeapUsed = new ByteSizeValue(randomIntBetween(1, 1 << 10));
        JvmStats currentJvmStats = mock(JvmStats.class);
        JvmStats.Mem currentMem = mock(JvmStats.Mem.class);
        when(currentMem.getHeapUsed()).thenReturn(currentHeapUsed);
        when(currentJvmStats.getMem()).thenReturn(currentMem);
        when(currentJvmStats.toString()).thenReturn("current");

        JvmStats.GarbageCollector gc = mock(JvmStats.GarbageCollector.class);
        when(gc.getName()).thenReturn(name);
        when(gc.getCollectionCount()).thenReturn(totalCollectionCount);
        when(gc.getCollectionTime()).thenReturn(totalCollectionTime);

        final ByteSizeValue maxHeapUsed = new ByteSizeValue(Math.max(lastHeapUsed.getBytes(), currentHeapUsed.getBytes()) + 1 << 10);

        JvmGcMonitorService.JvmMonitor.SlowGcEvent slowGcEvent = new JvmGcMonitorService.JvmMonitor.SlowGcEvent(
            gc,
            currentCollectionCount,
            currentCollectionTime,
            elapsedValue,
            lastJvmStats,
            currentJvmStats,
            maxHeapUsed);

        JvmGcMonitorService.logSlowGc(logger, threshold, seq, slowGcEvent, (l, c) -> l.toString() + ", " + c.toString());

        switch (threshold) {
            case WARN:
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
                    "last, current");
                break;
            case INFO:
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
                    "last, current");
                break;
            case DEBUG:
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
                    "last, current");
                break;
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
        switch(threshold) {
            case WARN:
                verify(logger).isWarnEnabled();
                verify(logger).warn(
                    "[gc][{}] overhead, spent [{}] collecting in the last [{}]",
                    seq,
                    TimeValue.timeValueMillis(current),
                    TimeValue.timeValueMillis(elapsed));
                break;
            case INFO:
                verify(logger).isInfoEnabled();
                verify(logger).info(
                    "[gc][{}] overhead, spent [{}] collecting in the last [{}]",
                    seq,
                    TimeValue.timeValueMillis(current),
                    TimeValue.timeValueMillis(elapsed));
                break;
            case DEBUG:
                verify(logger).isDebugEnabled();
                verify(logger).debug(
                    "[gc][{}] overhead, spent [{}] collecting in the last [{}]",
                    seq,
                    TimeValue.timeValueMillis(current),
                    TimeValue.timeValueMillis(elapsed));
                break;
        }
        verifyNoMoreInteractions(logger);
    }

}
