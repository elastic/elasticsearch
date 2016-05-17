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

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class JvmGcMonitorServiceTests extends ESTestCase {

    public void testSlowGcLogging() {
        final ESLogger logger = mock(ESLogger.class);
        when(logger.isWarnEnabled()).thenReturn(true);
        when(logger.isInfoEnabled()).thenReturn(true);
        when(logger.isDebugEnabled()).thenReturn(true);
        final JvmGcMonitorService.JvmMonitor.Threshold threshold = randomFrom(JvmGcMonitorService.JvmMonitor.Threshold.values());
        final String name = randomAsciiOfLength(16);
        final long seq = randomIntBetween(1, 1 << 30);
        final int elapsedValue = randomIntBetween(1, 1 << 10);
        final TimeValue elapsed = TimeValue.timeValueMillis(randomIntBetween(1, 1 << 10));
        final long totalCollectionCount = randomIntBetween(1, 16);
        final long currentCollectionCount = randomIntBetween(1, 16);
        final TimeValue totalCollectionTime = TimeValue.timeValueMillis(randomIntBetween(1, elapsedValue));
        final TimeValue currentColletionTime = TimeValue.timeValueMillis(randomIntBetween(1, elapsedValue));
        final ByteSizeValue lastHeapUsed = new ByteSizeValue(randomIntBetween(1, 1 << 10));
        final ByteSizeValue currentHeapUsed = new ByteSizeValue(randomIntBetween(1, 1 << 10));
        final ByteSizeValue maxHeapUsed = new ByteSizeValue(Math.max(lastHeapUsed.bytes(), currentHeapUsed.bytes()) + 1 << 10);
        JvmGcMonitorService.logSlowGc(
            logger,
            threshold,
            name,
            seq,
            elapsed,
            totalCollectionCount,
            currentCollectionCount,
            totalCollectionTime,
            currentColletionTime,
            lastHeapUsed,
            currentHeapUsed,
            maxHeapUsed,
            "asdf");
        switch (threshold) {
            case WARN:
                verify(logger).isWarnEnabled();
                verify(logger).warn(
                    "[gc][{}][{}][{}] duration [{}], collections [{}]/[{}], total [{}]/[{}], memory [{}]->[{}]/[{}], all_pools {}",
                    name,
                    seq,
                    totalCollectionCount,
                    currentColletionTime,
                    currentCollectionCount,
                    elapsed,
                    currentColletionTime,
                    totalCollectionTime,
                    lastHeapUsed,
                    currentHeapUsed,
                    maxHeapUsed,
                    "asdf");
                break;
            case INFO:
                verify(logger).isInfoEnabled();
                verify(logger).info(
                    "[gc][{}][{}][{}] duration [{}], collections [{}]/[{}], total [{}]/[{}], memory [{}]->[{}]/[{}], all_pools {}",
                    name,
                    seq,
                    totalCollectionCount,
                    currentColletionTime,
                    currentCollectionCount,
                    elapsed,
                    currentColletionTime,
                    totalCollectionTime,
                    lastHeapUsed,
                    currentHeapUsed,
                    maxHeapUsed,
                    "asdf");
                break;
            case DEBUG:
                verify(logger).isDebugEnabled();
                verify(logger).debug(
                    "[gc][{}][{}][{}] duration [{}], collections [{}]/[{}], total [{}]/[{}], memory [{}]->[{}]/[{}], all_pools {}",
                    name,
                    seq,
                    totalCollectionCount,
                    currentColletionTime,
                    currentCollectionCount,
                    elapsed,
                    currentColletionTime,
                    totalCollectionTime,
                    lastHeapUsed,
                    currentHeapUsed,
                    maxHeapUsed,
                    "asdf");
                break;
        }
        verifyNoMoreInteractions(logger);
    }

}
