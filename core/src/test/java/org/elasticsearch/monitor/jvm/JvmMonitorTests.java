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

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.hasToString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JvmMonitorTests extends ESTestCase {

    public void testMonitorFailure() {
        AtomicBoolean shouldFail = new AtomicBoolean();
        AtomicBoolean invoked = new AtomicBoolean();
        JvmGcMonitorService.JvmMonitor monitor = new JvmGcMonitorService.JvmMonitor(Collections.emptyMap()) {
            @Override
            void onMonitorFailure(Throwable t) {
                invoked.set(true);
                assertThat(t, instanceOf(RuntimeException.class));
                assertThat(t, hasToString(containsString("simulated")));
            }

            @Override
            synchronized void monitorLongGc() {
                if (shouldFail.get()) {
                    throw new RuntimeException("simulated");
                }
            }

            @Override
            void onSlowGc(
                final Threshold threshold,
                final String name,
                final long seq,
                final TimeValue elapsed,
                final long totalGcCollectionCount,
                final long currentGcCollectionCount,
                final TimeValue totalGcCollectionTime, final TimeValue currentGcCollectionTime,
                final ByteSizeValue lastHeapUsed,
                final ByteSizeValue currentHeapUsed,
                final ByteSizeValue maxHeapUsed,
                final String pools) {
            }
        };

        monitor.run();
        assertFalse(invoked.get());

        shouldFail.set(true);
        monitor.run();
        assertTrue(invoked.get());
    }

    public void testSlowGc() {

        final int initialYoungCollectionCount = randomIntBetween(1, 4);
        final int initialYoungCollectionTime = randomIntBetween(initialYoungCollectionCount * 100, initialYoungCollectionCount * 200);

        final int initialOldCollectionCount = randomIntBetween(1, 4);
        final int initialOldCollectionTime = randomIntBetween(initialYoungCollectionCount * 1000, initialYoungCollectionCount * 2000);

        final JvmStats.GarbageCollector initialYoungCollector = mock(JvmStats.GarbageCollector.class);
        when(initialYoungCollector.getName()).thenReturn("young");
        when(initialYoungCollector.getCollectionCount()).thenReturn((long) initialYoungCollectionCount);
        when(initialYoungCollector.getCollectionTime()).thenReturn(TimeValue.timeValueMillis(initialYoungCollectionTime));

        final JvmStats.GarbageCollector initialOldCollector = mock(JvmStats.GarbageCollector.class);
        when(initialYoungCollector.getName()).thenReturn("old");
        when(initialOldCollector.getCollectionCount()).thenReturn((long) initialOldCollectionCount);
        when(initialOldCollector.getCollectionTime()).thenReturn(TimeValue.timeValueMillis(initialOldCollectionTime));
        JvmStats initialJvmStats = jvmStats(initialYoungCollector, initialOldCollector);

        final Map<String, JvmGcMonitorService.GcThreshold> gcThresholds = new HashMap<>();

        // fake debug threshold, info will be double this and warn will
        // be triple
        final int youngDebugThreshold = randomIntBetween(1, 10) * 100;
        gcThresholds.put(
            "young",
            new JvmGcMonitorService.GcThreshold("young", youngDebugThreshold * 3, youngDebugThreshold * 2, youngDebugThreshold));

        final boolean youngGcThreshold = randomBoolean();
        final JvmGcMonitorService.JvmMonitor.Threshold youngThresholdLevel = randomFrom(JvmGcMonitorService.JvmMonitor.Threshold.values());
        final int youngMultiplier = 1 + youngThresholdLevel.ordinal();
        final int youngCollections = randomIntBetween(1, 4);

        final JvmStats.GarbageCollector youngCollector;
        youngCollector = mock(JvmStats.GarbageCollector.class);
        when(youngCollector.getName()).thenReturn("young");
        when(youngCollector.getCollectionCount()).thenReturn((long) (initialYoungCollectionCount + youngCollections));

        final int youngIncrement;
        if (youngGcThreshold) {
            // we are faking that youngCollections collections occurred
            // this number is chosen so that we squeak over the
            // random threshold when computing the average collection
            // time: note that average collection time will just be
            // youngMultiplier * youngDebugThreshold + 1 which ensures
            // that we are over the right threshold but below the next
            // threshold
            youngIncrement = youngCollections * youngMultiplier * youngDebugThreshold + youngCollections;
        } else {
            // fake that we did not exceed the threshold
            youngIncrement = randomIntBetween(1, youngDebugThreshold);
        }
        when(youngCollector.getCollectionTime()).thenReturn(TimeValue.timeValueMillis(initialYoungCollectionTime + youngIncrement));

        // fake debug threshold, info will be double this and warn will
        // be triple
        final int oldDebugThreshold = randomIntBetween(1, 10) * 100;
        gcThresholds.put(
            "old",
            new JvmGcMonitorService.GcThreshold("old", oldDebugThreshold * 3, oldDebugThreshold * 2, oldDebugThreshold));

        final boolean oldGcThreshold = randomBoolean();
        final JvmGcMonitorService.JvmMonitor.Threshold oldThresholdLevel = randomFrom(JvmGcMonitorService.JvmMonitor.Threshold.values());
        final int oldMultiplier = 1 + oldThresholdLevel.ordinal();
        final int oldCollections = randomIntBetween(1, 4);

        final JvmStats.GarbageCollector oldCollector = mock(JvmStats.GarbageCollector.class);
        when(oldCollector.getName()).thenReturn("old");
        when(oldCollector.getCollectionCount()).thenReturn((long) (initialOldCollectionCount + oldCollections));
        final long oldIncrement;
        if (oldGcThreshold) {
            // we are faking that oldCollections collections occurred
            // this number is chosen so that we squeak over the
            // random threshold when computing the average collection
            // time: note that average collection time will just be
            // oldMultiplier * oldDebugThreshold + 1 which ensures
            // that we are over the right threshold but below the next
            // threshold
            oldIncrement = oldCollections * oldMultiplier * oldDebugThreshold + oldCollections;
        } else {
            // fake that we did not exceed the threshold
            oldIncrement = randomIntBetween(1, oldDebugThreshold);
        }
        when(oldCollector.getCollectionTime()).thenReturn(TimeValue.timeValueMillis(initialOldCollectionTime + oldIncrement));

        final long start = randomIntBetween(1, 1 << 30);
        final long expectedElapsed = randomIntBetween(1, 1000);
        final AtomicLong now = new AtomicLong(start);

        final AtomicReference<JvmStats> jvmStats = new AtomicReference<>();
        jvmStats.set(initialJvmStats);

        final AtomicInteger count = new AtomicInteger();

        JvmGcMonitorService.JvmMonitor monitor = new JvmGcMonitorService.JvmMonitor(gcThresholds) {
            @Override
            void onMonitorFailure(Throwable t) {
            }

            @Override
            void onSlowGc(
                final Threshold threshold,
                final String name,
                final long seq,
                final TimeValue elapsed,
                final long totalGcCollectionCount,
                final long currentGcCollectionCount,
                final TimeValue totalGcCollectionTime,
                final TimeValue currentGcCollectionTime,
                final ByteSizeValue lastHeapUsed,
                final ByteSizeValue currentHeapUsed,
                final ByteSizeValue maxHeapUsed,
                final String pools) {
                count.incrementAndGet();
                assertThat(seq, equalTo(1L));
                assertThat(elapsed, equalTo(TimeValue.timeValueMillis(expectedElapsed)));
                if ("young".equals(name)) {
                    assertThat(youngThresholdLevel, equalTo(threshold));
                    assertThat(name, equalTo("young"));
                    assertThat(totalGcCollectionCount, equalTo((long) (initialYoungCollectionCount + youngCollections)));
                    assertThat(currentGcCollectionCount, equalTo((long) youngCollections));
                    assertThat(currentGcCollectionTime, equalTo(TimeValue.timeValueMillis(youngIncrement)));
                    assertThat(totalGcCollectionTime, equalTo(TimeValue.timeValueMillis(initialYoungCollectionTime + youngIncrement)));
                } else if ("old".equals(name)) {
                    assertThat(oldThresholdLevel, equalTo(threshold));
                    assertThat(name, equalTo("old"));
                    assertThat(totalGcCollectionCount, equalTo((long) (initialOldCollectionCount + oldCollections)));
                    assertThat(currentGcCollectionCount, equalTo((long) oldCollections));
                    assertThat(currentGcCollectionTime, equalTo(TimeValue.timeValueMillis(oldIncrement)));
                    assertThat(totalGcCollectionTime, equalTo(TimeValue.timeValueMillis(initialOldCollectionTime + oldIncrement)));
                } else {
                    fail("unexpected name [" + name + "]");
                }
            }

            @Override
            long now() {
                return now.get();
            }

            @Override
            JvmStats jvmStats() {
                return jvmStats.get();
            }
        };

        final JvmStats monitorJvmStats = jvmStats(youngCollector, oldCollector);

        now.set(start + TimeUnit.NANOSECONDS.convert(expectedElapsed, TimeUnit.MILLISECONDS));
        jvmStats.set(monitorJvmStats);
        monitor.monitorLongGc();

        assertThat(count.get(), equalTo((youngGcThreshold ? 1 : 0) + (oldGcThreshold ? 1 : 0)));
    }

    private JvmStats jvmStats(JvmStats.GarbageCollector youngCollector, JvmStats.GarbageCollector oldCollector) {
        final JvmStats jvmStats = mock(JvmStats.class);
        final JvmStats.GarbageCollectors initialGcs = mock(JvmStats.GarbageCollectors.class);
        final JvmStats.GarbageCollector[] initialCollectors = new JvmStats.GarbageCollector[2];
        initialCollectors[0] = youngCollector;
        initialCollectors[1] = oldCollector;
        when(initialGcs.getCollectors()).thenReturn(initialCollectors);
        when(jvmStats.getGc()).thenReturn(initialGcs);
        when(jvmStats.getMem()).thenReturn(JvmStats.jvmStats().getMem());
        return jvmStats;
    }

}
