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

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.hasToString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JvmMonitorTests extends ESTestCase {

    private static final JvmGcMonitorService.GcOverheadThreshold IGNORE = new JvmGcMonitorService.GcOverheadThreshold(0, 0, 0);

    public void testMonitorFailure() {
        AtomicBoolean shouldFail = new AtomicBoolean();
        AtomicBoolean invoked = new AtomicBoolean();
        JvmGcMonitorService.JvmMonitor monitor = new JvmGcMonitorService.JvmMonitor(Collections.emptyMap(), IGNORE) {
            @Override
            void onMonitorFailure(Exception e) {
                invoked.set(true);
                assertThat(e, instanceOf(RuntimeException.class));
                assertThat(e, hasToString(containsString("simulated")));
            }

            @Override
            synchronized void monitorGc() {
                if (shouldFail.get()) {
                    throw new RuntimeException("simulated");
                }
            }

            @Override
            void onSlowGc(final Threshold threshold, final long seq, final SlowGcEvent slowGcEvent) {
            }

            @Override
            void onGcOverhead(Threshold threshold, long total, long elapsed, long seq) {
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
        when(initialOldCollector.getName()).thenReturn("old");
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
        final int oldIncrement;
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

        JvmGcMonitorService.JvmMonitor monitor = new JvmGcMonitorService.JvmMonitor(gcThresholds, IGNORE) {
            @Override
            void onMonitorFailure(Exception e) {
            }

            @Override
            void onSlowGc(final Threshold threshold, final long seq, final SlowGcEvent slowGcEvent) {
                count.incrementAndGet();
                assertThat(seq, equalTo(1L));
                assertThat(slowGcEvent.elapsed, equalTo(expectedElapsed));
                assertThat(slowGcEvent.currentGc.getName(), anyOf(equalTo("young"), equalTo("old")));
                if ("young".equals(slowGcEvent.currentGc.getName())) {
                    assertCollection(
                        threshold,
                        youngThresholdLevel,
                        slowGcEvent,
                        initialYoungCollectionCount,
                        youngCollections,
                        initialYoungCollectionTime,
                        youngIncrement);
                } else if ("old".equals(slowGcEvent.currentGc.getName())) {
                    assertCollection(
                        threshold,
                        oldThresholdLevel,
                        slowGcEvent,
                        initialOldCollectionCount,
                        oldCollections,
                        initialOldCollectionTime,
                        oldIncrement);
                }
            }

            @Override
            void onGcOverhead(Threshold threshold, long total, long elapsed, long seq) {
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
        monitor.monitorGc();

        assertThat(count.get(), equalTo((youngGcThreshold ? 1 : 0) + (oldGcThreshold ? 1 : 0)));
    }

    private void assertCollection(
        final JvmGcMonitorService.JvmMonitor.Threshold actualThreshold,
        final JvmGcMonitorService.JvmMonitor.Threshold expectedThreshold,
        final JvmGcMonitorService.JvmMonitor.SlowGcEvent slowGcEvent,
        final int initialCollectionCount,
        final int collections,
        final int initialCollectionTime,
        final int increment) {
        assertThat(actualThreshold, equalTo(expectedThreshold));
        assertThat(slowGcEvent.currentGc.getCollectionCount(), equalTo((long) (initialCollectionCount + collections)));
        assertThat(slowGcEvent.collectionCount, equalTo((long) collections));
        assertThat(slowGcEvent.collectionTime, equalTo(TimeValue.timeValueMillis(increment)));
        assertThat(slowGcEvent.currentGc.getCollectionTime(), equalTo(TimeValue.timeValueMillis(initialCollectionTime + increment)));
    }

    private JvmStats jvmStats(JvmStats.GarbageCollector youngCollector, JvmStats.GarbageCollector oldCollector) {
        final JvmStats jvmStats = mock(JvmStats.class);
        final JvmStats.GarbageCollectors gcs = mock(JvmStats.GarbageCollectors.class);
        final JvmStats.GarbageCollector[] collectors = new JvmStats.GarbageCollector[2];
        collectors[0] = youngCollector;
        collectors[1] = oldCollector;
        when(gcs.getCollectors()).thenReturn(collectors);
        when(jvmStats.getGc()).thenReturn(gcs);
        when(jvmStats.getMem()).thenReturn(JvmStats.jvmStats().getMem());
        return jvmStats;
    }

    public void testMonitorGc() {
        final int youngCollectionCount = randomIntBetween(1, 16);
        final int youngCollectionIncrement = randomIntBetween(1, 16);
        final int youngCollectionTime = randomIntBetween(1, 1 << 10);
        final int youngCollectionTimeIncrement = randomIntBetween(1, 1 << 10);
        final int oldCollectionCount = randomIntBetween(1, 16);
        final int oldCollectionIncrement = randomIntBetween(1, 16);
        final int oldCollectionTime = randomIntBetween(1, 1 << 10);
        final int oldCollectionTimeIncrement = randomIntBetween(1, 1 << 10);

        final JvmStats.GarbageCollector lastYoungCollector = collector("young", youngCollectionCount, youngCollectionTime);
        final JvmStats.GarbageCollector lastOldCollector = collector("old", oldCollectionCount, oldCollectionTime);
        final JvmStats lastjvmStats = jvmStats(lastYoungCollector, lastOldCollector);

        final JvmStats.GarbageCollector currentYoungCollector =
            collector("young", youngCollectionCount + youngCollectionIncrement, youngCollectionTime + youngCollectionTimeIncrement);
        final JvmStats.GarbageCollector currentOldCollector =
            collector("old", oldCollectionCount + oldCollectionIncrement, oldCollectionTime + oldCollectionTimeIncrement);
        final JvmStats currentJvmStats = jvmStats(currentYoungCollector, currentOldCollector);
        final long expectedElapsed =
            randomIntBetween(
                Math.max(youngCollectionTime + youngCollectionTimeIncrement, oldCollectionTime + oldCollectionTimeIncrement),
                Integer.MAX_VALUE);

        final AtomicBoolean invoked = new AtomicBoolean();

        final JvmGcMonitorService.JvmMonitor monitor = new JvmGcMonitorService.JvmMonitor(Collections.emptyMap(), IGNORE) {

            @Override
            void onMonitorFailure(Exception e) {
            }

            @Override
            void onSlowGc(Threshold threshold, long seq, SlowGcEvent slowGcEvent) {
            }

            @Override
            void onGcOverhead(Threshold threshold, long total, long elapsed, long seq) {
            }

            @Override
            void checkGcOverhead(long current, long elapsed, long seq) {
                invoked.set(true);
                assertThat(current, equalTo((long)(youngCollectionTimeIncrement + oldCollectionTimeIncrement)));
                assertThat(elapsed, equalTo(expectedElapsed));
            }

            @Override
            JvmStats jvmStats() {
                return lastjvmStats;
            }
        };

        monitor.monitorGcOverhead(currentJvmStats, expectedElapsed);
        assertTrue(invoked.get());
    }

    private JvmStats.GarbageCollector collector(final String name, final int collectionCount, final int collectionTime) {
        final JvmStats.GarbageCollector gc = mock(JvmStats.GarbageCollector.class);
        when(gc.getName()).thenReturn(name);
        when(gc.getCollectionCount()).thenReturn((long)collectionCount);
        when(gc.getCollectionTime()).thenReturn(TimeValue.timeValueMillis(collectionTime));
        return gc;
    }

    public void testCheckGcOverhead() {
        final int debugThreshold = randomIntBetween(1, 98);
        final int infoThreshold = randomIntBetween(debugThreshold + 1, 99);
        final int warnThreshold = randomIntBetween(infoThreshold + 1, 100);
        final JvmGcMonitorService.GcOverheadThreshold gcOverheadThreshold =
            new JvmGcMonitorService.GcOverheadThreshold(warnThreshold, infoThreshold, debugThreshold);

        final JvmGcMonitorService.JvmMonitor.Threshold expectedThreshold;
        int fraction = 0;
        final long expectedCurrent;
        final long expectedElapsed;
        if (randomBoolean()) {
            expectedThreshold = randomFrom(JvmGcMonitorService.JvmMonitor.Threshold.values());
            switch (expectedThreshold) {
                case WARN:
                    fraction = randomIntBetween(warnThreshold, 100);
                    break;
                case INFO:
                    fraction = randomIntBetween(infoThreshold, warnThreshold - 1);
                    break;
                case DEBUG:
                    fraction = randomIntBetween(debugThreshold, infoThreshold - 1);
                    break;
            }
        } else {
            expectedThreshold = null;
            fraction = randomIntBetween(0, debugThreshold - 1);
        }

        expectedElapsed = 100 * randomIntBetween(1, 1000);
        expectedCurrent = fraction * expectedElapsed / 100;

        final AtomicBoolean invoked = new AtomicBoolean();
        final long expectedSeq = randomIntBetween(1, Integer.MAX_VALUE);

        final JvmGcMonitorService.JvmMonitor monitor = new JvmGcMonitorService.JvmMonitor(Collections.emptyMap(), gcOverheadThreshold) {

            @Override
            void onMonitorFailure(final Exception e) {
            }

            @Override
            void onSlowGc(Threshold threshold, long seq, SlowGcEvent slowGcEvent) {
            }

            @Override
            void onGcOverhead(final Threshold threshold, final long current, final long elapsed, final long seq) {
                invoked.set(true);
                assertThat(threshold, equalTo(expectedThreshold));
                assertThat(current, equalTo(expectedCurrent));
                assertThat(elapsed, equalTo(expectedElapsed));
                assertThat(seq, equalTo(expectedSeq));
            }

        };

        monitor.checkGcOverhead(expectedCurrent, expectedElapsed, expectedSeq);

        assertThat(invoked.get(), equalTo(expectedThreshold != null));
    }

}
