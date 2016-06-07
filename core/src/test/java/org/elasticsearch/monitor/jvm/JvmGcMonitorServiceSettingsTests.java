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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;

public class JvmGcMonitorServiceSettingsTests extends ESTestCase {

    public void testEmptySettingsAreOkay() throws InterruptedException {
        AtomicBoolean scheduled = new AtomicBoolean();
        execute(Settings.EMPTY, (command, interval) -> { scheduled.set(true); return null; }, () -> assertTrue(scheduled.get()));
    }

    public void testDisabledSetting() throws InterruptedException {
        Settings settings = Settings.builder().put("monitor.jvm.gc.enabled", "false").build();
        AtomicBoolean scheduled = new AtomicBoolean();
        execute(settings, (command, interval) -> { scheduled.set(true); return null; }, () -> assertFalse(scheduled.get()));
    }

    public void testNegativeSetting() throws InterruptedException {
        String collector = randomAsciiOfLength(5);
        Settings settings = Settings.builder().put("monitor.jvm.gc.collector." + collector + ".warn", "-" + randomTimeValue()).build();
        execute(settings, (command, interval) -> null, t -> {
            assertThat(t, instanceOf(IllegalArgumentException.class));
            assertThat(t.getMessage(), allOf(containsString("invalid gc_threshold"), containsString("for [monitor.jvm.gc.collector." + collector + ".")));
        }, true, null);
    }

    public void testMissingSetting() throws InterruptedException {
        String collector = randomAsciiOfLength(5);
        Set<AbstractMap.SimpleEntry<String, String>> entries = new HashSet<>();
        entries.add(new AbstractMap.SimpleEntry<>("monitor.jvm.gc.collector." + collector + ".warn", randomPositiveTimeValue()));
        entries.add(new AbstractMap.SimpleEntry<>("monitor.jvm.gc.collector." + collector + ".info", randomPositiveTimeValue()));
        entries.add(new AbstractMap.SimpleEntry<>("monitor.jvm.gc.collector." + collector + ".debug", randomPositiveTimeValue()));
        Settings.Builder builder = Settings.builder();

        // drop a random setting or two
        for (@SuppressWarnings("unchecked") AbstractMap.SimpleEntry<String, String> entry : randomSubsetOf(randomIntBetween(1, 2), entries.toArray(new AbstractMap.SimpleEntry[0]))) {
            builder.put(entry.getKey(), entry.getValue());
        }

        // we should get an exception that a setting is missing
        execute(builder.build(), (command, interval) -> null, t -> {
            assertThat(t, instanceOf(IllegalArgumentException.class));
            assertThat(t.getMessage(), containsString("missing gc_threshold for [monitor.jvm.gc.collector." + collector + "."));
        }, true, null);
    }

    public void testIllegalOverheadSettings() throws InterruptedException {
        for (final String threshold : new String[] { "warn", "info", "debug" }) {
            final Settings.Builder builder = Settings.builder();
            builder.put("monitor.jvm.gc.overhead." + threshold, randomIntBetween(Integer.MIN_VALUE, -1));
            execute(builder.build(), (command, interval) -> null, t -> {
                assertThat(t, instanceOf(IllegalArgumentException.class));
                assertThat(t.getMessage(), containsString("setting [monitor.jvm.gc.overhead." + threshold + "] must be >= 0"));
            }, true, null);
        }

        for (final String threshold : new String[] { "warn", "info", "debug" }) {
            final Settings.Builder builder = Settings.builder();
            builder.put("monitor.jvm.gc.overhead." + threshold, randomIntBetween(100 + 1, Integer.MAX_VALUE));
            execute(builder.build(), (command, interval) -> null, t -> {
                assertThat(t, instanceOf(IllegalArgumentException.class));
                assertThat(t.getMessage(), containsString("setting [monitor.jvm.gc.overhead." + threshold + "] must be <= 100"));
            }, true, null);
        }

        final Settings.Builder infoWarnOutOfOrderBuilder = Settings.builder();
        final int info = randomIntBetween(2, 98);
        infoWarnOutOfOrderBuilder.put("monitor.jvm.gc.overhead.info", info);
        final int warn = randomIntBetween(1, info - 1);
        infoWarnOutOfOrderBuilder.put("monitor.jvm.gc.overhead.warn", warn);
        execute(infoWarnOutOfOrderBuilder.build(), (command, interval) -> null, t -> {
            assertThat(t, instanceOf(IllegalArgumentException.class));
            assertThat(t.getMessage(), containsString("[monitor.jvm.gc.overhead.warn] must be greater than [monitor.jvm.gc.overhead.info] [" + info + "] but was [" + warn + "]"));
        }, true, null);

        final Settings.Builder debugInfoOutOfOrderBuilder = Settings.builder();
        debugInfoOutOfOrderBuilder.put("monitor.jvm.gc.overhead.info", info);
        final int debug = randomIntBetween(info + 1, 99);
        debugInfoOutOfOrderBuilder.put("monitor.jvm.gc.overhead.debug", debug);
        debugInfoOutOfOrderBuilder.put("monitor.jvm.gc.overhead.warn", randomIntBetween(debug + 1, 100)); // or the test will fail for the wrong reason
        execute(debugInfoOutOfOrderBuilder.build(), (command, interval) -> null, t -> {
            assertThat(t, instanceOf(IllegalArgumentException.class));
            assertThat(t.getMessage(), containsString("[monitor.jvm.gc.overhead.info] must be greater than [monitor.jvm.gc.overhead.debug] [" + debug + "] but was [" + info + "]"));
        }, true, null);
    }

    private static void execute(Settings settings, BiFunction<Runnable, TimeValue, ScheduledFuture<?>> scheduler, Runnable asserts) throws InterruptedException {
        execute(settings, scheduler, null, false, asserts);
    }

    private static void execute(Settings settings, BiFunction<Runnable, TimeValue, ScheduledFuture<?>> scheduler, Consumer<Throwable> consumer, boolean constructionShouldFail, Runnable asserts) throws InterruptedException {
        assert constructionShouldFail == (consumer != null);
        assert constructionShouldFail == (asserts == null);
        ThreadPool threadPool = null;
        try {
            threadPool = new TestThreadPool(JvmGcMonitorServiceSettingsTests.class.getCanonicalName()) {
                @Override
                public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, TimeValue interval) {
                    return scheduler.apply(command, interval);
                }
            };
            try {
                JvmGcMonitorService service = new JvmGcMonitorService(settings, threadPool);
                if (constructionShouldFail) {
                    fail("construction of jvm gc service should have failed");
                }
                service.doStart();
                asserts.run();
                service.doStop();
            } catch (Throwable t) {
                consumer.accept(t);
            }
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

}
