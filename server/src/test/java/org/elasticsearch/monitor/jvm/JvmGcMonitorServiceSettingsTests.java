/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor.jvm;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler.Cancellable;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class JvmGcMonitorServiceSettingsTests extends ESTestCase {

    public void testEmptySettingsAreOkay() throws InterruptedException {
        AtomicBoolean scheduled = new AtomicBoolean();
        execute(Settings.EMPTY, (command, interval, executor) -> {
            scheduled.set(true);
            return new MockCancellable();
        }, () -> assertTrue(scheduled.get()));
    }

    public void testDisabledSetting() throws InterruptedException {
        Settings settings = Settings.builder().put("monitor.jvm.gc.enabled", "false").build();
        AtomicBoolean scheduled = new AtomicBoolean();
        execute(settings, (command, interval, executor) -> {
            scheduled.set(true);
            return new MockCancellable();
        }, () -> assertFalse(scheduled.get()));
    }

    public void testNegativeSetting() throws InterruptedException {
        String collector = randomAlphaOfLength(5);
        final String timeValue = "-" + randomTimeValue(2, 1000); // -1 is handled separately
        Settings settings = Settings.builder().put("monitor.jvm.gc.collector." + collector + ".warn", timeValue).build();
        execute(settings, (command, interval, name) -> null, e -> {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(
                e.getMessage(),
                equalTo(
                    "failed to parse setting [monitor.jvm.gc.collector."
                        + collector
                        + ".warn] "
                        + "with value ["
                        + timeValue
                        + "] as a time value"
                )
            );
        }, true, null);
    }

    public void testNegativeOneSetting() throws InterruptedException {
        String collector = randomAlphaOfLength(5);
        final String timeValue = "-1" + randomFrom("", "d", "h", "m", "s", "ms", "nanos");
        Settings settings = Settings.builder().put("monitor.jvm.gc.collector." + collector + ".warn", timeValue).build();
        execute(settings, (command, interval, name) -> null, e -> {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(
                e.getMessage(),
                equalTo(
                    "invalid gc_threshold [monitor.jvm.gc.collector."
                        + collector
                        + ".warn] "
                        + "value ["
                        + timeValue
                        + "]: value cannot be negative"
                )
            );
        }, true, null);
    }

    public void testMissingSetting() throws InterruptedException {
        String collector = randomAlphaOfLength(5);
        Set<AbstractMap.SimpleEntry<String, String>> entries = new HashSet<>();
        entries.add(new AbstractMap.SimpleEntry<>("monitor.jvm.gc.collector." + collector + ".warn", randomPositiveTimeValue()));
        entries.add(new AbstractMap.SimpleEntry<>("monitor.jvm.gc.collector." + collector + ".info", randomPositiveTimeValue()));
        entries.add(new AbstractMap.SimpleEntry<>("monitor.jvm.gc.collector." + collector + ".debug", randomPositiveTimeValue()));
        Settings.Builder builder = Settings.builder();

        // drop a random setting or two
        for (AbstractMap.SimpleEntry<String, String> entry : randomSubsetOf(randomIntBetween(1, 2), entries)) {
            builder.put(entry.getKey(), entry.getValue());
        }

        // we should get an exception that a setting is missing
        execute(builder.build(), (command, interval, name) -> null, e -> {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(e.getMessage(), containsString("missing gc_threshold for [monitor.jvm.gc.collector." + collector + "."));
        }, true, null);
    }

    public void testIllegalOverheadSettings() throws InterruptedException {
        for (final String threshold : new String[] { "warn", "info", "debug" }) {
            final Settings.Builder builder = Settings.builder();
            builder.put("monitor.jvm.gc.overhead." + threshold, randomIntBetween(Integer.MIN_VALUE, -1));
            execute(builder.build(), (command, interval, name) -> null, e -> {
                assertThat(e, instanceOf(IllegalArgumentException.class));
                assertThat(e.getMessage(), containsString("setting [monitor.jvm.gc.overhead." + threshold + "] must be >= 0"));
            }, true, null);
        }

        for (final String threshold : new String[] { "warn", "info", "debug" }) {
            final Settings.Builder builder = Settings.builder();
            builder.put("monitor.jvm.gc.overhead." + threshold, randomIntBetween(100 + 1, Integer.MAX_VALUE));
            execute(builder.build(), (command, interval, name) -> null, e -> {
                assertThat(e, instanceOf(IllegalArgumentException.class));
                assertThat(e.getMessage(), containsString("setting [monitor.jvm.gc.overhead." + threshold + "] must be <= 100"));
            }, true, null);
        }

        final Settings.Builder infoWarnOutOfOrderBuilder = Settings.builder();
        final int info = randomIntBetween(2, 98);
        infoWarnOutOfOrderBuilder.put("monitor.jvm.gc.overhead.info", info);
        final int warn = randomIntBetween(1, info - 1);
        infoWarnOutOfOrderBuilder.put("monitor.jvm.gc.overhead.warn", warn);
        execute(infoWarnOutOfOrderBuilder.build(), (command, interval, name) -> null, e -> {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(
                e.getMessage(),
                containsString(
                    "[monitor.jvm.gc.overhead.warn] must be greater than "
                        + "[monitor.jvm.gc.overhead.info] ["
                        + info
                        + "] but was ["
                        + warn
                        + "]"
                )
            );
        }, true, null);

        final Settings.Builder debugInfoOutOfOrderBuilder = Settings.builder();
        debugInfoOutOfOrderBuilder.put("monitor.jvm.gc.overhead.info", info);
        final int debug = randomIntBetween(info + 1, 99);
        debugInfoOutOfOrderBuilder.put("monitor.jvm.gc.overhead.debug", debug);
        debugInfoOutOfOrderBuilder.put("monitor.jvm.gc.overhead.warn", randomIntBetween(debug + 1, 100)); // or the test will fail for the
                                                                                                          // wrong reason
        execute(debugInfoOutOfOrderBuilder.build(), (command, interval, name) -> null, e -> {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(
                e.getMessage(),
                containsString(
                    "[monitor.jvm.gc.overhead.info] must be greater than "
                        + "[monitor.jvm.gc.overhead.debug] ["
                        + debug
                        + "] but was ["
                        + info
                        + "]"
                )
            );
        }, true, null);
    }

    private static void execute(Settings settings, TriFunction<Runnable, TimeValue, Executor, Cancellable> scheduler, Runnable asserts)
        throws InterruptedException {
        execute(settings, scheduler, null, false, asserts);
    }

    private static void execute(
        Settings settings,
        TriFunction<Runnable, TimeValue, Executor, Cancellable> scheduler,
        Consumer<Throwable> consumer,
        boolean constructionShouldFail,
        Runnable asserts
    ) throws InterruptedException {
        assert constructionShouldFail == (consumer != null);
        assert constructionShouldFail == (asserts == null);
        ThreadPool threadPool = null;
        try {
            threadPool = new TestThreadPool(JvmGcMonitorServiceSettingsTests.class.getCanonicalName()) {
                @Override
                public Cancellable scheduleWithFixedDelay(Runnable command, TimeValue interval, Executor executor) {
                    return scheduler.apply(command, interval, executor);
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
            } catch (Exception t) {
                consumer.accept(t);
            }
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    interface TriFunction<S, T, U, R> {
        R apply(S s, T t, U u);
    }

    private static class MockCancellable implements Cancellable {

        @Override
        public boolean cancel() {
            return true;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }
    }
}
