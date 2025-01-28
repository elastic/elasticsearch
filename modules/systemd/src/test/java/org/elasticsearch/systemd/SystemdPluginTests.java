/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.systemd;

import org.elasticsearch.Build;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.hamcrest.OptionalMatchers;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresentWith;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SystemdPluginTests extends ESTestCase {

    private Build.Type randomPackageBuildType = randomFrom(Build.Type.DEB, Build.Type.RPM);
    private Build.Type randomNonPackageBuildType = randomValueOtherThanMany(
        t -> t == Build.Type.DEB || t == Build.Type.RPM,
        () -> randomFrom(Build.Type.values())
    );

    final Scheduler.Cancellable extender = mock(Scheduler.Cancellable.class);
    final ThreadPool threadPool = mock(ThreadPool.class);

    {
        when(extender.cancel()).thenReturn(true);
        when(
            threadPool.scheduleWithFixedDelay(
                any(Runnable.class),
                eq(TimeValue.timeValueSeconds(15)),
                same(EsExecutors.DIRECT_EXECUTOR_SERVICE)
            )
        ).thenReturn(extender);
    }

    private void startPlugin(SystemdPlugin plugin) {
        Plugin.PluginServices services = mock(Plugin.PluginServices.class);
        when(services.threadPool()).thenReturn(threadPool);
        plugin.createComponents(services);
    }

    public void testIsEnabled() {
        final SystemdPlugin plugin = new SystemdPlugin(false, randomPackageBuildType, Boolean.TRUE.toString());
        startPlugin(plugin);
        assertTrue(plugin.isEnabled());
        assertNotNull(plugin.extender());
    }

    public void testIsNotPackageDistribution() {
        final SystemdPlugin plugin = new SystemdPlugin(false, randomNonPackageBuildType, Boolean.TRUE.toString());
        startPlugin(plugin);
        assertFalse(plugin.isEnabled());
        assertNull(plugin.extender());
    }

    public void testIsImplicitlyNotEnabled() {
        final SystemdPlugin plugin = new SystemdPlugin(false, randomPackageBuildType, null);
        startPlugin(plugin);
        assertFalse(plugin.isEnabled());
        assertNull(plugin.extender());
    }

    public void testIsExplicitlyNotEnabled() {
        final SystemdPlugin plugin = new SystemdPlugin(false, randomPackageBuildType, Boolean.FALSE.toString());
        startPlugin(plugin);
        assertFalse(plugin.isEnabled());
        assertNull(plugin.extender());
    }

    public void testInvalid() {
        final String esSDNotify = randomValueOtherThanMany(
            s -> Boolean.TRUE.toString().equals(s) || Boolean.FALSE.toString().equals(s),
            () -> randomAlphaOfLength(4)
        );
        final RuntimeException e = expectThrows(RuntimeException.class, () -> new SystemdPlugin(false, randomPackageBuildType, esSDNotify));
        assertThat(e, hasToString(containsString("ES_SD_NOTIFY set to unexpected value [" + esSDNotify + "]")));
    }

    public void testOnNodeStartedSuccess() {
        runTestOnNodeStarted(Boolean.TRUE.toString(), false, (maybe, plugin) -> {
            assertThat(maybe, OptionalMatchers.isEmpty());
            assertThat(plugin.invokedReady.get(), is(true));
            verify(plugin.extender()).cancel();
        });
    }

    public void testOnNodeStartedFailure() {
        runTestOnNodeStarted(Boolean.TRUE.toString(), true, (maybe, plugin) -> {
            assertThat(maybe, isPresentWith(allOf(instanceOf(RuntimeException.class), hasToString(containsString("notify ready failed")))));
            assertThat(plugin.invokedReady.get(), is(true));
        });
    }

    public void testOnNodeStartedNotEnabled() {
        runTestOnNodeStarted(Boolean.FALSE.toString(), randomBoolean(), (maybe, plugin) -> assertThat(maybe, OptionalMatchers.isEmpty()));
    }

    private void runTestOnNodeStarted(
        final String esSDNotify,
        final boolean invokeFailure,
        final BiConsumer<Optional<Exception>, TestSystemdPlugin> assertions
    ) {
        runTest(esSDNotify, invokeFailure, assertions, SystemdPlugin::onNodeStarted);
    }

    public void testCloseSuccess() {
        runTestClose(Boolean.TRUE.toString(), false, (maybe, plugin) -> {
            assertThat(maybe, OptionalMatchers.isEmpty());
            assertThat(plugin.invokedStopping.get(), is(true));
        });
    }

    public void testCloseFailure() {
        runTestClose(Boolean.TRUE.toString(), true, (maybe, plugin) -> {
            assertThat(maybe, OptionalMatchers.isEmpty());
            assertThat(plugin.invokedStopping.get(), is(true));
        });
    }

    public void testCloseNotEnabled() {
        runTestClose(Boolean.FALSE.toString(), randomBoolean(), (maybe, plugin) -> {
            assertThat(maybe, OptionalMatchers.isEmpty());
            assertThat(plugin.invokedStopping.get(), is(false));
        });
    }

    private void runTestClose(
        final String esSDNotify,
        boolean invokeFailure,
        final BiConsumer<Optional<Exception>, TestSystemdPlugin> assertions
    ) {
        runTest(esSDNotify, invokeFailure, assertions, SystemdPlugin::close);
    }

    private void runTest(
        final String esSDNotify,
        final boolean invokeReadyFailure,
        final BiConsumer<Optional<Exception>, TestSystemdPlugin> assertions,
        final CheckedConsumer<SystemdPlugin, IOException> invocation
    ) {
        final TestSystemdPlugin plugin = new TestSystemdPlugin(esSDNotify, invokeReadyFailure);
        startPlugin(plugin);
        if (Boolean.TRUE.toString().equals(esSDNotify)) {
            assertNotNull(plugin.extender());
        } else {
            assertNull(plugin.extender());
        }

        boolean success = false;
        try {
            invocation.accept(plugin);
            success = true;
        } catch (final Exception e) {
            assertions.accept(Optional.of(e), plugin);
        }
        if (success) {
            assertions.accept(Optional.empty(), plugin);
        }
    }

    class TestSystemdPlugin extends SystemdPlugin {
        final AtomicBoolean invokedReady = new AtomicBoolean();
        final AtomicBoolean invokedStopping = new AtomicBoolean();
        final boolean invokeReadyFailure;

        TestSystemdPlugin(String esSDNotify, boolean invokeFailure) {
            super(false, randomPackageBuildType, esSDNotify);
            this.invokeReadyFailure = invokeFailure;
        }

        @Override
        void notifyReady() {
            invokedReady.set(true);
            if (invokeReadyFailure) {
                throw new RuntimeException("notify ready failed");
            }
        }

        @Override
        void notifyStopping() {
            invokedStopping.set(true);
        }
    }
}
