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

package org.elasticsearch.systemd;

import org.elasticsearch.Build;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.hamcrest.OptionalMatchers;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SystemdPluginTests extends ESTestCase {

    private Build.Type randomPackageBuildType = randomFrom(Build.Type.DEB, Build.Type.RPM);
    private Build.Type randomNonPackageBuildType =
        randomValueOtherThanMany(t -> t == Build.Type.DEB || t == Build.Type.RPM, () -> randomFrom(Build.Type.values()));

    final Scheduler.Cancellable extender = mock(Scheduler.Cancellable.class);
    final ThreadPool threadPool = mock(ThreadPool.class);

    {
        when(extender.cancel()).thenReturn(true);
        when(threadPool.scheduleWithFixedDelay(any(Runnable.class), eq(TimeValue.timeValueSeconds(15)), eq(ThreadPool.Names.SAME)))
            .thenReturn(extender);
    }

    public void testIsEnabled() {
        final SystemdPlugin plugin = new SystemdPlugin(false, randomPackageBuildType, Boolean.TRUE.toString());
        plugin.createComponents(null, null, threadPool, null, null, null, null, null, null, null, null);
        assertTrue(plugin.isEnabled());
        assertNotNull(plugin.extender());
    }

    public void testIsNotPackageDistribution() {
        final SystemdPlugin plugin = new SystemdPlugin(false, randomNonPackageBuildType, Boolean.TRUE.toString());
        plugin.createComponents(null, null, threadPool, null, null, null, null, null, null, null, null);
        assertFalse(plugin.isEnabled());
        assertNull(plugin.extender());
    }

    public void testIsImplicitlyNotEnabled() {
        final SystemdPlugin plugin = new SystemdPlugin(false, randomPackageBuildType, null);
        plugin.createComponents(null, null, threadPool, null, null, null, null, null, null, null, null);
        assertFalse(plugin.isEnabled());
        assertNull(plugin.extender());
    }

    public void testIsExplicitlyNotEnabled() {
        final SystemdPlugin plugin = new SystemdPlugin(false, randomPackageBuildType, Boolean.FALSE.toString());
        plugin.createComponents(null, null, threadPool, null, null, null, null, null, null, null, null);
        assertFalse(plugin.isEnabled());
        assertNull(plugin.extender());
    }

    public void testInvalid() {
        final String esSDNotify = randomValueOtherThanMany(
            s -> Boolean.TRUE.toString().equals(s) || Boolean.FALSE.toString().equals(s),
            () -> randomAlphaOfLength(4));
        final RuntimeException e = expectThrows(RuntimeException.class,
            () -> new SystemdPlugin(false, randomPackageBuildType, esSDNotify));
        assertThat(e, hasToString(containsString("ES_SD_NOTIFY set to unexpected value [" + esSDNotify + "]")));
    }

    public void testOnNodeStartedSuccess() {
        runTestOnNodeStarted(
            Boolean.TRUE.toString(),
            randomIntBetween(0, Integer.MAX_VALUE),
            (maybe, plugin) -> {
                assertThat(maybe, OptionalMatchers.isEmpty());
                verify(plugin.extender()).cancel();
            });
    }

    public void testOnNodeStartedFailure() {
        final int rc = randomIntBetween(Integer.MIN_VALUE, -1);
        runTestOnNodeStarted(
            Boolean.TRUE.toString(),
            rc,
            (maybe, plugin) -> {
                assertThat(maybe, OptionalMatchers.isPresent());
                // noinspection OptionalGetWithoutIsPresent
                assertThat(maybe.get(), instanceOf(RuntimeException.class));
                assertThat(maybe.get(), hasToString(containsString("sd_notify returned error [" + rc + "]")));
            });
    }

    public void testOnNodeStartedNotEnabled() {
        runTestOnNodeStarted(
            Boolean.FALSE.toString(),
            randomInt(),
            (maybe, plugin) -> assertThat(maybe, OptionalMatchers.isEmpty()));
    }

    private void runTestOnNodeStarted(
        final String esSDNotify,
        final int rc,
        final BiConsumer<Optional<Exception>, SystemdPlugin> assertions) {
        runTest(esSDNotify, rc, assertions, SystemdPlugin::onNodeStarted, "READY=1");
    }

    public void testCloseSuccess() {
        runTestClose(
            Boolean.TRUE.toString(),
            randomIntBetween(1, Integer.MAX_VALUE),
            (maybe, plugin) -> assertThat(maybe, OptionalMatchers.isEmpty()));
    }

    public void testCloseFailure() {
        runTestClose(
            Boolean.TRUE.toString(),
            randomIntBetween(Integer.MIN_VALUE, -1),
            (maybe, plugin) -> assertThat(maybe, OptionalMatchers.isEmpty()));
    }

    public void testCloseNotEnabled() {
        runTestClose(
            Boolean.FALSE.toString(),
            randomInt(),
            (maybe, plugin) -> assertThat(maybe, OptionalMatchers.isEmpty()));
    }

    private void runTestClose(
        final String esSDNotify,
        final int rc,
        final BiConsumer<Optional<Exception>, SystemdPlugin> assertions) {
        runTest(esSDNotify, rc, assertions, SystemdPlugin::close, "STOPPING=1");
    }

    private void runTest(
        final String esSDNotify,
        final int rc,
        final BiConsumer<Optional<Exception>, SystemdPlugin> assertions,
        final CheckedConsumer<SystemdPlugin, IOException> invocation,
        final String expectedState) {
        final AtomicBoolean invoked = new AtomicBoolean();
        final AtomicInteger invokedUnsetEnvironment = new AtomicInteger();
        final AtomicReference<String> invokedState = new AtomicReference<>();
        final SystemdPlugin plugin = new SystemdPlugin(false, randomPackageBuildType, esSDNotify) {

            @Override
            int sd_notify(final int unset_environment, final String state) {
                invoked.set(true);
                invokedUnsetEnvironment.set(unset_environment);
                invokedState.set(state);
                return rc;
            }

        };
        plugin.createComponents(null, null, threadPool, null, null, null, null, null, null, null, null);
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
        if (Boolean.TRUE.toString().equals(esSDNotify)) {
            assertTrue(invoked.get());
            assertThat(invokedUnsetEnvironment.get(), equalTo(0));
            assertThat(invokedState.get(), equalTo(expectedState));
        } else {
            assertFalse(invoked.get());
        }
    }

}
