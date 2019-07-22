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

import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.hamcrest.OptionalMatchers;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;

public class SystemdPluginTests extends ESTestCase {

    public void testIsEnabled() {
        final SystemdPlugin plugin = new SystemdPlugin() {

            @Override
            void assertIsPackage() {

            }

            @Override
            boolean isLinux() {
                return true;
            }

            @Override
            String getEsSDNotify() {
                return Boolean.TRUE.toString();
            }

        };
        assertTrue(plugin.isEnabled());
    }

    public void testIsNotLinux() {
        final SystemdPlugin plugin = new SystemdPlugin() {

            @Override
            void assertIsPackage() {

            }

            @Override
            boolean isLinux() {
                return false;
            }

            @Override
            String getEsSDNotify() {
                return Boolean.TRUE.toString();
            }

        };
        assertFalse(plugin.isEnabled());
    }

    public void testIsImplicitlyNotEnabled() {
        final SystemdPlugin plugin = new SystemdPlugin() {

            @Override
            void assertIsPackage() {

            }

            @Override
            boolean isLinux() {
                return true;
            }

            @Override
            String getEsSDNotify() {
                return null;
            }

        };
        assertFalse(plugin.isEnabled());
    }

    public void testIsExplicitlyNotEnabled() {
        final SystemdPlugin plugin = new SystemdPlugin() {

            @Override
            void assertIsPackage() {

            }

            @Override
            boolean isLinux() {
                return true;
            }

            @Override
            String getEsSDNotify() {
                return Boolean.FALSE.toString();
            }

        };
        assertFalse(plugin.isEnabled());
    }

    public void testInvalid() {
        final String esSDNotify = randomValueOtherThanMany(
            s -> Boolean.TRUE.toString().equals(s) || Boolean.FALSE.toString().equals(s),
            () -> randomAlphaOfLength(4));
        final RuntimeException e = expectThrows(RuntimeException.class,
            () -> new SystemdPlugin() {

                @Override
                void assertIsPackage() {

                }

                @Override
                boolean isLinux() {
                    return true;
                }

                @Override
                String getEsSDNotify() {
                    return esSDNotify;
                }

            });
        assertThat(e, hasToString(containsString("ES_SD_NOTIFY set to unexpected value [" + esSDNotify + "]")));
    }

    public void testOnNodeStartedSuccess() {
        runTestOnNodeStarted(
            true,
            Boolean.TRUE.toString(),
            randomIntBetween(0, Integer.MAX_VALUE),
            maybe -> assertThat(maybe, OptionalMatchers.isEmpty()));
    }

    public void testOnNodeStartedFailure() {
        final int rc = randomIntBetween(Integer.MIN_VALUE, -1);
        runTestOnNodeStarted(
            true,
            Boolean.TRUE.toString(),
            rc,
            maybe -> {
                assertThat(maybe, OptionalMatchers.isPresent());
                // noinspection OptionalGetWithoutIsPresent
                assertThat(maybe.get(), instanceOf(RuntimeException.class));
                assertThat(maybe.get(), hasToString(containsString("sd_notify returned error [" + rc + "]")));
            });
    }

    public void testOnNodeStartedNotEnabled() {
        runTestOnNodeStarted(
            true,
            Boolean.FALSE.toString(),
            randomInt(),
            maybe -> assertThat(maybe, OptionalMatchers.isEmpty()));
    }

    private void runTestOnNodeStarted(
        final boolean isLinux,
        final String esSDNotify,
        final int rc,
        final Consumer<Optional<Exception>> assertions) {
        runTest(isLinux, esSDNotify, rc, assertions, SystemdPlugin::onNodeStarted, "READY=1");
    }

    public void testCloseSuccess() {
        runTestClose(
            true,
            Boolean.TRUE.toString(),
            randomIntBetween(1, Integer.MAX_VALUE),
            maybe -> assertThat(maybe, OptionalMatchers.isEmpty()));
    }

    public void testCloseFailure() {
        runTestClose(
            true,
            Boolean.TRUE.toString(),
            randomIntBetween(Integer.MIN_VALUE, -1),
            maybe -> assertThat(maybe, OptionalMatchers.isEmpty()));
    }

    public void testCloseNotEnabled() {
        runTestClose(
            true,
            Boolean.FALSE.toString(),
            randomInt(),
            maybe -> assertThat(maybe, OptionalMatchers.isEmpty()));
    }

    private void runTestClose(
        final boolean isLinux,
        final String esSDNotify,
        final int rc,
        final Consumer<Optional<Exception>> assertions) {
        runTest(isLinux, esSDNotify, rc, assertions, SystemdPlugin::close, "STOPPING=1");
    }

    private void runTest(
        final boolean isLinux,
        final String esSDNotify,
        final int rc,
        final Consumer<Optional<Exception>> assertions,
        final CheckedConsumer<SystemdPlugin, IOException> invocation,
        final String expectedState) {
        final AtomicBoolean invoked = new AtomicBoolean();
        final AtomicInteger invokedUnsetEnvironment = new AtomicInteger();
        final AtomicReference<String> invokedState = new AtomicReference<>();
        final SystemdPlugin plugin = new SystemdPlugin() {

            @Override
            void assertIsPackage() {

            }

            @Override
            boolean isLinux() {
                return isLinux;
            }

            @Override
            String getEsSDNotify() {
                return esSDNotify;
            }

            @Override
            int sd_notify(final int unset_environment, final String state) {
                invoked.set(true);
                invokedUnsetEnvironment.set(unset_environment);
                invokedState.set(state);
                return rc;
            }

        };

        boolean success = false;
        try {
            invocation.accept(plugin);
            success = true;
        } catch (final Exception e) {
            assertions.accept(Optional.of(e));
        }
        if (success) {
            assertions.accept(Optional.empty());
        }
        if (isLinux && Boolean.TRUE.toString().equals(esSDNotify)) {
            assertTrue(invoked.get());
            assertThat(invokedUnsetEnvironment.get(), equalTo(0));
            assertThat(invokedState.get(), equalTo(expectedState));
        } else {
            assertFalse(invoked.get());
        }
    }

}
