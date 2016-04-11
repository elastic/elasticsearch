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

package org.elasticsearch.bootstrap;

import org.apache.lucene.util.Constants;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.not;

public class BootstrapCheckTests extends ESTestCase {

    public void testNonProductionMode() {
        // nothing should happen since we are in non-production mode
        BootstrapCheck.check(Settings.EMPTY);
    }

    public void testFileDescriptorLimits() {
        final boolean osX = randomBoolean(); // simulates OS X versus non-OS X
        final int limit = osX ? 10240 : 1 << 16;
        final AtomicLong maxFileDescriptorCount = new AtomicLong(randomIntBetween(1, limit - 1));
        final BootstrapCheck.FileDescriptorCheck check;
        if (osX) {
            check = new BootstrapCheck.OsXFileDescriptorCheck() {
                @Override
                long getMaxFileDescriptorCount() {
                    return maxFileDescriptorCount.get();
                }
            };
        } else {
            check = new BootstrapCheck.FileDescriptorCheck() {
                @Override
                long getMaxFileDescriptorCount() {
                    return maxFileDescriptorCount.get();
                }
            };
        }

        try {
            BootstrapCheck.check(true, Collections.singletonList(check));
            fail("should have failed due to max file descriptors too low");
        } catch (final RuntimeException e) {
            assertThat(e.getMessage(), containsString("max file descriptors"));
        }

        maxFileDescriptorCount.set(randomIntBetween(limit + 1, Integer.MAX_VALUE));

        BootstrapCheck.check(true, Collections.singletonList(check));

        // nothing should happen if current file descriptor count is
        // not available
        maxFileDescriptorCount.set(-1);
        BootstrapCheck.check(true, Collections.singletonList(check));
    }

    public void testFileDescriptorLimitsThrowsOnInvalidLimit() {
        final IllegalArgumentException e =
            expectThrows(
                IllegalArgumentException.class,
                () -> new BootstrapCheck.FileDescriptorCheck(-randomIntBetween(0, Integer.MAX_VALUE)));
        assertThat(e.getMessage(), containsString("limit must be positive but was"));
    }

    public void testMlockallCheck() {
        class MlockallCheckTestCase {

            private final boolean mlockallSet;
            private final boolean isMemoryLocked;
            private final boolean shouldFail;

            public MlockallCheckTestCase(final boolean mlockallSet, final boolean isMemoryLocked, final boolean shouldFail) {
                this.mlockallSet = mlockallSet;
                this.isMemoryLocked = isMemoryLocked;
                this.shouldFail = shouldFail;
            }

        }

        final List<MlockallCheckTestCase> testCases = new ArrayList<>();
        testCases.add(new MlockallCheckTestCase(true, true, false));
        testCases.add(new MlockallCheckTestCase(true, false, true));
        testCases.add(new MlockallCheckTestCase(false, true, false));
        testCases.add(new MlockallCheckTestCase(false, false, false));

        for (final MlockallCheckTestCase testCase : testCases) {
            final BootstrapCheck.MlockallCheck check = new BootstrapCheck.MlockallCheck(testCase.mlockallSet) {
                @Override
                boolean isMemoryLocked() {
                    return testCase.isMemoryLocked;
                }
            };

            if (testCase.shouldFail) {
                try {
                    BootstrapCheck.check(true, Collections.singletonList(check));
                    fail("should have failed due to memory not being locked");
                } catch (final RuntimeException e) {
                    assertThat(
                        e.getMessage(),
                        containsString("memory locking requested for elasticsearch process but memory is not locked"));
                }
            } else {
                // nothing should happen
                BootstrapCheck.check(true, Collections.singletonList(check));
            }
        }
    }

    public void testMaxNumberOfThreadsCheck() {
        final int limit = 1 << 11;
        final AtomicLong maxNumberOfThreads = new AtomicLong(randomIntBetween(1, limit - 1));
        final BootstrapCheck.MaxNumberOfThreadsCheck check = new BootstrapCheck.MaxNumberOfThreadsCheck() {
            @Override
            long getMaxNumberOfThreads() {
                return maxNumberOfThreads.get();
            }
        };

        try {
            BootstrapCheck.check(true, Collections.singletonList(check));
            fail("should have failed due to max number of threads too low");
        } catch (final RuntimeException e) {
            assertThat(e.getMessage(), containsString("max number of threads"));
        }

        maxNumberOfThreads.set(randomIntBetween(limit + 1, Integer.MAX_VALUE));

        BootstrapCheck.check(true, Collections.singletonList(check));

        // nothing should happen if current max number of threads is
        // not available
        maxNumberOfThreads.set(-1);
        BootstrapCheck.check(true, Collections.singletonList(check));
    }

    public void testMaxSizeVirtualMemory() {
        final long rlimInfinity = Constants.MAC_OS_X ? 9223372036854775807L : -1L;
        final AtomicLong maxSizeVirtualMemory = new AtomicLong(randomIntBetween(0, Integer.MAX_VALUE));
        final BootstrapCheck.MaxSizeVirtualMemoryCheck check = new BootstrapCheck.MaxSizeVirtualMemoryCheck() {
            @Override
            long getMaxSizeVirtualMemory() {
                return maxSizeVirtualMemory.get();
            }

            @Override
            long getRlimInfinity() {
                return rlimInfinity;
            }
        };

        try {
            BootstrapCheck.check(true, Collections.singletonList(check));
            fail("should have failed due to max size virtual memory too low");
        } catch (final RuntimeException e) {
            assertThat(e.getMessage(), containsString("max size virtual memory"));
        }

        maxSizeVirtualMemory.set(rlimInfinity);

        BootstrapCheck.check(true, Collections.singletonList(check));

        // nothing should happen if max size virtual memory is not
        // available
        maxSizeVirtualMemory.set(Long.MIN_VALUE);
        BootstrapCheck.check(true, Collections.singletonList(check));
    }

    public void testEnforceLimits() {
        final Set<Setting> enforceSettings = BootstrapCheck.enforceSettings();
        final Setting setting = randomFrom(Arrays.asList(enforceSettings.toArray(new Setting[enforceSettings.size()])));
        final Settings settings = Settings.builder().put(setting.getKey(), randomAsciiOfLength(8)).build();
        assertTrue(BootstrapCheck.enforceLimits(settings));
    }

    public void testMinMasterNodes() {
        boolean isSet = randomBoolean();
        BootstrapCheck.Check check = new BootstrapCheck.MinMasterNodesCheck(isSet);
        assertThat(check.check(), not(equalTo(isSet)));
        List<BootstrapCheck.Check> defaultChecks = BootstrapCheck.checks(Settings.EMPTY);

        expectThrows(RuntimeException.class, () -> BootstrapCheck.check(true, defaultChecks));
    }
}
