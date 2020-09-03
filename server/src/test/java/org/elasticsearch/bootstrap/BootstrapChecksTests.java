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

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.elasticsearch.cluster.coordination.ClusterBootstrapService;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.SettingsBasedSeedHostsProvider;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.test.AbstractBootstrapCheckTestCase;
import org.hamcrest.Matcher;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.discovery.DiscoveryModule.ZEN2_DISCOVERY_TYPE;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class BootstrapChecksTests extends AbstractBootstrapCheckTestCase {

    public void testNonProductionMode() throws NodeValidationException {
        // nothing should happen since we are in non-production mode
        final List<TransportAddress> transportAddresses = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 8); i++) {
            TransportAddress localTransportAddress = new TransportAddress(InetAddress.getLoopbackAddress(), i);
            transportAddresses.add(localTransportAddress);
        }

        TransportAddress publishAddress = new TransportAddress(InetAddress.getLoopbackAddress(), 0);
        BoundTransportAddress boundTransportAddress = mock(BoundTransportAddress.class);
        when(boundTransportAddress.boundAddresses()).thenReturn(transportAddresses.toArray(new TransportAddress[0]));
        when(boundTransportAddress.publishAddress()).thenReturn(publishAddress);
        BootstrapChecks.check(emptyContext, boundTransportAddress, Collections.emptyList());
    }

    public void testNoLogMessageInNonProductionMode() throws NodeValidationException {
        final Logger logger = mock(Logger.class);
        BootstrapChecks.check(emptyContext, false, Collections.emptyList(), logger);
        verifyNoMoreInteractions(logger);
    }

    public void testLogMessageInProductionMode() throws NodeValidationException {
        final Logger logger = mock(Logger.class);
        BootstrapChecks.check(emptyContext, true, Collections.emptyList(), logger);
        verify(logger).info("bound or publishing to a non-loopback address, enforcing bootstrap checks");
        verifyNoMoreInteractions(logger);
    }

    public void testEnforceLimitsWhenBoundToNonLocalAddress() {
        final List<TransportAddress> transportAddresses = new ArrayList<>();
        final TransportAddress nonLocalTransportAddress = buildNewFakeTransportAddress();
        transportAddresses.add(nonLocalTransportAddress);

        for (int i = 0; i < randomIntBetween(0, 7); i++) {
            final TransportAddress randomTransportAddress = randomBoolean() ? buildNewFakeTransportAddress() :
                new TransportAddress(InetAddress.getLoopbackAddress(), i);
            transportAddresses.add(randomTransportAddress);
        }

        final TransportAddress publishAddress = randomBoolean() ? buildNewFakeTransportAddress() :
            new TransportAddress(InetAddress.getLoopbackAddress(), 0);

        final BoundTransportAddress boundTransportAddress = mock(BoundTransportAddress.class);
        Collections.shuffle(transportAddresses, random());
        when(boundTransportAddress.boundAddresses()).thenReturn(transportAddresses.toArray(new TransportAddress[0]));
        when(boundTransportAddress.publishAddress()).thenReturn(publishAddress);

        final String discoveryType = randomFrom(ZEN2_DISCOVERY_TYPE, "single-node");

        assertEquals(BootstrapChecks.enforceLimits(boundTransportAddress, discoveryType), !"single-node".equals(discoveryType));
    }

    public void testEnforceLimitsWhenPublishingToNonLocalAddress() {
        final List<TransportAddress> transportAddresses = new ArrayList<>();

        for (int i = 0; i < randomIntBetween(1, 8); i++) {
            final TransportAddress randomTransportAddress = buildNewFakeTransportAddress();
            transportAddresses.add(randomTransportAddress);
        }

        final TransportAddress publishAddress = new TransportAddress(InetAddress.getLoopbackAddress(), 0);
        final BoundTransportAddress boundTransportAddress = mock(BoundTransportAddress.class);
        when(boundTransportAddress.boundAddresses()).thenReturn(transportAddresses.toArray(new TransportAddress[0]));
        when(boundTransportAddress.publishAddress()).thenReturn(publishAddress);

        final String discoveryType = randomFrom(ZEN2_DISCOVERY_TYPE, "single-node");

        assertEquals(BootstrapChecks.enforceLimits(boundTransportAddress, discoveryType), !"single-node".equals(discoveryType));
    }

    public void testExceptionAggregation() {
        final List<BootstrapCheck> checks = Arrays.asList(
                context -> BootstrapCheck.BootstrapCheckResult.failure("first"),
                context -> BootstrapCheck.BootstrapCheckResult.failure("second"));

        final NodeValidationException e =
                expectThrows(NodeValidationException.class,
                    () -> BootstrapChecks.check(emptyContext, true, checks));
        assertThat(e, hasToString(allOf(containsString("bootstrap checks failed"), containsString("first"), containsString("second"))));
        final Throwable[] suppressed = e.getSuppressed();
        assertThat(suppressed.length, equalTo(2));
        assertThat(suppressed[0], instanceOf(IllegalStateException.class));
        assertThat(suppressed[0], hasToString(containsString("first")));
        assertThat(suppressed[1], instanceOf(IllegalStateException.class));
        assertThat(suppressed[1], hasToString(containsString("second")));
    }

    public void testHeapSizeCheck() throws NodeValidationException {
        final int initial = randomIntBetween(0, Integer.MAX_VALUE - 1);
        final int max = randomIntBetween(initial + 1, Integer.MAX_VALUE);
        final AtomicLong initialHeapSize = new AtomicLong(initial);
        final AtomicLong maxHeapSize = new AtomicLong(max);
        final boolean isMemoryLocked = randomBoolean();

        final BootstrapChecks.HeapSizeCheck check = new BootstrapChecks.HeapSizeCheck() {

            @Override
            long getInitialHeapSize() {
                return initialHeapSize.get();
            }

            @Override
            long getMaxHeapSize() {
                return maxHeapSize.get();
            }

            @Override
            boolean isMemoryLocked() {
                return isMemoryLocked;
            }

        };

        final NodeValidationException e =
                expectThrows(
                        NodeValidationException.class,
                        () -> BootstrapChecks.check(emptyContext, true, Collections.singletonList(check)));
        assertThat(
                e.getMessage(),
                containsString("initial heap size [" + initialHeapSize.get() + "] " +
                        "not equal to maximum heap size [" + maxHeapSize.get() + "]"));
        final String memoryLockingMessage = "and prevents memory locking from locking the entire heap";
        final Matcher<String> memoryLockingMatcher;
        if (isMemoryLocked) {
            memoryLockingMatcher = containsString(memoryLockingMessage);
        } else {
            memoryLockingMatcher = not(containsString(memoryLockingMessage));
        }
        assertThat(e.getMessage(), memoryLockingMatcher);

        initialHeapSize.set(maxHeapSize.get());

        BootstrapChecks.check(emptyContext, true, Collections.singletonList(check));

        // nothing should happen if the initial heap size or the max
        // heap size is not available
        if (randomBoolean()) {
            initialHeapSize.set(0);
        } else {
            maxHeapSize.set(0);
        }
        BootstrapChecks.check(emptyContext, true, Collections.singletonList(check));
    }

    public void testFileDescriptorLimits() throws NodeValidationException {
        final boolean osX = randomBoolean(); // simulates OS X versus non-OS X
        final int limit = osX ? 10240 : 65535;
        final AtomicLong maxFileDescriptorCount = new AtomicLong(randomIntBetween(1, limit - 1));
        final BootstrapChecks.FileDescriptorCheck check;
        if (osX) {
            check = new BootstrapChecks.OsXFileDescriptorCheck() {
                @Override
                long getMaxFileDescriptorCount() {
                    return maxFileDescriptorCount.get();
                }
            };
        } else {
            check = new BootstrapChecks.FileDescriptorCheck() {
                @Override
                long getMaxFileDescriptorCount() {
                    return maxFileDescriptorCount.get();
                }
            };
        }

        final NodeValidationException e =
                expectThrows(NodeValidationException.class,
                        () -> BootstrapChecks.check(emptyContext, true, Collections.singletonList(check)));
        assertThat(e.getMessage(), containsString("max file descriptors"));

        maxFileDescriptorCount.set(randomIntBetween(limit + 1, Integer.MAX_VALUE));

        BootstrapChecks.check(emptyContext, true, Collections.singletonList(check));

        // nothing should happen if current file descriptor count is
        // not available
        maxFileDescriptorCount.set(-1);
        BootstrapChecks.check(emptyContext, true, Collections.singletonList(check));
    }

    public void testFileDescriptorLimitsThrowsOnInvalidLimit() {
        final IllegalArgumentException e =
            expectThrows(
                IllegalArgumentException.class,
                () -> new BootstrapChecks.FileDescriptorCheck(-randomIntBetween(0, Integer.MAX_VALUE)));
        assertThat(e.getMessage(), containsString("limit must be positive but was"));
    }

    public void testMlockallCheck() throws NodeValidationException {
        class MlockallCheckTestCase {

            private final boolean mlockallSet;
            private final boolean isMemoryLocked;
            private final boolean shouldFail;

            MlockallCheckTestCase(final boolean mlockallSet, final boolean isMemoryLocked, final boolean shouldFail) {
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
            final BootstrapChecks.MlockallCheck check = new BootstrapChecks.MlockallCheck() {
                @Override
                boolean isMemoryLocked() {
                    return testCase.isMemoryLocked;
                }
            };
            BootstrapContext bootstrapContext = createTestContext(
                Settings.builder().put("bootstrap.memory_lock", testCase.mlockallSet).build(), null);
            if (testCase.shouldFail) {
                final NodeValidationException e = expectThrows(
                        NodeValidationException.class,
                        () -> BootstrapChecks.check(
                                bootstrapContext,
                                true,
                                Collections.singletonList(check)));
                assertThat(
                        e.getMessage(),
                        containsString("memory locking requested for elasticsearch process but memory is not locked"));
            } else {
                // nothing should happen
                BootstrapChecks.check(bootstrapContext, true, Collections.singletonList(check));
            }
        }
    }

    public void testMaxNumberOfThreadsCheck() throws NodeValidationException {
        final int limit = 1 << 11;
        final AtomicLong maxNumberOfThreads = new AtomicLong(randomIntBetween(1, limit - 1));
        final BootstrapChecks.MaxNumberOfThreadsCheck check = new BootstrapChecks.MaxNumberOfThreadsCheck() {
            @Override
            long getMaxNumberOfThreads() {
                return maxNumberOfThreads.get();
            }
        };

        final NodeValidationException e = expectThrows(
                NodeValidationException.class,
                () -> BootstrapChecks.check(emptyContext, true, Collections.singletonList(check)));
        assertThat(e.getMessage(), containsString("max number of threads"));

        maxNumberOfThreads.set(randomIntBetween(limit + 1, Integer.MAX_VALUE));

        BootstrapChecks.check(emptyContext, true, Collections.singletonList(check));

        // nothing should happen if current max number of threads is
        // not available
        maxNumberOfThreads.set(-1);
        BootstrapChecks.check(emptyContext, true, Collections.singletonList(check));
    }

    public void testMaxSizeVirtualMemory() throws NodeValidationException {
        final long rlimInfinity = Constants.MAC_OS_X ? 9223372036854775807L : -1L;
        final AtomicLong maxSizeVirtualMemory = new AtomicLong(randomIntBetween(0, Integer.MAX_VALUE));
        final BootstrapChecks.MaxSizeVirtualMemoryCheck check = new BootstrapChecks.MaxSizeVirtualMemoryCheck() {
            @Override
            long getMaxSizeVirtualMemory() {
                return maxSizeVirtualMemory.get();
            }

            @Override
            long getRlimInfinity() {
                return rlimInfinity;
            }
        };

        final NodeValidationException e = expectThrows(
                NodeValidationException.class,
                () -> BootstrapChecks.check(emptyContext, true, Collections.singletonList(check)));
        assertThat(e.getMessage(), containsString("max size virtual memory"));

        maxSizeVirtualMemory.set(rlimInfinity);

        BootstrapChecks.check(emptyContext, true, Collections.singletonList(check));

        // nothing should happen if max size virtual memory is not available
        maxSizeVirtualMemory.set(Long.MIN_VALUE);
        BootstrapChecks.check(emptyContext, true, Collections.singletonList(check));
    }

    public void testMaxFileSizeCheck() throws NodeValidationException {
        final long rlimInfinity = Constants.MAC_OS_X ? 9223372036854775807L : -1L;
        final AtomicLong maxFileSize = new AtomicLong(randomIntBetween(0, Integer.MAX_VALUE));
        final BootstrapChecks.MaxFileSizeCheck check = new BootstrapChecks.MaxFileSizeCheck() {
            @Override
            long getMaxFileSize() {
                return maxFileSize.get();
            }

            @Override
            long getRlimInfinity() {
                return rlimInfinity;
            }
        };

        final NodeValidationException e = expectThrows(
                NodeValidationException.class,
                () -> BootstrapChecks.check(emptyContext, true, Collections.singletonList(check)));
        assertThat(e.getMessage(), containsString("max file size"));

        maxFileSize.set(rlimInfinity);

        BootstrapChecks.check(emptyContext, true, Collections.singletonList(check));

        // nothing should happen if max file size is not available
        maxFileSize.set(Long.MIN_VALUE);
        BootstrapChecks.check(emptyContext, true, Collections.singletonList(check));
    }

    public void testClientJvmCheck() throws NodeValidationException {
        final AtomicReference<String> vmName = new AtomicReference<>("Java HotSpot(TM) 32-Bit Client VM");
        final BootstrapCheck check = new BootstrapChecks.ClientJvmCheck() {
            @Override
            String getVmName() {
                return vmName.get();
            }
        };

        final NodeValidationException e = expectThrows(
                NodeValidationException.class,
                () -> BootstrapChecks.check(emptyContext, true, Collections.singletonList(check)));
        assertThat(
                e.getMessage(),
                containsString("JVM is using the client VM [Java HotSpot(TM) 32-Bit Client VM] " +
                        "but should be using a server VM for the best performance"));

        vmName.set("Java HotSpot(TM) 32-Bit Server VM");
        BootstrapChecks.check(emptyContext, true, Collections.singletonList(check));
    }

    public void testUseSerialGCCheck() throws NodeValidationException {
        final AtomicReference<String> useSerialGC = new AtomicReference<>("true");
        final BootstrapCheck check = new BootstrapChecks.UseSerialGCCheck() {
            @Override
            String getUseSerialGC() {
                return useSerialGC.get();
            }
        };

        final NodeValidationException e = expectThrows(
            NodeValidationException.class,
            () -> BootstrapChecks.check(emptyContext, true, Collections.singletonList(check)));
        assertThat(
            e.getMessage(),
            containsString("JVM is using the serial collector but should not be for the best performance; " + "" +
                "either it's the default for the VM [" + JvmInfo.jvmInfo().getVmName() +"] or -XX:+UseSerialGC was explicitly specified"));

        useSerialGC.set("false");
        BootstrapChecks.check(emptyContext, true, Collections.singletonList(check));
    }

    public void testSystemCallFilterCheck() throws NodeValidationException {
        final AtomicBoolean isSystemCallFilterInstalled = new AtomicBoolean();
        BootstrapContext context = randomBoolean() ? createTestContext(Settings.builder().put("bootstrap.system_call_filter", true)
            .build(), null) : emptyContext;

        final BootstrapChecks.SystemCallFilterCheck systemCallFilterEnabledCheck = new BootstrapChecks.SystemCallFilterCheck() {
            @Override
            boolean isSystemCallFilterInstalled() {
                return isSystemCallFilterInstalled.get();
            }
        };

        final NodeValidationException e = expectThrows(
            NodeValidationException.class,
            () -> BootstrapChecks.check(context, true, Collections.singletonList(systemCallFilterEnabledCheck)));
        assertThat(
            e.getMessage(),
            containsString("system call filters failed to install; " +
                "check the logs and fix your configuration or disable system call filters at your own risk"));

        isSystemCallFilterInstalled.set(true);
        BootstrapChecks.check(context, true, Collections.singletonList(systemCallFilterEnabledCheck));
        BootstrapContext context_1 = createTestContext(Settings.builder().put("bootstrap.system_call_filter", false).build(), null);
        final BootstrapChecks.SystemCallFilterCheck systemCallFilterNotEnabledCheck = new BootstrapChecks.SystemCallFilterCheck() {
            @Override
            boolean isSystemCallFilterInstalled() {
                return isSystemCallFilterInstalled.get();
            }
        };
        isSystemCallFilterInstalled.set(false);
        BootstrapChecks.check(context_1, true, Collections.singletonList(systemCallFilterNotEnabledCheck));
        isSystemCallFilterInstalled.set(true);
        BootstrapChecks.check(context_1, true, Collections.singletonList(systemCallFilterNotEnabledCheck));
    }

    public void testMightForkCheck() throws NodeValidationException {
        final AtomicBoolean isSystemCallFilterInstalled = new AtomicBoolean();
        final AtomicBoolean mightFork = new AtomicBoolean();
        final BootstrapChecks.MightForkCheck check = new BootstrapChecks.MightForkCheck() {
            @Override
            boolean isSystemCallFilterInstalled() {
                return isSystemCallFilterInstalled.get();
            }

            @Override
            boolean mightFork() {
                return mightFork.get();
            }

            @Override
            String message(BootstrapContext context) {
                return "error";
            }
        };

        runMightForkTest(
            check,
            isSystemCallFilterInstalled,
            () -> mightFork.set(false),
            () -> mightFork.set(true),
            e -> assertThat(e.getMessage(), containsString("error")));
    }

    public void testOnErrorCheck() throws NodeValidationException {
        final AtomicBoolean isSystemCallFilterInstalled = new AtomicBoolean();
        final AtomicReference<String> onError = new AtomicReference<>();
        final BootstrapChecks.MightForkCheck check = new BootstrapChecks.OnErrorCheck() {
            @Override
            boolean isSystemCallFilterInstalled() {
                return isSystemCallFilterInstalled.get();
            }

            @Override
            String onError() {
                return onError.get();
            }
        };

        final String command = randomAlphaOfLength(16);
        runMightForkTest(
            check,
            isSystemCallFilterInstalled,
            () -> onError.set(randomBoolean() ? "" : null),
            () -> onError.set(command),
            e -> assertThat(
                e.getMessage(),
                containsString(
                    "OnError [" + command + "] requires forking but is prevented by system call filters " +
                        "([bootstrap.system_call_filter=true]); upgrade to at least Java 8u92 and use ExitOnOutOfMemoryError")));
    }

    public void testOnOutOfMemoryErrorCheck() throws NodeValidationException {
        final AtomicBoolean isSystemCallFilterInstalled = new AtomicBoolean();
        final AtomicReference<String> onOutOfMemoryError = new AtomicReference<>();
        final BootstrapChecks.MightForkCheck check = new BootstrapChecks.OnOutOfMemoryErrorCheck() {
            @Override
            boolean isSystemCallFilterInstalled() {
                return isSystemCallFilterInstalled.get();
            }

            @Override
            String onOutOfMemoryError() {
                return onOutOfMemoryError.get();
            }
        };

        final String command = randomAlphaOfLength(16);
        runMightForkTest(
            check,
            isSystemCallFilterInstalled,
            () -> onOutOfMemoryError.set(randomBoolean() ? "" : null),
            () -> onOutOfMemoryError.set(command),
            e -> assertThat(
                e.getMessage(),
                containsString(
                    "OnOutOfMemoryError [" + command + "]"
                        + " requires forking but is prevented by system call filters ([bootstrap.system_call_filter=true]);"
                        + " upgrade to at least Java 8u92 and use ExitOnOutOfMemoryError")));
    }

    private void runMightForkTest(
        final BootstrapChecks.MightForkCheck check,
        final AtomicBoolean isSystemCallFilterInstalled,
        final Runnable disableMightFork,
        final Runnable enableMightFork,
        final Consumer<NodeValidationException> consumer) throws NodeValidationException {

        // if system call filter is disabled, nothing should happen
        isSystemCallFilterInstalled.set(false);
        if (randomBoolean()) {
            disableMightFork.run();
        } else {
            enableMightFork.run();
        }
        BootstrapChecks.check(emptyContext, true, Collections.singletonList(check));

        // if system call filter is enabled, but we will not fork, nothing should
        // happen
        isSystemCallFilterInstalled.set(true);
        disableMightFork.run();
        BootstrapChecks.check(emptyContext, true, Collections.singletonList(check));

        // if system call filter is enabled, and we might fork, the check should be enforced, regardless of bootstrap checks being enabled
        // or not
        isSystemCallFilterInstalled.set(true);
        enableMightFork.run();

        final NodeValidationException e = expectThrows(
            NodeValidationException.class,
            () -> BootstrapChecks.check(emptyContext, randomBoolean(), Collections.singletonList(check)));
        consumer.accept(e);
    }

    public void testEarlyAccessCheck() throws NodeValidationException {
        final AtomicReference<String> javaVersion
                = new AtomicReference<>(randomFrom("1.8.0_152-ea", "9-ea"));
        final BootstrapChecks.EarlyAccessCheck eaCheck = new BootstrapChecks.EarlyAccessCheck() {

            @Override
            String jvmVendor() {
                return "Oracle Corporation";
            }

            @Override
            String javaVersion() {
                return javaVersion.get();
            }

        };

        final List<BootstrapCheck> checks = Collections.singletonList(eaCheck);
        final NodeValidationException e = expectThrows(
                NodeValidationException.class,
                () -> {
                    BootstrapChecks.check(emptyContext, true, checks);
                });
        assertThat(
                e.getMessage(),
                containsString(
                        "Java version ["
                                + javaVersion.get()
                                + "] is an early-access build, only use release builds"));

        // if not on an early-access build, nothing should happen
        javaVersion.set(randomFrom("1.8.0_152", "9"));
        BootstrapChecks.check(emptyContext, true, checks);

    }

    public void testG1GCCheck() throws NodeValidationException {
        final AtomicBoolean isG1GCEnabled = new AtomicBoolean(true);
        final AtomicBoolean isJava8 = new AtomicBoolean(true);
        final AtomicReference<String> jvmVersion =
            new AtomicReference<>(String.format(Locale.ROOT, "25.%d-b%d", randomIntBetween(0, 39), randomIntBetween(1, 128)));
        final BootstrapChecks.G1GCCheck g1GCCheck = new BootstrapChecks.G1GCCheck() {

            @Override
            String jvmVendor() {
                return "Oracle Corporation";
            }

            @Override
            boolean isG1GCEnabled() {
                return isG1GCEnabled.get();
            }

            @Override
            String jvmVersion() {
                return jvmVersion.get();
            }

            @Override
            boolean isJava8() {
                return isJava8.get();
            }

        };

        final NodeValidationException e =
            expectThrows(
                NodeValidationException.class,
                () -> BootstrapChecks.check(emptyContext, true, Collections.singletonList(g1GCCheck)));
        assertThat(
            e.getMessage(),
            containsString(
                "JVM version [" + jvmVersion.get() + "] can cause data corruption when used with G1GC; upgrade to at least Java 8u40"));

        // if G1GC is disabled, nothing should happen
        isG1GCEnabled.set(false);
        BootstrapChecks.check(emptyContext, true, Collections.singletonList(g1GCCheck));

        // if on or after update 40, nothing should happen independent of whether or not G1GC is enabled
        isG1GCEnabled.set(randomBoolean());
        jvmVersion.set(String.format(Locale.ROOT, "25.%d-b%d", randomIntBetween(40, 112), randomIntBetween(1, 128)));
        BootstrapChecks.check(emptyContext, true, Collections.singletonList(g1GCCheck));

        final BootstrapChecks.G1GCCheck nonOracleCheck = new BootstrapChecks.G1GCCheck() {

            @Override
            String jvmVendor() {
                return randomAlphaOfLength(8);
            }

        };

        // if not on an Oracle JVM, nothing should happen
        BootstrapChecks.check(emptyContext, true, Collections.singletonList(nonOracleCheck));

        final BootstrapChecks.G1GCCheck nonJava8Check = new BootstrapChecks.G1GCCheck() {

            @Override
            boolean isJava8() {
                return false;
            }

        };

        // if not Java 8, nothing should happen
        BootstrapChecks.check(emptyContext, true, Collections.singletonList(nonJava8Check));
    }

    public void testAllPermissionCheck() throws NodeValidationException {
        final AtomicBoolean isAllPermissionGranted = new AtomicBoolean(true);
        final BootstrapChecks.AllPermissionCheck allPermissionCheck = new BootstrapChecks.AllPermissionCheck() {
            @Override
            boolean isAllPermissionGranted() {
                return isAllPermissionGranted.get();
            }
        };

        final List<BootstrapCheck> checks = Collections.singletonList(allPermissionCheck);
        final NodeValidationException e = expectThrows(
                NodeValidationException.class,
                () -> BootstrapChecks.check(emptyContext, true, checks));
        assertThat(e, hasToString(containsString("granting the all permission effectively disables security")));

        // if all permissions are not granted, nothing should happen
        isAllPermissionGranted.set(false);
        BootstrapChecks.check(emptyContext, true, checks);
    }

    public void testAlwaysEnforcedChecks() {
        final BootstrapCheck check = new BootstrapCheck() {
            @Override
            public BootstrapCheckResult check(BootstrapContext context) {
                return BootstrapCheckResult.failure("error");
            }

            @Override
            public boolean alwaysEnforce() {
                return true;
            }
        };

        final NodeValidationException alwaysEnforced = expectThrows(
            NodeValidationException.class,
            () -> BootstrapChecks.check(emptyContext, randomBoolean(), Collections.singletonList(check)));
        assertThat(alwaysEnforced, hasToString(containsString("error")));
    }

    public void testDiscoveryConfiguredCheck() throws NodeValidationException {
        final List<BootstrapCheck> checks = Collections.singletonList(new BootstrapChecks.DiscoveryConfiguredCheck());

        final BootstrapContext zen2Context = createTestContext(Settings.builder()
            .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), ZEN2_DISCOVERY_TYPE).build(), Metadata.EMPTY_METADATA);

        // not always enforced
        BootstrapChecks.check(zen2Context, false, checks);

        // not enforced for non-zen2 discovery
        BootstrapChecks.check(createTestContext(Settings.builder().put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(),
            randomFrom("single-node", randomAlphaOfLength(5))).build(), Metadata.EMPTY_METADATA), true, checks);

        final NodeValidationException e = expectThrows(NodeValidationException.class,
            () -> BootstrapChecks.check(zen2Context, true, checks));
        assertThat(e, hasToString(containsString("the default discovery settings are unsuitable for production use; at least one " +
            "of [discovery.seed_hosts, discovery.seed_providers, cluster.initial_master_nodes] must be configured")));

        CheckedConsumer<Settings.Builder, NodeValidationException> ensureChecksPass = b ->
        {
            final BootstrapContext context = createTestContext(b
                .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), ZEN2_DISCOVERY_TYPE).build(), Metadata.EMPTY_METADATA);
            BootstrapChecks.check(context, true, checks);
        };

        ensureChecksPass.accept(Settings.builder().putList(ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING.getKey()));
        ensureChecksPass.accept(Settings.builder().putList(DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING.getKey()));
        ensureChecksPass.accept(Settings.builder().putList(SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING.getKey()));
    }
}
