/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.bootstrap;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.coordination.ClusterBootstrapService;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.SettingsBasedSeedHostsProvider;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.nativeaccess.ProcessLimits;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.test.AbstractBootstrapCheckTestCase;
import org.hamcrest.Matcher;

import java.net.InetAddress;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.elasticsearch.discovery.DiscoveryModule.MULTI_NODE_DISCOVERY_TYPE;
import static org.elasticsearch.discovery.DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;
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
            final TransportAddress randomTransportAddress = randomBoolean()
                ? buildNewFakeTransportAddress()
                : new TransportAddress(InetAddress.getLoopbackAddress(), i);
            transportAddresses.add(randomTransportAddress);
        }

        final TransportAddress publishAddress = randomBoolean()
            ? buildNewFakeTransportAddress()
            : new TransportAddress(InetAddress.getLoopbackAddress(), 0);

        final BoundTransportAddress boundTransportAddress = mock(BoundTransportAddress.class);
        Collections.shuffle(transportAddresses, random());
        when(boundTransportAddress.boundAddresses()).thenReturn(transportAddresses.toArray(new TransportAddress[0]));
        when(boundTransportAddress.publishAddress()).thenReturn(publishAddress);

        final String discoveryType = randomFrom(MULTI_NODE_DISCOVERY_TYPE, SINGLE_NODE_DISCOVERY_TYPE);

        assertEquals(
            BootstrapChecks.enforceLimits(boundTransportAddress, discoveryType, FALSE::booleanValue),
            SINGLE_NODE_DISCOVERY_TYPE.equals(discoveryType) == false
        );
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

        final String discoveryType = randomFrom(MULTI_NODE_DISCOVERY_TYPE, SINGLE_NODE_DISCOVERY_TYPE);

        assertEquals(
            BootstrapChecks.enforceLimits(boundTransportAddress, discoveryType, FALSE::booleanValue),
            SINGLE_NODE_DISCOVERY_TYPE.equals(discoveryType) == false
        );
    }

    public void testDoNotEnforceLimitsWhenSnapshotBuild() {
        final List<TransportAddress> transportAddresses = new ArrayList<>();

        for (int i = 0; i < randomIntBetween(1, 8); i++) {
            final TransportAddress randomTransportAddress = buildNewFakeTransportAddress();
            transportAddresses.add(randomTransportAddress);
        }

        final TransportAddress publishAddress = new TransportAddress(InetAddress.getLoopbackAddress(), 0);
        final BoundTransportAddress boundTransportAddress = mock(BoundTransportAddress.class);
        when(boundTransportAddress.boundAddresses()).thenReturn(transportAddresses.toArray(new TransportAddress[0]));
        when(boundTransportAddress.publishAddress()).thenReturn(publishAddress);

        assertThat(BootstrapChecks.enforceLimits(boundTransportAddress, MULTI_NODE_DISCOVERY_TYPE, TRUE::booleanValue), is(false));
    }

    public void testExceptionAggregation() {
        final List<BootstrapCheck> checks = Arrays.asList(new BootstrapCheck() {
            @Override
            public BootstrapCheckResult check(BootstrapContext context) {
                return BootstrapCheck.BootstrapCheckResult.failure("first");
            }

            @Override
            public ReferenceDocs referenceDocs() {
                return ReferenceDocs.BOOTSTRAP_CHECKS;
            }
        }, new BootstrapCheck() {
            @Override
            public BootstrapCheckResult check(BootstrapContext context) {
                return BootstrapCheck.BootstrapCheckResult.failure("second");
            }

            @Override
            public ReferenceDocs referenceDocs() {
                return ReferenceDocs.BOOTSTRAP_CHECKS;
            }
        });

        final NodeValidationException e = expectThrows(
            NodeValidationException.class,
            () -> BootstrapChecks.check(emptyContext, true, checks)
        );
        assertThat(
            e,
            hasToString(
                allOf(
                    containsString("[2] bootstrap checks failed"),
                    containsString("You must address the points described in the following [2] lines before starting Elasticsearch"),
                    containsString("bootstrap check failure [1] of [2]:"),
                    containsString("first"),
                    containsString("bootstrap check failure [2] of [2]:"),
                    containsString("second"),
                    containsString("For more information see [https://www.elastic.co/guide/en/elasticsearch/reference/")
                )
            )
        );
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

        final NodeValidationException e = expectThrows(
            NodeValidationException.class,
            () -> BootstrapChecks.check(emptyContext, true, Collections.singletonList(check))
        );
        assertThat(
            e.getMessage(),
            containsString(
                "initial heap size [" + initialHeapSize.get() + "] " + "not equal to maximum heap size [" + maxHeapSize.get() + "]"
            )
        );
        assertThat(e.getMessage(), containsString("; for more information see [https://www.elastic.co/guide/en/elasticsearch/reference/"));
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

        final NodeValidationException e = expectThrows(
            NodeValidationException.class,
            () -> BootstrapChecks.check(emptyContext, true, Collections.singletonList(check))
        );
        assertThat(e.getMessage(), containsString("max file descriptors"));
        assertThat(e.getMessage(), containsString("; for more information see [https://www.elastic.co/guide/en/elasticsearch/reference/"));

        maxFileDescriptorCount.set(randomIntBetween(limit + 1, Integer.MAX_VALUE));

        BootstrapChecks.check(emptyContext, true, Collections.singletonList(check));

        // nothing should happen if current file descriptor count is
        // not available
        maxFileDescriptorCount.set(-1);
        BootstrapChecks.check(emptyContext, true, Collections.singletonList(check));
    }

    public void testFileDescriptorLimitsThrowsOnInvalidLimit() {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new BootstrapChecks.FileDescriptorCheck(-randomIntBetween(0, Integer.MAX_VALUE))
        );
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
                Settings.builder().put("bootstrap.memory_lock", testCase.mlockallSet).build(),
                null
            );
            if (testCase.shouldFail) {
                final NodeValidationException e = expectThrows(
                    NodeValidationException.class,
                    () -> BootstrapChecks.check(bootstrapContext, true, Collections.singletonList(check))
                );
                assertThat(e.getMessage(), containsString("memory locking requested for elasticsearch process but memory is not locked"));
                assertThat(
                    e.getMessage(),
                    containsString("; for more information see [https://www.elastic.co/guide/en/elasticsearch/reference/")
                );
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
            () -> BootstrapChecks.check(emptyContext, true, Collections.singletonList(check))
        );
        assertThat(e.getMessage(), containsString("max number of threads"));
        assertThat(e.getMessage(), containsString("; for more information see [https://www.elastic.co/guide/en/elasticsearch/reference/"));

        maxNumberOfThreads.set(randomIntBetween(limit + 1, Integer.MAX_VALUE));

        BootstrapChecks.check(emptyContext, true, Collections.singletonList(check));

        // nothing should happen if current max number of threads is
        // not available
        maxNumberOfThreads.set(ProcessLimits.UNKNOWN);
        BootstrapChecks.check(emptyContext, true, Collections.singletonList(check));
    }

    public void testMaxSizeVirtualMemory() throws NodeValidationException {
        final AtomicLong maxSizeVirtualMemory = new AtomicLong(randomIntBetween(0, Integer.MAX_VALUE));
        final BootstrapChecks.MaxSizeVirtualMemoryCheck check = new BootstrapChecks.MaxSizeVirtualMemoryCheck() {
            @Override
            long getMaxSizeVirtualMemory() {
                return maxSizeVirtualMemory.get();
            }
        };

        final NodeValidationException e = expectThrows(
            NodeValidationException.class,
            () -> BootstrapChecks.check(emptyContext, true, Collections.singletonList(check))
        );
        assertThat(e.getMessage(), containsString("max size virtual memory"));
        assertThat(e.getMessage(), containsString("; for more information see [https://www.elastic.co/guide/en/elasticsearch/reference/"));

        maxSizeVirtualMemory.set(ProcessLimits.UNLIMITED);

        BootstrapChecks.check(emptyContext, true, Collections.singletonList(check));

        // nothing should happen if max size virtual memory is not available
        maxSizeVirtualMemory.set(Long.MIN_VALUE);
        BootstrapChecks.check(emptyContext, true, Collections.singletonList(check));
    }

    public void testMaxFileSizeCheck() throws NodeValidationException {
        final AtomicLong maxFileSize = new AtomicLong(randomIntBetween(0, Integer.MAX_VALUE));
        final BootstrapChecks.MaxFileSizeCheck check = new BootstrapChecks.MaxFileSizeCheck() {
            @Override
            protected ProcessLimits getProcessLimits() {
                return new ProcessLimits(ProcessLimits.UNKNOWN, ProcessLimits.UNKNOWN, maxFileSize.get());
            }
        };

        final NodeValidationException e = expectThrows(
            NodeValidationException.class,
            () -> BootstrapChecks.check(emptyContext, true, Collections.singletonList(check))
        );
        assertThat(e.getMessage(), containsString("max file size"));
        assertThat(e.getMessage(), containsString("; for more information see [https://www.elastic.co/guide/en/elasticsearch/reference/"));

        maxFileSize.set(ProcessLimits.UNLIMITED);

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
            () -> BootstrapChecks.check(emptyContext, true, Collections.singletonList(check))
        );
        assertThat(
            e.getMessage(),
            containsString(
                "JVM is using the client VM [Java HotSpot(TM) 32-Bit Client VM] "
                    + "but should be using a server VM for the best performance"
            )
        );
        assertThat(e.getMessage(), containsString("; for more information see [https://www.elastic.co/guide/en/elasticsearch/reference/"));

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
            () -> BootstrapChecks.check(emptyContext, true, Collections.singletonList(check))
        );
        assertThat(
            e.getMessage(),
            containsString(
                "JVM is using the serial collector but should not be for the best performance; "
                    + ""
                    + "either it's the default for the VM ["
                    + JvmInfo.jvmInfo().getVmName()
                    + "] or -XX:+UseSerialGC was explicitly specified"
            )
        );
        assertThat(e.getMessage(), containsString("; for more information see [https://www.elastic.co/guide/en/elasticsearch/reference/"));

        useSerialGC.set("false");
        BootstrapChecks.check(emptyContext, true, Collections.singletonList(check));
    }

    public void testSystemCallFilterCheck() throws NodeValidationException {
        final AtomicBoolean isSystemCallFilterInstalled = new AtomicBoolean();
        final BootstrapContext context = emptyContext;

        final BootstrapChecks.SystemCallFilterCheck systemCallFilterEnabledCheck = new BootstrapChecks.SystemCallFilterCheck() {

            @Override
            boolean isSystemCallFilterInstalled() {
                return isSystemCallFilterInstalled.get();
            }

        };

        final NodeValidationException e = expectThrows(
            NodeValidationException.class,
            () -> BootstrapChecks.check(context, true, Collections.singletonList(systemCallFilterEnabledCheck))
        );
        assertThat(e.getMessage(), containsString("system call filters failed to install; check the logs and fix your configuration"));
        assertThat(e.getMessage(), containsString("; for more information see [https://www.elastic.co/guide/en/elasticsearch/reference/"));

        isSystemCallFilterInstalled.set(true);
        BootstrapChecks.check(context, true, Collections.singletonList(systemCallFilterEnabledCheck));
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

        runMightForkTest(check, isSystemCallFilterInstalled, () -> mightFork.set(false), () -> mightFork.set(true), e -> {
            assertThat(e.getMessage(), containsString("error"));
            assertThat(
                e.getMessage(),
                containsString("; for more information see [https://www.elastic.co/guide/en/elasticsearch/reference/")
            );
        });
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
            e -> {
                assertThat(
                    e.getMessage(),
                    containsString(
                        "OnError ["
                            + command
                            + "] requires forking but is prevented by system call filters;"
                            + " upgrade to at least Java 8u92 and use ExitOnOutOfMemoryError"
                    )
                );
                assertThat(
                    e.getMessage(),
                    containsString("; for more information see [https://www.elastic.co/guide/en/elasticsearch/reference/")
                );
            }
        );
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
            e -> {
                assertThat(
                    e.getMessage(),
                    containsString(
                        "OnOutOfMemoryError ["
                            + command
                            + "]"
                            + " requires forking but is prevented by system call filters;"
                            + " upgrade to at least Java 8u92 and use ExitOnOutOfMemoryError"
                    )
                );
                assertThat(
                    e.getMessage(),
                    containsString("; for more information see [https://www.elastic.co/guide/en/elasticsearch/reference/")
                );
            }
        );
    }

    private void runMightForkTest(
        final BootstrapChecks.MightForkCheck check,
        final AtomicBoolean isSystemCallFilterInstalled,
        final Runnable disableMightFork,
        final Runnable enableMightFork,
        final Consumer<NodeValidationException> consumer
    ) throws NodeValidationException {

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
            () -> BootstrapChecks.check(emptyContext, randomBoolean(), Collections.singletonList(check))
        );
        consumer.accept(e);
    }

    public void testEarlyAccessCheck() throws NodeValidationException {
        final AtomicReference<String> javaVersion = new AtomicReference<>(randomFrom("1.8.0_152-ea", "9-ea"));
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
            () -> { BootstrapChecks.check(emptyContext, true, checks); }
        );
        assertThat(
            e.getMessage(),
            containsString("Java version [" + javaVersion.get() + "] is an early-access build, only use release builds")
        );
        assertThat(e.getMessage(), containsString("; for more information see [https://www.elastic.co/guide/en/elasticsearch/reference/"));

        // if not on an early-access build, nothing should happen
        javaVersion.set(randomFrom("1.8.0_152", "9"));
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

            @Override
            public ReferenceDocs referenceDocs() {
                return ReferenceDocs.BOOTSTRAP_CHECKS;
            }
        };

        final NodeValidationException alwaysEnforced = expectThrows(
            NodeValidationException.class,
            () -> BootstrapChecks.check(emptyContext, randomBoolean(), Collections.singletonList(check))
        );
        assertThat(alwaysEnforced, hasToString(containsString("error")));
    }

    public void testDiscoveryConfiguredCheck() throws NodeValidationException {
        final BootstrapChecks.DiscoveryConfiguredCheck check = new BootstrapChecks.DiscoveryConfiguredCheck();
        final List<BootstrapCheck> checks = Collections.singletonList(check);

        final BootstrapContext zen2Context = createTestContext(
            Settings.builder().put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), MULTI_NODE_DISCOVERY_TYPE).build(),
            Metadata.EMPTY_METADATA
        );

        // not always enforced
        BootstrapChecks.check(zen2Context, false, checks);

        // not enforced for non-multi-node discovery
        BootstrapChecks.check(
            createTestContext(
                Settings.builder()
                    .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), randomFrom(SINGLE_NODE_DISCOVERY_TYPE, randomAlphaOfLength(5)))
                    .build(),
                Metadata.EMPTY_METADATA
            ),
            true,
            checks
        );

        final NodeValidationException e = expectThrows(
            NodeValidationException.class,
            () -> BootstrapChecks.check(zen2Context, true, checks)
        );
        assertThat(
            e,
            hasToString(
                containsString(
                    "the default discovery settings are unsuitable for production use; at least one "
                        + "of [discovery.seed_hosts, discovery.seed_providers, cluster.initial_master_nodes] must be configured"
                )
            )
        );
        assertThat(e.getMessage(), containsString("; for more information see [https://www.elastic.co/guide/en/elasticsearch/reference/"));

        CheckedConsumer<Settings.Builder, NodeValidationException> ensureChecksPass = b -> {
            final BootstrapContext context = createTestContext(
                b.put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), MULTI_NODE_DISCOVERY_TYPE).build(),
                Metadata.EMPTY_METADATA
            );
            BootstrapChecks.check(context, true, checks);
        };

        ensureChecksPass.accept(Settings.builder().putList(ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING.getKey()));
        ensureChecksPass.accept(Settings.builder().putList(DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING.getKey()));
        ensureChecksPass.accept(Settings.builder().putList(SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING.getKey()));
    }

    public void testByteOrderCheck() throws NodeValidationException {
        ByteOrder[] reference = new ByteOrder[] { ByteOrder.BIG_ENDIAN };
        BootstrapChecks.ByteOrderCheck byteOrderCheck = new BootstrapChecks.ByteOrderCheck() {
            @Override
            ByteOrder nativeByteOrder() {
                return reference[0];
            }
        };

        final NodeValidationException e = expectThrows(
            NodeValidationException.class,
            () -> BootstrapChecks.check(emptyContext, true, List.of(byteOrderCheck))
        );
        assertThat(e.getMessage(), containsString("Little-endian native byte order is required to run Elasticsearch"));
        assertThat(e.getMessage(), containsString("; for more information see [https://www.elastic.co/guide/en/elasticsearch/reference/"));

        reference[0] = ByteOrder.LITTLE_ENDIAN;
        BootstrapChecks.check(emptyContext, true, List.of(byteOrderCheck));
    }
}
