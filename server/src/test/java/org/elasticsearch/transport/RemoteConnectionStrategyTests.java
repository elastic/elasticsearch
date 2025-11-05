/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.apache.logging.log4j.Level;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EnumSerializationTestUtils;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Set;

import static org.elasticsearch.test.MockLog.assertThatLogger;
import static org.elasticsearch.transport.RemoteClusterSettings.ProxyConnectionStrategySettings.PROXY_ADDRESS;
import static org.elasticsearch.transport.RemoteClusterSettings.REMOTE_CONNECTION_MODE;
import static org.elasticsearch.transport.RemoteClusterSettings.SniffConnectionStrategySettings.REMOTE_CLUSTER_SEEDS;
import static org.elasticsearch.transport.RemoteClusterSettings.toConfig;
import static org.elasticsearch.transport.RemoteConnectionStrategy.buildConnectionProfile;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class RemoteConnectionStrategyTests extends ESTestCase {

    private static final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

    public void testStrategyChangeMeansThatStrategyMustBeRebuilt() {
        final ClusterConnectionManager connectionManager = new ClusterConnectionManager(
            Settings.EMPTY,
            mock(Transport.class),
            threadContext
        );
        RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(
            "cluster-alias",
            RemoteClusterCredentialsManager.EMPTY,
            connectionManager
        );
        FakeConnectionStrategy first = new FakeConnectionStrategy(
            "cluster-alias",
            mock(TransportService.class),
            remoteConnectionManager,
            RemoteConnectionStrategy.ConnectionStrategy.PROXY
        );
        Settings newSettings = Settings.builder()
            .put(REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace("cluster-alias").getKey(), "sniff")
            .put(REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace("cluster-alias").getKey(), "127.0.0.1:9300")
            .build();
        assertTrue(first.shouldRebuildConnection(toConfig("cluster-alias", newSettings)));
    }

    public void testSameStrategyChangeMeansThatStrategyDoesNotNeedToBeRebuilt() {
        final ClusterConnectionManager connectionManager = new ClusterConnectionManager(
            Settings.EMPTY,
            mock(Transport.class),
            threadContext
        );
        RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(
            "cluster-alias",
            RemoteClusterCredentialsManager.EMPTY,
            connectionManager
        );
        FakeConnectionStrategy first = new FakeConnectionStrategy(
            "cluster-alias",
            mock(TransportService.class),
            remoteConnectionManager,
            RemoteConnectionStrategy.ConnectionStrategy.PROXY
        );
        Settings newSettings = Settings.builder()
            .put(REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace("cluster-alias").getKey(), "proxy")
            .put(PROXY_ADDRESS.getConcreteSettingForNamespace("cluster-alias").getKey(), "127.0.0.1:9300")
            .build();
        assertFalse(first.shouldRebuildConnection(toConfig("cluster-alias", newSettings)));
    }

    public void testChangeInConnectionProfileMeansTheStrategyMustBeRebuilt() {
        final ClusterConnectionManager connectionManager = new ClusterConnectionManager(
            TestProfiles.LIGHT_PROFILE,
            mock(Transport.class),
            threadContext
        );
        assertEquals(TimeValue.MINUS_ONE, connectionManager.getConnectionProfile().getPingInterval());
        assertEquals(Compression.Enabled.INDEXING_DATA, connectionManager.getConnectionProfile().getCompressionEnabled());
        assertEquals(Compression.Scheme.LZ4, connectionManager.getConnectionProfile().getCompressionScheme());
        RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(
            "cluster-alias",
            RemoteClusterCredentialsManager.EMPTY,
            connectionManager
        );
        FakeConnectionStrategy first = new FakeConnectionStrategy(
            "cluster-alias",
            mock(TransportService.class),
            remoteConnectionManager,
            RemoteConnectionStrategy.ConnectionStrategy.PROXY
        );

        Settings.Builder newBuilder = Settings.builder();
        newBuilder.put(REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace("cluster-alias").getKey(), "proxy");
        newBuilder.put(PROXY_ADDRESS.getConcreteSettingForNamespace("cluster-alias").getKey(), "127.0.0.1:9300");
        String ping = "ping";
        String compress = "compress";
        String compressionScheme = "compression_scheme";
        String change = randomFrom(ping, compress, compressionScheme);
        if (change.equals(ping)) {
            newBuilder.put(
                RemoteClusterSettings.REMOTE_CLUSTER_PING_SCHEDULE.getConcreteSettingForNamespace("cluster-alias").getKey(),
                TimeValue.timeValueSeconds(5)
            );
        } else if (change.equals(compress)) {
            newBuilder.put(
                RemoteClusterSettings.REMOTE_CLUSTER_COMPRESS.getConcreteSettingForNamespace("cluster-alias").getKey(),
                randomFrom(Compression.Enabled.FALSE, Compression.Enabled.TRUE)
            );
        } else if (change.equals(compressionScheme)) {
            newBuilder.put(
                RemoteClusterSettings.REMOTE_CLUSTER_COMPRESSION_SCHEME.getConcreteSettingForNamespace("cluster-alias").getKey(),
                Compression.Scheme.DEFLATE
            );
        } else {
            throw new AssertionError("Unexpected option: " + change);
        }
        assertTrue(first.shouldRebuildConnection(toConfig("cluster-alias", newBuilder.build())));
    }

    public void testCorrectChannelNumber() {
        String clusterAlias = "cluster-alias";

        for (RemoteConnectionStrategy.ConnectionStrategy strategy : RemoteConnectionStrategy.ConnectionStrategy.values()) {
            String settingKey = REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(clusterAlias).getKey();
            Settings proxySettings = Settings.builder().put(settingKey, strategy.name()).build();
            ConnectionProfile proxyProfile = buildConnectionProfile(
                toConfig(clusterAlias, proxySettings),
                randomBoolean() ? RemoteClusterPortSettings.REMOTE_CLUSTER_PROFILE : TransportSettings.DEFAULT_PROFILE
            );
            assertEquals(
                "Incorrect number of channels for " + strategy.name(),
                strategy.getNumberOfChannels(),
                proxyProfile.getNumConnections()
            );
        }
    }

    public void testTransportProfile() {
        String clusterAlias = "cluster-alias";

        // New rcs connection with credentials
        for (RemoteConnectionStrategy.ConnectionStrategy strategy : RemoteConnectionStrategy.ConnectionStrategy.values()) {
            ConnectionProfile profile = buildConnectionProfile(
                toConfig(clusterAlias, Settings.EMPTY),
                RemoteClusterPortSettings.REMOTE_CLUSTER_PROFILE
            );
            assertEquals(
                "Incorrect transport profile for " + strategy.name(),
                RemoteClusterPortSettings.REMOTE_CLUSTER_PROFILE,
                profile.getTransportProfile()
            );
        }

        // Legacy ones without credentials
        for (RemoteConnectionStrategy.ConnectionStrategy strategy : RemoteConnectionStrategy.ConnectionStrategy.values()) {
            ConnectionProfile profile = buildConnectionProfile(toConfig(clusterAlias, Settings.EMPTY), TransportSettings.DEFAULT_PROFILE);
            assertEquals(
                "Incorrect transport profile for " + strategy.name(),
                TransportSettings.DEFAULT_PROFILE,
                profile.getTransportProfile()
            );
        }
    }

    public void testConnectionStrategySerialization() {
        EnumSerializationTestUtils.assertEnumSerialization(
            RemoteConnectionStrategy.ConnectionStrategy.class,
            RemoteConnectionStrategy.ConnectionStrategy.SNIFF,
            RemoteConnectionStrategy.ConnectionStrategy.PROXY
        );
    }

    @TestLogging(
        value = "org.elasticsearch.transport.RemoteConnectionStrategyTests.FakeConnectionStrategy:DEBUG",
        reason = "logging verification"
    )
    public void testConnectionAttemptMetricsAndLogging() {
        final var originProjectId = randomUniqueProjectId();
        final var linkedProjectId = randomUniqueProjectId();
        final var alias = randomAlphanumericOfLength(10);

        try (
            var threadPool = new TestThreadPool(getClass().getName());
            var transportService = startTransport(threadPool);
            var connectionManager = new RemoteConnectionManager(
                alias,
                RemoteClusterCredentialsManager.EMPTY,
                new ClusterConnectionManager(TestProfiles.LIGHT_PROFILE, mock(Transport.class), threadContext)
            )
        ) {
            assert transportService.getTelemetryProvider() != null;
            final var meterRegistry = transportService.getTelemetryProvider().getMeterRegistry();
            assert meterRegistry instanceof RecordingMeterRegistry;
            final var metricRecorder = ((RecordingMeterRegistry) meterRegistry).getRecorder();

            for (boolean shouldConnectFail : new boolean[] { true, false }) {
                for (boolean isInitialConnectAttempt : new boolean[] { true, false }) {
                    final var strategy = new FakeConnectionStrategy(
                        originProjectId,
                        linkedProjectId,
                        alias,
                        transportService,
                        connectionManager
                    );
                    if (isInitialConnectAttempt == false) {
                        waitForConnect(strategy);
                    }
                    strategy.setShouldConnectFail(shouldConnectFail);
                    final var expectedLogLevel = shouldConnectFail ? Level.WARN : Level.DEBUG;
                    final var expectedLogMessage = Strings.format(
                        "Origin project [%s] %s to linked project [%s] with alias [%s] on %s attempt",
                        originProjectId,
                        shouldConnectFail ? "failed to connect" : "successfully connected",
                        linkedProjectId,
                        alias,
                        isInitialConnectAttempt ? "the initial connection" : "a reconnection"
                    );
                    assertThatLogger(() -> {
                        if (shouldConnectFail) {
                            assertThrows(RuntimeException.class, () -> waitForConnect(strategy));
                        } else {
                            waitForConnect(strategy);
                        }
                    },
                        strategy.getClass(),
                        new MockLog.SeenEventExpectation(
                            "connection strategy should log at "
                                + expectedLogLevel
                                + " after a "
                                + (shouldConnectFail ? "failed" : "successful")
                                + (isInitialConnectAttempt ? " initial connection attempt" : " reconnection attempt"),
                            strategy.getClass().getCanonicalName(),
                            expectedLogLevel,
                            expectedLogMessage
                        )
                    );
                    if (shouldConnectFail) {
                        metricRecorder.collect();
                        final var counterName = RemoteClusterService.CONNECTION_ATTEMPT_FAILURES_COUNTER_NAME;
                        final var measurements = metricRecorder.getMeasurements(InstrumentType.LONG_COUNTER, counterName);
                        assertFalse(measurements.isEmpty());
                        final var measurement = measurements.getLast();
                        assertThat(measurement.getLong(), equalTo(1L));
                        final var attributes = measurement.attributes();
                        final var keySet = Set.of("linked_project_id", "linked_project_alias", "attempt", "strategy");
                        final var expectedAttemptType = isInitialConnectAttempt
                            ? RemoteConnectionStrategy.ConnectionAttempt.initial
                            : RemoteConnectionStrategy.ConnectionAttempt.reconnect;
                        assertThat(attributes.keySet(), equalTo(keySet));
                        assertThat(attributes.get("linked_project_id"), equalTo(linkedProjectId.toString()));
                        assertThat(attributes.get("linked_project_alias"), equalTo(alias));
                        assertThat(attributes.get("attempt"), equalTo(expectedAttemptType.toString()));
                        assertThat(attributes.get("strategy"), equalTo(strategy.strategyType().toString()));
                    }
                }
            }
        }
    }

    private MockTransportService startTransport(ThreadPool threadPool) {
        boolean success = false;
        final Settings s = Settings.builder().put(ClusterName.CLUSTER_NAME_SETTING.getKey(), "cluster1").put("node.name", "node1").build();
        MockTransportService newService = MockTransportService.createNewService(
            s,
            VersionInformation.CURRENT,
            TransportVersion.current(),
            threadPool
        );
        try {
            newService.start();
            newService.acceptIncomingRequests();
            success = true;
            return newService;
        } finally {
            if (success == false) {
                newService.close();
            }
        }
    }

    private static void waitForConnect(RemoteConnectionStrategy strategy) {
        PlainActionFuture<Void> connectFuture = new PlainActionFuture<>();
        strategy.connect(connectFuture);
        connectFuture.actionGet();
    }

    private static class FakeConnectionStrategy extends RemoteConnectionStrategy {

        private final ConnectionStrategy strategy;
        private boolean shouldConnectFail;

        FakeConnectionStrategy(
            ProjectId originProjectId,
            ProjectId linkedProjectId,
            String clusterAlias,
            TransportService transportService,
            RemoteConnectionManager connectionManager
        ) {
            this(
                originProjectId,
                linkedProjectId,
                clusterAlias,
                transportService,
                connectionManager,
                randomFrom(RemoteConnectionStrategy.ConnectionStrategy.values())
            );
        }

        FakeConnectionStrategy(
            String clusterAlias,
            TransportService transportService,
            RemoteConnectionManager connectionManager,
            RemoteConnectionStrategy.ConnectionStrategy strategy
        ) {
            this(ProjectId.DEFAULT, ProjectId.DEFAULT, clusterAlias, transportService, connectionManager, strategy);
        }

        FakeConnectionStrategy(
            ProjectId originProjectId,
            ProjectId linkedProjectId,
            String clusterAlias,
            TransportService transportService,
            RemoteConnectionManager connectionManager,
            RemoteConnectionStrategy.ConnectionStrategy strategy
        ) {
            super(switch (strategy) {
                case PROXY -> new LinkedProjectConfig.ProxyLinkedProjectConfigBuilder(originProjectId, linkedProjectId, clusterAlias)
                    .build();
                case SNIFF -> new LinkedProjectConfig.SniffLinkedProjectConfigBuilder(originProjectId, linkedProjectId, clusterAlias)
                    .build();
            }, transportService, connectionManager);
            this.strategy = strategy;
            this.shouldConnectFail = false;
        }

        void setShouldConnectFail(boolean shouldConnectFail) {
            this.shouldConnectFail = shouldConnectFail;
        }

        @Override
        protected boolean strategyMustBeRebuilt(LinkedProjectConfig config) {
            return false;
        }

        @Override
        protected ConnectionStrategy strategyType() {
            return this.strategy;
        }

        @Override
        protected boolean shouldOpenMoreConnections() {
            return false;
        }

        @Override
        protected void connectImpl(ActionListener<Void> listener) {
            if (shouldConnectFail) {
                listener.onFailure(new RuntimeException("simulated failure"));
            } else {
                listener.onResponse(null);
            }
        }

        @Override
        protected RemoteConnectionInfo.ModeInfo getModeInfo() {
            return null;
        }
    }
}
