/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.settings;

import org.apache.logging.log4j.Level;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.tasks.MockTaskManager;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterPortSettings;
import org.elasticsearch.transport.RemoteClusterService;
import org.junit.After;
import org.junit.Before;

import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptySet;
import static org.elasticsearch.test.MockLog.assertThatLogger;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@TestLogging(
    value = "org.elasticsearch.xpack.security.action.settings.TransportReloadRemoteClusterCredentialsAction:DEBUG",
    reason = "debugging test failures, log verification"
)
public class TransportReloadRemoteClusterCredentialsActionTests extends ESTestCase {
    private static final Settings REMOTE_CLUSTER_SERVER_ENABLED_SETTINGS = Settings.builder()
        .put(RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED.getKey(), "true")
        .put(RemoteClusterPortSettings.PORT.getKey(), "0")
        .build();

    private ThreadPool threadPool;
    private MockTaskManager taskManager;
    private ClusterSettings clusterSettings;
    private ClusterService clusterService;
    private MockTransportService remoteTransportService;
    private MockTransportService localTransportService;
    private RemoteClusterService localRemoteClusterService;
    private Settings proxyConnectionStrategySettings;
    private TransportReloadRemoteClusterCredentialsAction action;

    @Before
    protected void createActionResources() {
        threadPool = new TestThreadPool(getClass().getName());
        taskManager = new MockTaskManager(Settings.EMPTY, threadPool, emptySet());
        clusterSettings = ClusterSettings.createBuiltInClusterSettings();
        clusterService = new ClusterService(Settings.EMPTY, clusterSettings, threadPool, taskManager);
        remoteTransportService = MockTransportService.createNewService(
            REMOTE_CLUSTER_SERVER_ENABLED_SETTINGS,
            VersionInformation.CURRENT,
            TransportVersion.current(),
            threadPool,
            null
        );
        remoteTransportService.start();
        remoteTransportService.acceptIncomingRequests();
        final var remoteNode = remoteTransportService.getLocalNode()
            .withTransportAddress(remoteTransportService.boundRemoteAccessAddress().publishAddress());
        proxyConnectionStrategySettings = Settings.builder()
            .put("cluster.remote.foo.mode", "proxy")
            .put("cluster.remote.foo.proxy_socket_connections", 1)
            .put("cluster.remote.foo.proxy_address", remoteNode.getAddress().toString())
            .build();
        localTransportService = MockTransportService.createNewService(
            Settings.EMPTY,
            VersionInformation.CURRENT,
            TransportVersion.current(),
            threadPool,
            clusterSettings
        );
        localTransportService.start();
        localRemoteClusterService = localTransportService.getRemoteClusterService();
        action = new TransportReloadRemoteClusterCredentialsAction(
            localTransportService,
            clusterService,
            new ActionFilters(Set.of()),
            DefaultProjectResolver.INSTANCE
        );
    }

    @After
    protected void destroyActionResources() {
        localTransportService.close();
        remoteTransportService.close();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testEmptySettings() {
        clusterService.getClusterApplierService().setInitialState(ClusterState.EMPTY_STATE);
        executeRequestWithExpectedLogMessage(
            Settings.EMPTY,
            "Should log when no rebuild required",
            Level.DEBUG,
            "project [default] no connection rebuilding required after credentials update"
        );
    }

    public void testCheckForClusterBlockException() {
        final var task = taskManager.getTask(randomLongBetween(1, Long.MAX_VALUE));
        final var request = new TransportReloadRemoteClusterCredentialsAction.Request(Settings.EMPTY);
        final ClusterBlocks blocks = ClusterBlocks.builder()
            .addGlobalBlock(
                new ClusterBlock(
                    randomIntBetween(128, 256),
                    "metadata read block",
                    false,
                    false,
                    false,
                    RestStatus.SERVICE_UNAVAILABLE,
                    EnumSet.of(ClusterBlockLevel.METADATA_READ)
                )
            )
            .build();
        clusterService.getClusterApplierService()
            .setInitialState(new ClusterState.Builder(clusterService.getClusterName()).blocks(blocks).build());
        safeAwaitFailure(ClusterBlockException.class, ActionResponse.Empty.class, l -> action.execute(task, request, l));
    }

    public void testSettingsWithCredentialsButAliasNotRegisteredYet() {
        clusterService.getClusterApplierService().setInitialState(ClusterState.EMPTY_STATE);
        Settings settingsWithCredentials = Settings.builder()
            .put(proxyConnectionStrategySettings)
            .setSecureSettings(toSecureSettings("foo", randomAlphaOfLength(10)))
            .build();
        executeRequestWithExpectedLogMessage(
            settingsWithCredentials,
            "Should log when no rebuild required",
            Level.INFO,
            "project [default] no connection rebuild required for remote cluster [foo] after credentials change"
        );
    }

    public void testReconnectedWhenCredentialAddedAndRemoved() {
        clusterService.getClusterApplierService().setInitialState(ClusterState.EMPTY_STATE);

        // Triggers the initial connection attempt via the RemoteClusterService.
        clusterSettings.applySettings(proxyConnectionStrategySettings);
        checkAliasConnectionStatus("foo", true);
        final var firstConnectionInstance = localRemoteClusterService.getRemoteClusterConnection("foo");

        final var updatedCredentialsSettings = Settings.builder()
            .put(proxyConnectionStrategySettings)
            .setSecureSettings(toSecureSettings("foo", randomAlphaOfLength(10)))
            .build();
        executeRequestWithExpectedLogMessage(
            updatedCredentialsSettings,
            "Should log for added credential and reconnected",
            Level.INFO,
            "project [default] remote cluster connection [foo] updated after credentials change: [RECONNECTED]"
        );
        checkAliasConnectionStatus("foo", true);
        final var secondConnectionInstance = localRemoteClusterService.getRemoteClusterConnection("foo");
        assertNotSame(firstConnectionInstance, secondConnectionInstance);

        // Send the request again with the initial settings (no credential), should detect the credential was removed and force a rebuild.
        executeRequestWithExpectedLogMessage(
            proxyConnectionStrategySettings,
            "Should log for removed credential and reconnected",
            Level.INFO,
            "project [default] remote cluster connection [foo] updated after credentials change: [RECONNECTED]"
        );
        checkAliasConnectionStatus("foo", true);
        final var thirdConnectionInstance = localRemoteClusterService.getRemoteClusterConnection("foo");
        assertNotSame(secondConnectionInstance, thirdConnectionInstance);
    }

    public void testNoRebuildRequiredWhenCredentialValueChanged() {
        clusterService.getClusterApplierService().setInitialState(ClusterState.EMPTY_STATE);
        final var credential = randomAlphaOfLength(10);
        final var initialSettings = Settings.builder()
            .put(proxyConnectionStrategySettings)
            .setSecureSettings(toSecureSettings("foo", credential))
            .build();
        localRemoteClusterService.getRemoteClusterCredentialsManager().updateClusterCredentials(initialSettings);
        clusterSettings.applySettings(initialSettings);
        checkAliasConnectionStatus("foo", true);
        final var firstConnectionInstance = localRemoteClusterService.getRemoteClusterConnection("foo");

        final var updateCredentialsSettings = Settings.builder()
            .put(proxyConnectionStrategySettings)
            .setSecureSettings(toSecureSettings("foo", credential + "_mod"))
            .build();
        executeRequestWithExpectedLogMessage(
            updateCredentialsSettings,
            "Should log when no rebuild required",
            Level.DEBUG,
            "project [default] no connection rebuilding required after credentials update"
        );
        checkAliasConnectionStatus("foo", true);
        final var secondConnectionInstance = localRemoteClusterService.getRemoteClusterConnection("foo");
        assertSame(firstConnectionInstance, secondConnectionInstance);
    }

    public void testReconnectFailureLoggedWhenCredentialAdded() {
        clusterService.getClusterApplierService().setInitialState(ClusterState.EMPTY_STATE);
        clusterSettings.applySettings(proxyConnectionStrategySettings);
        checkAliasConnectionStatus("foo", true);

        final var updateCredentialsSettings = Settings.builder()
            .put(proxyConnectionStrategySettings)
            .put("cluster.remote.foo.proxy_address", "unknownhost:8080")
            .setSecureSettings(toSecureSettings("foo", randomAlphaOfLength(10)))
            .build();
        executeRequestWithExpectedLogMessage(
            updateCredentialsSettings,
            "Should log for reconnect failure",
            Level.WARN,
            "project [default] failed to update remote cluster connection [foo] after credentials change"
        );
        checkAliasConnectionStatus("foo", false);
    }

    public void testRemoveCalledWhenConnectionIsDisabled() {
        clusterService.getClusterApplierService().setInitialState(ClusterState.EMPTY_STATE);
        clusterSettings.applySettings(proxyConnectionStrategySettings);
        checkAliasConnectionStatus("foo", true);

        final var updateCredentialsSettings = Settings.builder()
            .put(proxyConnectionStrategySettings)
            .put("cluster.remote.foo.proxy_address", "")
            .setSecureSettings(toSecureSettings("foo", randomAlphaOfLength(10)))
            .build();
        executeRequestWithExpectedLogMessage(
            updateCredentialsSettings,
            "Should log when connection is disabled and remove is called",
            Level.INFO,
            "project [default] remote cluster connection [foo] not enabled after credentials change"
        );
        assertThat(localRemoteClusterService.getRemoteConnectionInfos().toList(), hasSize(0));
    }

    private void executeRequestWithExpectedLogMessage(Settings settings, String logExpectationName, Level level, String expectedMessage) {
        final var task = taskManager.getTask(randomLongBetween(1, Long.MAX_VALUE));
        final var request = new TransportReloadRemoteClusterCredentialsAction.Request(settings);
        final var future = new PlainActionFuture<ActionResponse.Empty>();
        assertThatLogger(() -> {
            action.execute(task, request, future);
            safeGet(future);
        },
            TransportReloadRemoteClusterCredentialsAction.class,
            new MockLog.SeenEventExpectation(
                logExpectationName,
                TransportReloadRemoteClusterCredentialsAction.class.getCanonicalName(),
                level,
                expectedMessage
            )
        );
    }

    private void checkAliasConnectionStatus(String alias, boolean expectIsConnected) {
        final var infos = localRemoteClusterService.getRemoteConnectionInfos().toList();
        assertThat(infos, hasSize(1));
        final var info = infos.getFirst();
        assertThat(info.getClusterAlias(), equalTo(alias));
        assertThat(
            "expected the " + alias + " cluster to be " + (expectIsConnected ? "connected" : "disconnected"),
            info.isConnected(),
            equalTo(expectIsConnected)
        );
    }

    private MockSecureSettings toSecureSettings(String alias, String credential) {
        final var secureSettings = new MockSecureSettings();
        secureSettings.setString("cluster.remote." + alias + ".credentials", credential);
        return secureSettings;
    }
}
