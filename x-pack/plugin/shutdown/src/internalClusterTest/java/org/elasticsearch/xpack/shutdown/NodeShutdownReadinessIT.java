/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.readiness.MockReadinessService;
import org.elasticsearch.readiness.ReadinessService;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.readiness.MockReadinessService.tcpReadinessProbeFalse;
import static org.elasticsearch.readiness.MockReadinessService.tcpReadinessProbeTrue;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class NodeShutdownReadinessIT extends ESIntegTestCase {

    Path configDir;

    @Before
    public void setupMasterConfigDir() throws IOException {
        configDir = createTempDir();
        Path settingsFile = configDir.resolve("operator").resolve("settings.json");
        Files.createDirectories(settingsFile.getParent());
        Files.writeString(settingsFile, """
            {
                 "metadata": {
                     "version": "1",
                     "compatibility": "8.4.0"
                 },
                 "state": {
                     "cluster_settings": {}
                 }
            }""");
    }

    @Override
    protected Path nodeConfigPath(int nodeOrdinal) {
        return configDir;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.getMockPlugins());
        plugins.add(MockReadinessService.TestPlugin.class);
        return Collections.unmodifiableList(plugins);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ShutdownPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(Settings.builder().put(ReadinessService.PORT.getKey(), 0).build());
        return settings.build();
    }

    private void putNodeShutdown(String nodeId, SingleNodeShutdownMetadata.Type type, TimeValue allocationDelay) {
        assertAcked(
            client().execute(
                PutShutdownNodeAction.INSTANCE,
                new PutShutdownNodeAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    nodeId,
                    type,
                    this.getTestName(),
                    allocationDelay,
                    null,
                    null
                )
            )
        );
    }

    private void deleteNodeShutdown(String nodeId) {
        assertAcked(
            client().execute(
                DeleteShutdownNodeAction.INSTANCE,
                new DeleteShutdownNodeAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, nodeId)
            )
        );
    }

    private void assertNoShuttingDownNodes(String nodeId) throws ExecutionException, InterruptedException {
        var response = client().execute(GetShutdownStatusAction.INSTANCE, new GetShutdownStatusAction.Request(TEST_REQUEST_TIMEOUT, nodeId))
            .get();
        assertThat(response.getShutdownStatuses(), empty());
    }

    public void testShutdownReadinessService() throws Exception {

        final String nodeName = internalCluster().startMasterOnlyNode();
        final String nodeId = getNodeId(nodeName);

        final var readinessService = internalCluster().getInstance(ReadinessService.class, nodeName);

        // Once we have the right port, check to see if it's ready, has to be for a properly started cluster
        tcpReadinessProbeTrue(readinessService);

        // Mark the node for shutdown and check that it's not ready
        putNodeShutdown(nodeId, SingleNodeShutdownMetadata.Type.RESTART, TimeValue.timeValueMinutes(1));
        tcpReadinessProbeFalse(readinessService);

        // Delete the shutdown request and verify that the node is ready again
        deleteNodeShutdown(nodeId);
        assertNoShuttingDownNodes(nodeId);

        tcpReadinessProbeTrue(readinessService);
    }
}
