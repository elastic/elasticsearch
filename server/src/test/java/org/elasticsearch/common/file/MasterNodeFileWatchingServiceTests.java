/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.file;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MasterNodeFileWatchingServiceTests extends ESTestCase {

    static final DiscoveryNode localNode = DiscoveryNodeUtils.create("local-node");
    MasterNodeFileWatchingService testService;
    Path watchedFile;
    Consumer<Path> fileChangedCallback;

    @Before
    public void setupTestService() throws IOException {
        watchedFile = createTempFile();
        ClusterService clusterService = mock(ClusterService.class);
        Settings settings = Settings.builder()
            .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.MASTER_ROLE.roleName())
            .build();
        when(clusterService.getSettings()).thenReturn(settings);
        fileChangedCallback = f -> {};
        testService = new MasterNodeFileWatchingService(clusterService, watchedFile.getParent()) {

            @Override
            protected void processFileChanges(Path file) throws InterruptedException, ExecutionException, IOException {
                fileChangedCallback.accept(file);
            }

            @Override
            protected void processInitialFilesMissing() throws InterruptedException, ExecutionException, IOException {
                // file always exists, but we don't care about the missing case for master node behavior
            }

            // the following methods are a workaround to ensure exclusive access for files
            // required by child watchers; this is required because we only check the caller's module
            // not the entire stack
            @Override
            protected boolean filesExists(Path path) {
                return Files.exists(path);
            }

            @Override
            protected boolean filesIsDirectory(Path path) {
                return Files.isDirectory(path);
            }

            @Override
            protected boolean filesIsSymbolicLink(Path path) {
                return Files.isSymbolicLink(path);
            }

            @Override
            protected <A extends BasicFileAttributes> A filesReadAttributes(Path path, Class<A> clazz) throws IOException {
                return Files.readAttributes(path, clazz);
            }

            @Override
            protected Stream<Path> filesList(Path dir) throws IOException {
                return Files.list(dir);
            }

            @Override
            protected Path filesSetLastModifiedTime(Path path, FileTime time) throws IOException {
                return Files.setLastModifiedTime(path, time);
            }

            @Override
            protected InputStream filesNewInputStream(Path path) throws IOException {
                return Files.newInputStream(path);
            }
        };
        testService.start();
    }

    @After
    public void stopTestService() {
        testService.stop();
    }

    public void testBecomingMasterNodeStartsWatcher() {
        ClusterState notRecoveredClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()))
            .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK))
            .build();
        testService.clusterChanged(new ClusterChangedEvent("test", notRecoveredClusterState, ClusterState.EMPTY_STATE));
        // just a master node isn't sufficient, cluster state also must be recovered
        assertThat(testService.watching(), is(false));

        ClusterState recoveredClusterState = ClusterState.builder(notRecoveredClusterState)
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();
        testService.clusterChanged(new ClusterChangedEvent("test", recoveredClusterState, notRecoveredClusterState));
        // just a master node isn't sufficient, cluster state also must be recovered
        assertThat(testService.watching(), is(true));
    }

    public void testChangingMasterStopsWatcher() {
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()))
            .build();
        testService.clusterChanged(new ClusterChangedEvent("test", clusterState, ClusterState.EMPTY_STATE));
        assertThat(testService.watching(), is(true));

        final DiscoveryNode anotherNode = DiscoveryNodeUtils.create("another-node");
        ClusterState differentMasterClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder().add(localNode).add(anotherNode).localNodeId(localNode.getId()).masterNodeId(anotherNode.getId())
            )
            .build();
        testService.clusterChanged(new ClusterChangedEvent("test", differentMasterClusterState, clusterState));
        assertThat(testService.watching(), is(false));
    }

    public void testBlockingClusterStateStopsWatcher() {
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()))
            .build();
        testService.clusterChanged(new ClusterChangedEvent("test", clusterState, ClusterState.EMPTY_STATE));
        assertThat(testService.watching(), is(true));

        ClusterState blockedClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()))
            .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK))
            .build();
        testService.clusterChanged(new ClusterChangedEvent("test", blockedClusterState, clusterState));
        assertThat(testService.watching(), is(false));
    }
}
