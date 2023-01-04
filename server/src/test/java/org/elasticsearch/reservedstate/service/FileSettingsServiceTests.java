/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reservedstate.service;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.ingest.IngestInfo;
import org.elasticsearch.ingest.ProcessorInfo;
import org.elasticsearch.reservedstate.action.ReservedClusterSettingsAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.WatchKey;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class FileSettingsServiceTests extends ESTestCase {
    private Environment env;
    private ClusterService clusterService;
    private FileSettingsService fileSettingsService;
    private ReservedClusterStateService controller;
    private ThreadPool threadpool;
    private NodeClient nodeClient;
    private ClusterAdminClient clusterAdminClient;
    private NodeInfo nodeInfo;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        super.setUp();

        threadpool = new TestThreadPool("file_settings_service_tests");

        clusterService = spy(
            new ClusterService(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadpool,
                null
            )
        );

        final DiscoveryNode localNode = new DiscoveryNode("node", buildNewFakeTransportAddress(), Version.CURRENT);
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()))
            .build();
        doAnswer((Answer<ClusterState>) invocation -> clusterState).when(clusterService).state();

        clusterService.setRerouteService(mock(RerouteService.class));
        env = newEnvironment(Settings.EMPTY);

        Files.createDirectories(env.configFile());

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        controller = new ReservedClusterStateService(clusterService, List.of(new ReservedClusterSettingsAction(clusterSettings)));

        DiscoveryNode discoveryNode = new DiscoveryNode(
            "_node_id",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            Version.CURRENT
        );

        nodeInfo = new NodeInfo(
            Version.CURRENT,
            Build.CURRENT,
            discoveryNode,
            Settings.EMPTY,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            new IngestInfo(Collections.singletonList(new ProcessorInfo("set"))),
            null,
            null
        );
        NodesInfoResponse response = new NodesInfoResponse(new ClusterName("elasticsearch"), List.of(nodeInfo), List.of());

        clusterAdminClient = mock(ClusterAdminClient.class);
        doAnswer(i -> {
            ((ActionListener<NodesInfoResponse>) i.getArgument(1)).onResponse(response);
            return null;
        }).when(clusterAdminClient).nodesInfo(any(), any());

        nodeClient = mock(NodeClient.class);
        fileSettingsService = spy(new FileSettingsService(clusterService, controller, env, nodeClient));
        doAnswer(i -> clusterAdminClient).when(fileSettingsService).clusterAdminClient();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadpool.shutdownNow();
    }

    public void testOperatorDirName() {
        Path operatorPath = fileSettingsService.operatorSettingsDir();
        assertTrue(operatorPath.startsWith(env.configFile()));
        assertTrue(operatorPath.endsWith("operator"));

        Path operatorSettingsFile = fileSettingsService.operatorSettingsFile();
        assertTrue(operatorSettingsFile.startsWith(operatorPath));
        assertTrue(operatorSettingsFile.endsWith("settings.json"));
    }

    public void testWatchedFile() throws Exception {
        Path tmpFile = createTempFile();
        Path tmpFile1 = createTempFile();
        Path otherFile = tmpFile.getParent().resolve("other.json");
        // we return false on non-existent paths, we don't remember state
        assertFalse(fileSettingsService.watchedFileChanged(otherFile));

        // we remember the previous state
        assertTrue(fileSettingsService.watchedFileChanged(tmpFile));
        assertFalse(fileSettingsService.watchedFileChanged(tmpFile));

        // we modify the timestamp of the file, it should trigger a change
        Instant now = LocalDateTime.now(ZoneId.systemDefault()).toInstant(ZoneOffset.ofHours(0));
        Files.setLastModifiedTime(tmpFile, FileTime.from(now));

        assertTrue(fileSettingsService.watchedFileChanged(tmpFile));
        assertFalse(fileSettingsService.watchedFileChanged(tmpFile));

        // we change to another real file, it should be changed
        assertTrue(fileSettingsService.watchedFileChanged(tmpFile1));
        assertFalse(fileSettingsService.watchedFileChanged(tmpFile1));
    }

    public void testStartStop() {
        fileSettingsService.start();
        assertTrue(fileSettingsService.watching());
        fileSettingsService.stop();
        assertFalse(fileSettingsService.watching());
        fileSettingsService.close();
    }

    public void testCallsProcessing() throws Exception {
        FileSettingsService service = spy(fileSettingsService);
        CountDownLatch processFileLatch = new CountDownLatch(1);

        doAnswer((Answer<CompletableFuture<Void>>) invocation -> {
            processFileLatch.countDown();
            return CompletableFuture.completedFuture(null);
        }).when(service).processFileSettings(any());

        service.start();
        assertTrue(service.watching());

        Files.createDirectories(service.operatorSettingsDir());

        writeTestFile(service.operatorSettingsFile(), "{}");

        // we need to wait a bit, on MacOS it may take up to 10 seconds for the Java watcher service to notice the file,
        // on Linux is instantaneous. Windows is instantaneous too.
        processFileLatch.await(30, TimeUnit.SECONDS);

        verify(service, Mockito.atLeast(1)).watchedFileChanged(any());
        verify(service, times(1)).processFileSettings(any());

        service.stop();
        assertFalse(service.watching());
        service.close();
    }

    @SuppressWarnings("unchecked")
    public void testInitialFile() throws Exception {
        ReservedClusterStateService stateService = mock(ReservedClusterStateService.class);

        doAnswer((Answer<Void>) invocation -> {
            ((Consumer<Exception>) invocation.getArgument(2)).accept(new IllegalStateException("Some exception"));
            return null;
        }).when(stateService).process(any(), (ReservedStateChunk) any(), any());

        FileSettingsService service = spy(new FileSettingsService(clusterService, stateService, env, nodeClient));
        doAnswer(i -> clusterAdminClient).when(service).clusterAdminClient();

        Files.createDirectories(service.operatorSettingsDir());

        // contents of the JSON don't matter, we just need a file to exist
        writeTestFile(service.operatorSettingsFile(), "{}");

        Exception startupException = expectThrows(IllegalStateException.class, () -> service.start());
        assertThat(
            startupException.getCause(),
            allOf(
                instanceOf(FileSettingsService.FileSettingsStartupException.class),
                hasToString(
                    "org.elasticsearch.reservedstate.service.FileSettingsService$FileSettingsStartupException: "
                        + "Error applying operator settings"
                )
            )
        );

        verify(service, times(1)).processFileSettings(any());

        service.stop();

        clearInvocations(service);

        // Let's check that if we didn't throw an error that everything works
        doAnswer((Answer<Void>) invocation -> {
            ((Consumer<Exception>) invocation.getArgument(2)).accept(null);
            return null;
        }).when(stateService).process(any(), (ReservedStateChunk) any(), any());

        service.start();
        service.startWatcher(clusterService.state(), true);

        verify(service, times(1)).processFileSettings(any());

        service.stop();
        service.close();
    }

    @SuppressWarnings("unchecked")
    public void testStopWorksInMiddleOfProcessing() throws Exception {
        var spiedController = spy(controller);
        var fsService = new FileSettingsService(clusterService, spiedController, env, nodeClient);
        FileSettingsService service = spy(fsService);
        doAnswer(i -> clusterAdminClient).when(service).clusterAdminClient();

        CountDownLatch processFileLatch = new CountDownLatch(1);
        CountDownLatch deadThreadLatch = new CountDownLatch(1);

        doAnswer((Answer<ReservedStateChunk>) invocation -> {
            processFileLatch.countDown();
            new Thread(() -> {
                // Simulate a thread that never comes back and decrements the
                // countdown latch in FileSettingsService.processFileSettings
                try {
                    deadThreadLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }).start();
            return new ReservedStateChunk(Collections.emptyMap(), new ReservedStateVersion(1L, Version.CURRENT));
        }).when(spiedController).parse(any(String.class), any());

        service.start();
        assertTrue(service.watching());

        Files.createDirectories(service.operatorSettingsDir());

        // Make some fake settings file to cause the file settings service to process it
        writeTestFile(service.operatorSettingsFile(), "{}");

        // we need to wait a bit, on MacOS it may take up to 10 seconds for the Java watcher service to notice the file,
        // on Linux is instantaneous. Windows is instantaneous too.
        assertTrue(processFileLatch.await(30, TimeUnit.SECONDS));

        // Stopping the service should interrupt the watcher thread, we should be able to stop
        service.stop();
        assertFalse(service.watching());
        service.close();
        // let the deadlocked thread end, so we can cleanly exit the test
        deadThreadLatch.countDown();
    }

    @SuppressWarnings("unchecked")
    public void testStopWorksIfProcessingDidntReturnYet() throws Exception {
        var spiedController = spy(controller);
        var fsService = new FileSettingsService(clusterService, spiedController, env, nodeClient);

        FileSettingsService service = spy(fsService);
        doAnswer(i -> clusterAdminClient).when(service).clusterAdminClient();
        CountDownLatch processFileLatch = new CountDownLatch(1);
        CountDownLatch deadThreadLatch = new CountDownLatch(1);

        doAnswer((Answer<ReservedStateChunk>) invocation -> {
            // allow the other thread to continue, but hold on a bit to avoid
            // completing the task immediately in the main watcher loop
            Thread.sleep(1_000);
            processFileLatch.countDown();
            new Thread(() -> {
                // Simulate a thread that never allows the completion to complete
                try {
                    deadThreadLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }).start();
            return new ReservedStateChunk(Collections.emptyMap(), new ReservedStateVersion(1L, Version.CURRENT));
        }).when(spiedController).parse(any(String.class), any());

        service.start();
        assertTrue(service.watching());

        Files.createDirectories(service.operatorSettingsDir());

        // Make some fake settings file to cause the file settings service to process it
        writeTestFile(service.operatorSettingsFile(), "{}");

        // we need to wait a bit, on MacOS it may take up to 10 seconds for the Java watcher service to notice the file,
        // on Linux is instantaneous. Windows is instantaneous too.
        assertTrue(processFileLatch.await(30, TimeUnit.SECONDS));

        // Stopping the service should interrupt the watcher thread, allowing the whole thing to exit
        service.stop();
        assertFalse(service.watching());
        service.close();
        // let the deadlocked thread end, so we can cleanly exit the test
        deadThreadLatch.countDown();
    }

    @SuppressWarnings("unchecked")
    public void testNodeInfosRefresh() throws Exception {
        var spiedController = spy(controller);
        var csAdminClient = spy(clusterAdminClient);
        var response = new NodesInfoResponse(new ClusterName("elasticsearch"), List.of(nodeInfo), List.of());

        doAnswer(i -> {
            ((ActionListener<NodesInfoResponse>) i.getArgument(1)).onResponse(response);
            return null;
        }).when(csAdminClient).nodesInfo(any(), any());

        var service = spy(new FileSettingsService(clusterService, spiedController, env, nodeClient));
        doAnswer(i -> csAdminClient).when(service).clusterAdminClient();

        doAnswer(
            (Answer<ReservedStateChunk>) invocation -> new ReservedStateChunk(
                Collections.emptyMap(),
                new ReservedStateVersion(1L, Version.CURRENT)
            )
        ).when(spiedController).parse(any(String.class), any());

        Files.createDirectories(service.operatorSettingsDir());
        // Make some fake settings file to cause the file settings service to process it
        writeTestFile(service.operatorSettingsFile(), "{}");

        clearInvocations(csAdminClient);
        clearInvocations(spiedController);

        // we haven't fetched the node infos ever, since we haven't done any file processing
        assertNull(service.nodeInfos());

        // call the processing twice
        service.processFileSettings(service.operatorSettingsFile()).whenComplete((o, e) -> {
            if (e != null) {
                fail("shouldn't get an exception");
            }
        });
        // after the first processing we should have node infos
        assertEquals(1, service.nodeInfos().getNodes().size());

        service.processFileSettings(service.operatorSettingsFile()).whenComplete((o, e) -> {
            if (e != null) {
                fail("shouldn't get an exception");
            }
        });

        // node infos should have been fetched only once
        verify(csAdminClient, times(1)).nodesInfo(any(), any());
        verify(spiedController, times(2)).process(any(), any(ReservedStateChunk.class), any());

        // pretend we added a new node

        final DiscoveryNode localNode = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT);

        NodeInfo localNodeInfo = new NodeInfo(
            Version.CURRENT,
            Build.CURRENT,
            localNode,
            Settings.EMPTY,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            new IngestInfo(Collections.singletonList(new ProcessorInfo("set"))),
            null,
            null
        );
        var newResponse = new NodesInfoResponse(new ClusterName("elasticsearch"), List.of(nodeInfo, localNodeInfo), List.of());

        final ClusterState prevState = clusterService.state();
        final ClusterState clusterState = ClusterState.builder(prevState)
            .nodes(
                DiscoveryNodes.builder(prevState.getNodes()).add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId())
            )
            .build();

        ClusterChangedEvent event = new ClusterChangedEvent("transport", clusterState, prevState);
        assertTrue(event.nodesChanged());
        service.clusterChanged(event);

        doAnswer(i -> {
            ((ActionListener<NodesInfoResponse>) i.getArgument(1)).onResponse(newResponse);
            return null;
        }).when(csAdminClient).nodesInfo(any(), any());

        // this wouldn't change yet, node fetch transport action is invoked on demand, when we need to process file changes,
        // not every time we update the cluster state
        assertEquals(1, service.nodeInfos().getNodes().size());

        // call the processing twice
        service.processFileSettings(service.operatorSettingsFile()).whenComplete((o, e) -> {
            if (e != null) {
                fail("shouldn't get an exception");
            }
        });

        assertEquals(2, service.nodeInfos().getNodes().size());

        service.processFileSettings(service.operatorSettingsFile()).whenComplete((o, e) -> {
            if (e != null) {
                fail("shouldn't get an exception");
            }
        });

        assertEquals(2, service.nodeInfos().getNodes().size());

        // node infos should have been fetched one more time
        verify(csAdminClient, times(2)).nodesInfo(any(), any());
        verify(spiedController, times(4)).process(any(), any(ReservedStateChunk.class), any());
    }

    public void testRegisterWatchKeyRetry() throws IOException, InterruptedException {
        var service = spy(fileSettingsService);
        doAnswer(i -> 0L).when(service).retryDelayMillis(anyInt());

        Files.createDirectories(service.operatorSettingsDir());

        var mockedPath = spy(service.operatorSettingsDir());
        var prevWatchKey = mock(WatchKey.class);
        var newWatchKey = mock(WatchKey.class);

        doThrow(new IOException("can't register")).doThrow(new IOException("can't register - attempt 2"))
            .doAnswer(i -> newWatchKey)
            .when(mockedPath)
            .register(any(), any());

        var result = service.enableSettingsWatcher(prevWatchKey, mockedPath);
        assertNotNull(result);
        assertTrue(result != prevWatchKey);

        verify(service, times(2)).retryDelayMillis(anyInt());
    }

    // helpers
    private void writeTestFile(Path path, String contents) throws IOException {
        Path tempFilePath = createTempFile();

        Files.write(tempFilePath, contents.getBytes(StandardCharsets.UTF_8));
        Files.move(tempFilePath, path, StandardCopyOption.ATOMIC_MOVE);
    }
}
