/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.BuildVersion;
import org.elasticsearch.env.Environment;
import org.elasticsearch.reservedstate.action.ReservedClusterSettingsAction;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.After;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.hasEntry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class FileSettingsServiceTests extends ESTestCase {
    private Environment env;
    private ClusterService clusterService;
    private ReservedClusterStateService controller;
    private ThreadPool threadpool;
    private FileSettingsService fileSettingsService;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        threadpool = new TestThreadPool("file_settings_service_tests");

        clusterService = new ClusterService(
            Settings.builder().put(NODE_NAME_SETTING.getKey(), "test").build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadpool,
            new TaskManager(Settings.EMPTY, threadpool, Set.of())
        );

        DiscoveryNode localNode = DiscoveryNodeUtils.create("node");
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()))
            .build();

        clusterService.setNodeConnectionsService(mock(NodeConnectionsService.class));
        clusterService.getClusterApplierService().setInitialState(clusterState);
        clusterService.getMasterService().setClusterStatePublisher((e, pl, al) -> {
            ClusterServiceUtils.setAllElapsedMillis(e);
            al.onCommit(TimeValue.ZERO);
            for (DiscoveryNode node : e.getNewState().nodes()) {
                al.onNodeAck(node, null);
            }
            pl.onResponse(null);
        });
        clusterService.getMasterService().setClusterStateSupplier(() -> clusterState);
        env = newEnvironment(Settings.EMPTY);

        Files.createDirectories(env.configFile());

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        controller = spy(
            new ReservedClusterStateService(
                clusterService,
                mock(RerouteService.class),
                List.of(new ReservedClusterSettingsAction(clusterSettings))
            )
        );
        fileSettingsService = spy(new FileSettingsService(clusterService, controller, env));
    }

    @After
    public void tearDown() throws Exception {
        if (fileSettingsService.lifecycleState() == Lifecycle.State.STARTED) {
            fileSettingsService.stop();
        }
        if (fileSettingsService.lifecycleState() == Lifecycle.State.STOPPED) {
            fileSettingsService.close();
        }

        super.tearDown();
        clusterService.close();
        threadpool.shutdownNow();
    }

    public void testStartStop() {
        fileSettingsService.start();
        assertFalse(fileSettingsService.watching());
        fileSettingsService.clusterChanged(new ClusterChangedEvent("test", clusterService.state(), ClusterState.EMPTY_STATE));
        assertTrue(fileSettingsService.watching());
        fileSettingsService.stop();
        assertFalse(fileSettingsService.watching());
    }

    public void testOperatorDirName() {
        Path operatorPath = fileSettingsService.watchedFileDir();
        assertTrue(operatorPath.startsWith(env.configFile()));
        assertTrue(operatorPath.endsWith("operator"));

        Path operatorSettingsFile = fileSettingsService.watchedFile();
        assertTrue(operatorSettingsFile.startsWith(operatorPath));
        assertTrue(operatorSettingsFile.endsWith("settings.json"));
    }

    @SuppressWarnings("unchecked")
    public void testInitialFileError() throws Exception {
        doAnswer((Answer<Void>) invocation -> {
            ((Consumer<Exception>) invocation.getArgument(3)).accept(new IllegalStateException("Some exception"));
            return null;
        }).when(controller).process(any(), any(XContentParser.class), eq(randomFrom(ReservedStateVersionCheck.values())), any());

        AtomicBoolean settingsChanged = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        fileSettingsService.addFileChangedListener(() -> settingsChanged.set(true));

        doAnswer((Answer<?>) invocation -> {
            try {
                return invocation.callRealMethod();
            } finally {
                latch.countDown();
            }
        }).when(fileSettingsService).processFileOnServiceStart();

        Files.createDirectories(fileSettingsService.watchedFileDir());
        // contents of the JSON don't matter, we just need a file to exist
        writeTestFile(fileSettingsService.watchedFile(), "{}");

        fileSettingsService.start();
        fileSettingsService.clusterChanged(new ClusterChangedEvent("test", clusterService.state(), ClusterState.EMPTY_STATE));

        // wait until the watcher thread has started, and it has discovered the file
        assertTrue(latch.await(20, TimeUnit.SECONDS));

        verify(fileSettingsService, times(1)).processFileOnServiceStart();
        verify(controller, times(1)).process(any(), any(XContentParser.class), eq(ReservedStateVersionCheck.HIGHER_OR_SAME_VERSION), any());
        // assert we never notified any listeners of successful application of file based settings
        assertFalse(settingsChanged.get());
    }

    @SuppressWarnings("unchecked")
    public void testInitialFileWorks() throws Exception {
        // Let's check that if we didn't throw an error that everything works
        doAnswer((Answer<Void>) invocation -> {
            ((Consumer<Exception>) invocation.getArgument(3)).accept(null);
            return null;
        }).when(controller).process(any(), any(XContentParser.class), any(), any());

        CountDownLatch latch = new CountDownLatch(1);

        fileSettingsService.addFileChangedListener(latch::countDown);

        Files.createDirectories(fileSettingsService.watchedFileDir());
        // contents of the JSON don't matter, we just need a file to exist
        writeTestFile(fileSettingsService.watchedFile(), "{}");

        doAnswer((Answer<?>) invocation -> {
            try {
                return invocation.callRealMethod();
            } finally {
                latch.countDown();
            }
        }).when(fileSettingsService).processFileOnServiceStart();

        fileSettingsService.start();
        fileSettingsService.clusterChanged(new ClusterChangedEvent("test", clusterService.state(), ClusterState.EMPTY_STATE));

        // wait for listener to be called
        assertTrue(latch.await(20, TimeUnit.SECONDS));

        verify(fileSettingsService, times(1)).processFileOnServiceStart();
        verify(controller, times(1)).process(any(), any(XContentParser.class), eq(ReservedStateVersionCheck.HIGHER_OR_SAME_VERSION), any());
    }

    @SuppressWarnings("unchecked")
    public void testProcessFileChanges() throws Exception {
        doAnswer((Answer<Void>) invocation -> {
            ((Consumer<Exception>) invocation.getArgument(3)).accept(null);
            return null;
        }).when(controller).process(any(), any(XContentParser.class), any(), any());

        // we get three events: initial clusterChanged event, first write, second write
        CountDownLatch latch = new CountDownLatch(3);

        fileSettingsService.addFileChangedListener(latch::countDown);

        Files.createDirectories(fileSettingsService.watchedFileDir());
        // contents of the JSON don't matter, we just need a file to exist
        writeTestFile(fileSettingsService.watchedFile(), "{}");

        doAnswer((Answer<?>) invocation -> {
            try {
                return invocation.callRealMethod();
            } finally {
                latch.countDown();
            }
        }).when(fileSettingsService).processFileOnServiceStart();
        doAnswer((Answer<?>) invocation -> {
            try {
                return invocation.callRealMethod();
            } finally {
                latch.countDown();
            }
        }).when(fileSettingsService).processFileChanges();

        fileSettingsService.start();
        fileSettingsService.clusterChanged(new ClusterChangedEvent("test", clusterService.state(), ClusterState.EMPTY_STATE));
        // second file change; contents still don't matter
        overwriteTestFile(fileSettingsService.watchedFile(), "{}");

        // wait for listener to be called (once for initial processing, once for subsequent update)
        assertTrue(latch.await(20, TimeUnit.SECONDS));

        verify(fileSettingsService, times(1)).processFileOnServiceStart();
        verify(controller, times(1)).process(any(), any(XContentParser.class), eq(ReservedStateVersionCheck.HIGHER_OR_SAME_VERSION), any());
        verify(fileSettingsService, times(1)).processFileChanges();
        verify(controller, times(1)).process(any(), any(XContentParser.class), eq(ReservedStateVersionCheck.HIGHER_VERSION_ONLY), any());
    }

    @SuppressWarnings("unchecked")
    public void testStopWorksInMiddleOfProcessing() throws Exception {
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
            return new ReservedStateChunk(Map.of(), new ReservedStateVersion(1L, BuildVersion.current()));
        }).when(controller).parse(any(String.class), any());

        doAnswer((Answer<Void>) invocation -> {
            var completionListener = invocation.getArgument(1, ActionListener.class);
            completionListener.onResponse(null);
            return null;
        }).when(controller).initEmpty(any(String.class), any());

        fileSettingsService.start();
        fileSettingsService.clusterChanged(new ClusterChangedEvent("test", clusterService.state(), ClusterState.EMPTY_STATE));
        assertTrue(fileSettingsService.watching());

        Files.createDirectories(fileSettingsService.watchedFileDir());

        // Make some fake settings file to cause the file settings service to process it
        writeTestFile(fileSettingsService.watchedFile(), "{}");

        // we need to wait a bit, on MacOS it may take up to 10 seconds for the Java watcher service to notice the file,
        // on Linux is instantaneous. Windows is instantaneous too.
        assertTrue(processFileLatch.await(30, TimeUnit.SECONDS));

        // Stopping the service should interrupt the watcher thread, we should be able to stop
        fileSettingsService.stop();
        assertFalse(fileSettingsService.watching());
        fileSettingsService.close();
        // let the deadlocked thread end, so we can cleanly exit the test
        deadThreadLatch.countDown();
    }

    public void testHandleSnapshotRestoreClearsMetadata() throws Exception {
        ClusterState state = ClusterState.builder(clusterService.state())
            .metadata(
                Metadata.builder(clusterService.state().metadata())
                    .put(new ReservedStateMetadata(FileSettingsService.NAMESPACE, 1L, Map.of(), null))
                    .build()
            )
            .build();

        Metadata.Builder metadata = Metadata.builder(state.metadata());
        fileSettingsService.handleSnapshotRestore(state, metadata);

        assertThat(metadata.build().reservedStateMetadata(), anEmptyMap());
    }

    public void testHandleSnapshotRestoreResetsMetadata() throws Exception {
        fileSettingsService.start();
        fileSettingsService.clusterChanged(new ClusterChangedEvent("test", clusterService.state(), ClusterState.EMPTY_STATE));

        Files.createDirectories(fileSettingsService.watchedFileDir());
        // contents of the JSON don't matter, we just need a file to exist
        writeTestFile(fileSettingsService.watchedFile(), "{}");
        assertTrue(fileSettingsService.watching());

        ClusterState state = ClusterState.builder(clusterService.state())
            .metadata(
                Metadata.builder(clusterService.state().metadata())
                    .put(new ReservedStateMetadata(FileSettingsService.NAMESPACE, 1L, Map.of(), null))
                    .build()
            )
            .build();

        Metadata.Builder metadata = Metadata.builder();
        fileSettingsService.handleSnapshotRestore(state, metadata);

        assertThat(
            metadata.build().reservedStateMetadata(),
            hasEntry(
                FileSettingsService.NAMESPACE,
                new ReservedStateMetadata(FileSettingsService.NAMESPACE, ReservedStateMetadata.RESTORED_VERSION, Map.of(), null)
            )
        );
    }

    // helpers
    private void writeTestFile(Path path, String contents) throws IOException {
        Path tempFilePath = createTempFile();
        Files.writeString(tempFilePath, contents);
        Files.move(tempFilePath, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    private void overwriteTestFile(Path path, String contents) throws IOException {
        Path tempFilePath = createTempFile();
        Files.writeString(tempFilePath, contents);
        Files.move(tempFilePath, path, StandardCopyOption.REPLACE_EXISTING);
    }
}
