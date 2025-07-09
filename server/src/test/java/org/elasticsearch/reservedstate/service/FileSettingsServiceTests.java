/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate.service;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.tests.util.LuceneTestCase;
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
import org.elasticsearch.common.file.AbstractFileWatchingService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.BuildVersion;
import org.elasticsearch.env.Environment;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.reservedstate.action.ReservedClusterSettingsAction;
import org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthTracker;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.hasEntry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class FileSettingsServiceTests extends ESTestCase {
    private static final Logger logger = LogManager.getLogger(FileSettingsServiceTests.class);
    private Environment env;
    private ClusterService clusterService;
    private ReservedClusterStateService controller;
    private ThreadPool threadpool;
    private FileSettingsService fileSettingsService;
    private FileSettingsHealthTracker healthIndicatorTracker;
    private Path watchedFile;

    /**
     * We're not testing health info publication here.
     */
    public static final FileSettingsHealthIndicatorPublisher NOOP_PUBLISHER = (f, a) -> {};

    @Before
    public void setUp() throws Exception {
        super.setUp();
        // TODO remove me once https://github.com/elastic/elasticsearch/issues/115280 is closed
        Loggers.setLevel(LogManager.getLogger(AbstractFileWatchingService.class), Level.DEBUG);

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

        Files.createDirectories(env.configDir());

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        controller = spy(
            new ReservedClusterStateService(
                clusterService,
                mock(RerouteService.class),
                List.of(new ReservedClusterSettingsAction(clusterSettings)),
                List.of()
            )
        );
        healthIndicatorTracker = spy(new FileSettingsHealthTracker(Settings.EMPTY, NOOP_PUBLISHER));
        fileSettingsService = spy(new FileSettingsService(clusterService, controller, env, healthIndicatorTracker));
        watchedFile = fileSettingsService.watchedFile();
    }

    @After
    public void tearDown() throws Exception {
        try {
            if (fileSettingsService.lifecycleState() == Lifecycle.State.STARTED) {
                logger.info("Stopping file settings service");
                fileSettingsService.stop();
            }
            if (fileSettingsService.lifecycleState() == Lifecycle.State.STOPPED) {
                logger.info("Closing file settings service");
                fileSettingsService.close();
            }

            super.tearDown();
            clusterService.close();
            threadpool.shutdownNow();
        } finally {
            // TODO remove me once https://github.com/elastic/elasticsearch/issues/115280 is closed
            Loggers.setLevel(LogManager.getLogger(AbstractFileWatchingService.class), Level.INFO);
        }
    }

    public void testStartStop() {
        fileSettingsService.start();
        assertFalse(fileSettingsService.watching());
        fileSettingsService.clusterChanged(new ClusterChangedEvent("test", clusterService.state(), ClusterState.EMPTY_STATE));
        assertTrue(fileSettingsService.watching());
        fileSettingsService.stop();
        assertFalse(fileSettingsService.watching());
        verify(healthIndicatorTracker, times(1)).startOccurred();
        verify(healthIndicatorTracker, times(1)).stopOccurred();
    }

    public void testOperatorDirName() {
        Path operatorPath = fileSettingsService.watchedFileDir();
        assertTrue(operatorPath.startsWith(env.configDir()));
        assertTrue(operatorPath.endsWith("operator"));

        Path operatorSettingsFile = fileSettingsService.watchedFile();
        assertTrue(operatorSettingsFile.startsWith(operatorPath));
        assertTrue(operatorSettingsFile.endsWith("settings.json"));
    }

    @SuppressWarnings("unchecked")
    public void testInitialFileError() throws Exception {
        doAnswer(i -> {
            ((Consumer<Exception>) i.getArgument(3)).accept(new IllegalStateException("Some exception"));
            return null;
        }).when(controller).process(any(), any(XContentParser.class), eq(randomFrom(ReservedStateVersionCheck.values())), any());

        Answer<?> checkExecute = i -> {
            i.callRealMethod();  // should throw an exception
            fail(i.getMethod().getName() + " should have thrown an exception");
            return null;
        };
        doAnswer(checkExecute).when(fileSettingsService).processInitialFilesMissing();
        doAnswer(checkExecute).when(fileSettingsService).processFile(eq(watchedFile), eq(false));

        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(i -> {
            try {
                return i.callRealMethod();
            } finally {
                latch.countDown();
            }
        }).when(fileSettingsService).processFile(eq(watchedFile), eq(true));

        Files.createDirectories(fileSettingsService.watchedFileDir());
        // contents of the JSON don't matter, we just need a file to exist
        writeTestFile(watchedFile, "{}");

        fileSettingsService.start();
        fileSettingsService.clusterChanged(new ClusterChangedEvent("test", clusterService.state(), ClusterState.EMPTY_STATE));

        // wait until the watcher thread has started, and it has discovered the file
        assertTrue(latch.await(20, TimeUnit.SECONDS));

        // Note: the name "processFileOnServiceStart" is a bit misleading because it is not
        // referring to fileSettingsService.start(). Rather, it is referring to the initialization
        // of the watcher thread itself, which occurs asynchronously when clusterChanged is first called.
        verify(fileSettingsService, times(1)).processFile(eq(watchedFile), eq(true));
        verify(controller, times(1)).process(any(), any(XContentParser.class), eq(ReservedStateVersionCheck.HIGHER_OR_SAME_VERSION), any());

        assertEquals(YELLOW, currentHealthIndicatorResult().status());
        verify(healthIndicatorTracker, times(1)).changeOccurred();
        verify(healthIndicatorTracker, times(1)).failureOccurred(argThat(s -> s.startsWith(IllegalStateException.class.getName())));
    }

    @SuppressWarnings("unchecked")
    public void testInitialFileWorks() throws Exception {
        // Let's check that if we didn't throw an error that everything works
        doAnswer(i -> {
            ((Consumer<Exception>) i.getArgument(3)).accept(null);
            return null;
        }).when(controller).process(any(), any(XContentParser.class), any(), any());

        CountDownLatch processFileLatch = new CountDownLatch(1);
        Answer<?> checkExecute = i -> {
            try {
                return i.callRealMethod();
            } finally {
                processFileLatch.countDown();
            }
        };
        doAnswer(checkExecute).when(fileSettingsService).processFile(eq(watchedFile), eq(true));

        Files.createDirectories(fileSettingsService.watchedFileDir());
        // contents of the JSON don't matter, we just need a file to exist
        writeTestFile(watchedFile, "{}");

        fileSettingsService.start();
        fileSettingsService.clusterChanged(new ClusterChangedEvent("test", clusterService.state(), ClusterState.EMPTY_STATE));

        longAwait(processFileLatch);

        verify(fileSettingsService, times(1)).processFile(eq(watchedFile), eq(true));
        verify(controller, times(1)).process(any(), any(XContentParser.class), eq(ReservedStateVersionCheck.HIGHER_OR_SAME_VERSION), any());

        assertEquals(GREEN, currentHealthIndicatorResult().status());
        verify(healthIndicatorTracker, times(1)).changeOccurred();
        verify(healthIndicatorTracker, times(1)).successOccurred();
    }

    @SuppressWarnings("unchecked")
    public void testProcessFileChanges() throws Exception {
        doAnswer(i -> {
            ((Consumer<Exception>) i.getArgument(3)).accept(null);
            return null;
        }).when(controller).process(any(), any(XContentParser.class), any(), any());

        // Await on some latches when files change so we can sync up
        CountDownLatch processFileCreationLatch = new CountDownLatch(1);
        doAnswer(i -> {
            try {
                return i.callRealMethod();
            } finally {
                processFileCreationLatch.countDown();
            }
        }).when(fileSettingsService).processFile(eq(watchedFile), eq(true));
        CountDownLatch processFileChangeLatch = new CountDownLatch(1);
        doAnswer(i -> {
            try {
                return i.callRealMethod();
            } finally {
                processFileChangeLatch.countDown();
            }
        }).when(fileSettingsService).processFile(eq(watchedFile), eq(false));

        Files.createDirectories(fileSettingsService.watchedFileDir());
        // contents of the JSON don't matter, we just need a file to exist
        writeTestFile(watchedFile, "{}");

        // It's important to configure all the mocks before calling start() here,
        // because otherwise there can be races between configuration and use of mocks
        // which leads to a UnfinishedStubbingException.

        fileSettingsService.start();
        fileSettingsService.clusterChanged(new ClusterChangedEvent("test", clusterService.state(), ClusterState.EMPTY_STATE));

        longAwait(processFileCreationLatch);

        verify(fileSettingsService, times(1)).processFile(eq(watchedFile), eq(true));
        verify(controller, times(1)).process(any(), any(XContentParser.class), eq(ReservedStateVersionCheck.HIGHER_OR_SAME_VERSION), any());

        // Touch the file to get an update
        Instant now = LocalDateTime.now(ZoneId.systemDefault()).toInstant(ZoneOffset.ofHours(0));
        Files.setLastModifiedTime(watchedFile, FileTime.from(now));

        longAwait(processFileChangeLatch);

        verify(fileSettingsService, times(1)).processFile(eq(watchedFile), eq(false));
        verify(controller, times(1)).process(any(), any(XContentParser.class), eq(ReservedStateVersionCheck.HIGHER_VERSION_ONLY), any());

        assertEquals(GREEN, currentHealthIndicatorResult().status());
        verify(healthIndicatorTracker, times(2)).changeOccurred();
        verify(healthIndicatorTracker, times(2)).successOccurred();
    }

    public void testInvalidJSON() throws Exception {
        // Chop off the functionality so we don't run too much of the actual cluster logic that we're not testing
        doNothing().when(controller).updateErrorState(any());
        doAnswer(i -> { throw new AssertionError("Parse error should happen before this process method is called"); }).when(controller)
            .process(any(), any(ReservedStateChunk.class), any(), any());

        // Don't really care about the initial state
        Files.createDirectories(fileSettingsService.watchedFileDir());
        doNothing().when(fileSettingsService).processInitialFilesMissing();

        // Prepare to await on a barrier when the file changes so we can sync up
        CyclicBarrier fileChangeBarrier = new CyclicBarrier(2);
        doAnswer((Answer<?>) invocation -> {
            try {
                return invocation.callRealMethod();
            } finally {
                awaitOrBust(fileChangeBarrier);
            }
        }).when(fileSettingsService).onProcessFileChangesException(eq(watchedFile), any());

        // Kick off the service
        fileSettingsService.start();
        fileSettingsService.clusterChanged(new ClusterChangedEvent("test", clusterService.state(), ClusterState.EMPTY_STATE));

        // Now break the JSON and wait
        writeTestFile(watchedFile, "test_invalid_JSON");
        awaitOrBust(fileChangeBarrier);

        // These checks use atLeast(1) because the initial JSON is also invalid,
        // and so we sometimes get two calls to these error-reporting methods
        // depending on timing. Rather than trace down the root cause and fix
        // it, we tolerate this for now because, hey, invalid JSON is invalid JSON
        // and this is still testing what we want to test.

        verify(fileSettingsService, Mockito.atLeast(1)).onProcessFileChangesException(
            eq(watchedFile),
            argThat(e -> unwrapException(e) instanceof XContentParseException)
        );

        assertEquals(YELLOW, currentHealthIndicatorResult().status());
        verify(healthIndicatorTracker, Mockito.atLeast(1)).failureOccurred(contains(XContentParseException.class.getName()));
    }

    /**
     * Looks for the ultimate cause of {@code e} by stripping off layers of bookkeeping exception wrappers.
     */
    private Throwable unwrapException(Throwable e) {
        while (e != null) {
            if (e instanceof ExecutionException || e instanceof IllegalStateException) {
                e = e.getCause();
            } else {
                break;
            }
        }
        return e;
    }

    private static void awaitOrBust(CyclicBarrier barrier) {
        try {
            barrier.await(20, TimeUnit.SECONDS);
        } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
            throw new AssertionError("Unexpected exception waiting for barrier", e);
        }
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
            invocation.getArgument(1, ActionListener.class).onResponse(null);
            return null;
        }).when(controller).initEmpty(any(String.class), any());

        fileSettingsService.start();
        fileSettingsService.clusterChanged(new ClusterChangedEvent("test", clusterService.state(), ClusterState.EMPTY_STATE));
        assertTrue(fileSettingsService.watching());

        Files.createDirectories(fileSettingsService.watchedFileDir());

        // Make some fake settings file to cause the file settings service to process it
        writeTestFile(watchedFile, "{}");

        longAwait(processFileLatch);

        // Stopping the service should interrupt the watcher thread, we should be able to stop
        fileSettingsService.stop();
        assertFalse(fileSettingsService.watching());
        fileSettingsService.close();

        // When the service is stopped, the health indicator should be green
        assertEquals(GREEN, currentHealthIndicatorResult().status());
        verify(healthIndicatorTracker).stopOccurred();

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
        writeTestFile(watchedFile, "{}");
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
    private static void writeTestFile(Path path, String contents) throws IOException {
        logger.info("Writing settings file under [{}]", path.toAbsolutePath());
        Path tempFilePath = createTempFile();
        Files.writeString(tempFilePath, contents);
        try {
            Files.move(tempFilePath, path, REPLACE_EXISTING, ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            logger.info("Atomic move not available. Falling back on non-atomic move to write [{}]", path.toAbsolutePath());
            Files.move(tempFilePath, path, REPLACE_EXISTING);
        }
    }

    // this waits for up to 20 seconds to account for watcher service differences between OSes
    // on MacOS it may take up to 10 seconds for the Java watcher service to notice the file,
    // on Linux is instantaneous. Windows is instantaneous too.
    private static void longAwait(CountDownLatch latch) {
        try {
            assertTrue("longAwait: CountDownLatch did not reach zero within the timeout", latch.await(20, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e, "longAwait: interrupted waiting for CountDownLatch to reach zero");
        }
    }

    private HealthIndicatorResult currentHealthIndicatorResult() {
        return new FileSettingsService.FileSettingsHealthIndicatorService().calculate(healthIndicatorTracker.getCurrentInfo());
    }

}
