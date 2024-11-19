/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate.service;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ReservedStateErrorMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateHandlerMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.reservedstate.action.ReservedClusterSettingsAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ReservedClusterStateServiceTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    private static <T extends ClusterStateTaskListener> MasterServiceTaskQueue<T> mockTaskQueue() {
        return (MasterServiceTaskQueue<T>) mock(MasterServiceTaskQueue.class);
    }

    private static class TestTaskContext<T extends ClusterStateTaskListener> implements ClusterStateTaskExecutor.TaskContext<T> {
        private final T task;

        private TestTaskContext(T task) {
            this.task = task;
        }

        @Override
        public T getTask() {
            return task;
        }

        @Override
        public void success(Runnable onPublicationSuccess) {
            onPublicationSuccess.run();
        }

        @Override
        public void success(Consumer<ClusterState> publishedStateConsumer) {}

        @Override
        public void success(Runnable onPublicationSuccess, ClusterStateAckListener clusterStateAckListener) {}

        @Override
        public void success(Consumer<ClusterState> publishedStateConsumer, ClusterStateAckListener clusterStateAckListener) {}

        @Override
        public void onFailure(Exception failure) {}

        @Override
        public Releasable captureResponseHeaders() {
            return null;
        }
    }

    private static class TestStateHandler implements ReservedClusterStateHandler<Map<String, Object>> {
        private final String name;

        private TestStateHandler(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public TransformState transform(Object source, TransformState prevState) throws Exception {
            ClusterState newState = new ClusterState.Builder(prevState.state()).build();
            return new TransformState(newState, prevState.keys());
        }

        @Override
        public Map<String, Object> fromXContent(XContentParser parser) throws IOException {
            return parser.map();
        }
    }

    public void testOperatorController() throws IOException {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.createTaskQueue(any(), any(), any())).thenReturn(mockTaskQueue());
        final ClusterName clusterName = new ClusterName("elasticsearch");

        ClusterState state = ClusterState.builder(clusterName).build();
        when(clusterService.state()).thenReturn(state);

        ReservedClusterStateService controller = new ReservedClusterStateService(
            clusterService,
            mock(RerouteService.class),
            List.of(new ReservedClusterSettingsAction(clusterSettings))
        );

        String testJSON = """
            {
                 "metadata": {
                     "version": "1234",
                     "compatibility": "8.4.0"
                 },
                 "state": {
                     "cluster_settings": {
                         "indices.recovery.max_bytes_per_sec": "50mb"

                 }
            }
            """;

        AtomicReference<Exception> x = new AtomicReference<>();

        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, testJSON)) {
            controller.process(
                "operator",
                parser,
                randomFrom(ReservedStateVersionCheck.HIGHER_VERSION_ONLY, ReservedStateVersionCheck.HIGHER_OR_SAME_VERSION),
                x::set
            );

            assertThat(x.get(), instanceOf(IllegalStateException.class));
            assertThat(x.get().getMessage(), containsString("Error processing state change request for operator"));
        }

        testJSON = """
            {
                 "metadata": {
                     "version": "1234",
                     "compatibility": "8.4.0"
                 },
                 "state": {
                     "cluster_settings": {
                         "indices.recovery.max_bytes_per_sec": "50mb",
                         "cluster": {
                             "remote": {
                                 "cluster_one": {
                                     "seeds": [
                                         "127.0.0.1:9300"
                                     ]
                                 }
                             }
                         }
                     }
                 }
            }
            """;

        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, testJSON)) {
            controller.process(
                "operator",
                parser,
                randomFrom(ReservedStateVersionCheck.HIGHER_VERSION_ONLY, ReservedStateVersionCheck.HIGHER_OR_SAME_VERSION),
                Assert::assertNull
            );
        }
    }

    public void testInitEmptyTask() {
        ClusterService clusterService = mock(ClusterService.class);

        ArgumentCaptor<ReservedStateUpdateTask> updateTask = ArgumentCaptor.captor();

        // grab the update task when it gets given to us
        when(clusterService.createTaskQueue(ArgumentMatchers.contains("reserved state update"), any(), any())).thenAnswer(i -> {
            MasterServiceTaskQueue<ReservedStateUpdateTask> queue = mockTaskQueue();
            doNothing().when(queue).submitTask(any(), updateTask.capture(), any());
            return queue;
        });

        ReservedClusterStateService service = new ReservedClusterStateService(clusterService, mock(RerouteService.class), List.of());
        service.initEmpty("namespace", ActionListener.noop());

        assertThat(updateTask.getValue(), notNullValue());
        ClusterState state = ClusterState.builder(new ClusterName("test")).build();
        ClusterState updatedState = updateTask.getValue().execute(state);

        assertThat(
            updatedState.metadata().reservedStateMetadata(),
            equalTo(Map.of("namespace", new ReservedStateMetadata("namespace", ReservedStateMetadata.EMPTY_VERSION, Map.of(), null)))
        );
    }

    public void testUpdateStateTasks() throws Exception {
        RerouteService rerouteService = mock(RerouteService.class);

        ClusterState state = ClusterState.builder(new ClusterName("test")).build();

        ReservedStateUpdateTaskExecutor taskExecutor = new ReservedStateUpdateTaskExecutor(rerouteService);

        AtomicBoolean successCalled = new AtomicBoolean(false);

        ReservedStateUpdateTask task = spy(
            new ReservedStateUpdateTask(
                "test",
                null,
                ReservedStateVersionCheck.HIGHER_VERSION_ONLY,
                Map.of(),
                Set.of(),
                errorState -> {},
                ActionListener.noop()
            )
        );

        doReturn(state).when(task).execute(any());

        ClusterStateTaskExecutor.TaskContext<ReservedStateUpdateTask> taskContext = new TestTaskContext<>(task) {
            @Override
            public void success(Runnable onPublicationSuccess) {
                super.success(onPublicationSuccess);
                successCalled.set(true);
            }
        };

        ClusterState newState = taskExecutor.execute(
            new ClusterStateTaskExecutor.BatchExecutionContext<>(state, List.of(taskContext), () -> null)
        );
        assertEquals(state, newState);
        assertTrue(successCalled.get());
        verify(task, times(1)).execute(any());

        taskExecutor.clusterStatePublished(state);
        verify(rerouteService, times(1)).reroute(anyString(), any(), any());
    }

    public void testUpdateErrorState() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = ClusterState.builder(new ClusterName("test")).build();

        ArgumentCaptor<ReservedStateErrorTask> updateTask = ArgumentCaptor.captor();
        MasterServiceTaskQueue<ReservedStateErrorTask> errorQueue = mockTaskQueue();
        doNothing().when(errorQueue).submitTask(any(), updateTask.capture(), any());

        // grab the update task when it gets given to us
        when(clusterService.<ReservedStateErrorTask>createTaskQueue(ArgumentMatchers.contains("reserved state error"), any(), any()))
            .thenReturn(errorQueue);
        when(clusterService.state()).thenReturn(state);

        ReservedClusterStateService service = new ReservedClusterStateService(clusterService, mock(RerouteService.class), List.of());

        ErrorState error = new ErrorState(
            "namespace",
            2L,
            ReservedStateVersionCheck.HIGHER_VERSION_ONLY,
            List.of("error"),
            ReservedStateErrorMetadata.ErrorKind.TRANSIENT
        );
        service.updateErrorState(error);

        assertThat(updateTask.getValue(), notNullValue());
        verify(errorQueue).submitTask(any(), any(), any());

        ClusterState updatedState = updateTask.getValue().execute(state);
        assertThat(
            updatedState.metadata().reservedStateMetadata().get("namespace"),
            equalTo(
                new ReservedStateMetadata(
                    "namespace",
                    ReservedStateMetadata.NO_VERSION,
                    Map.of(),
                    new ReservedStateErrorMetadata(2L, ReservedStateErrorMetadata.ErrorKind.TRANSIENT, List.of("error"))
                )
            )
        );

        // it should not update if the error version is less than the current version
        when(clusterService.state()).thenReturn(updatedState);
        ErrorState oldError = new ErrorState(
            "namespace",
            1L,
            ReservedStateVersionCheck.HIGHER_VERSION_ONLY,
            List.of("old error"),
            ReservedStateErrorMetadata.ErrorKind.TRANSIENT
        );
        service.updateErrorState(oldError);
        verifyNoMoreInteractions(errorQueue);
    }

    public void testErrorStateTask() throws Exception {
        ClusterState state = ClusterState.builder(new ClusterName("test")).build();

        final var listenerCompleted = new AtomicBoolean(false);

        ReservedStateErrorTask task = spy(
            new ReservedStateErrorTask(
                new ErrorState(
                    "test",
                    1L,
                    ReservedStateVersionCheck.HIGHER_VERSION_ONLY,
                    List.of("some parse error", "some io error"),
                    ReservedStateErrorMetadata.ErrorKind.PARSING
                ),
                ActionListener.running(() -> listenerCompleted.set(true))
            )
        );

        ClusterState newState = new ReservedStateErrorTaskExecutor().execute(
            new ClusterStateTaskExecutor.BatchExecutionContext<>(state, List.of(new TestTaskContext<>(task)), () -> null)
        );

        verify(task, times(1)).execute(any());

        ReservedStateMetadata operatorMetadata = newState.metadata().reservedStateMetadata().get("test");
        assertNotNull(operatorMetadata);
        assertNotNull(operatorMetadata.errorMetadata());
        assertThat(operatorMetadata.errorMetadata().version(), is(1L));
        assertThat(operatorMetadata.errorMetadata().errorKind(), is(ReservedStateErrorMetadata.ErrorKind.PARSING));
        assertThat(operatorMetadata.errorMetadata().errors(), contains("some parse error", "some io error"));
        assertTrue(listenerCompleted.get());
    }

    public void testUpdateTaskDuplicateError() {
        ReservedClusterStateHandler<Map<String, Object>> newStateMaker = new TestStateHandler("maker");
        ReservedClusterStateHandler<Map<String, Object>> exceptionThrower = new TestStateHandler("one") {
            @Override
            public TransformState transform(Object source, TransformState prevState) throws Exception {
                throw new Exception("anything");
            }
        };

        ReservedStateHandlerMetadata hmOne = new ReservedStateHandlerMetadata("one", Set.of("a", "b"));
        ReservedStateErrorMetadata emOne = new ReservedStateErrorMetadata(
            2L,
            ReservedStateErrorMetadata.ErrorKind.VALIDATION,
            List.of("Test error 1", "Test error 2")
        );

        final ReservedStateMetadata operatorMetadata = ReservedStateMetadata.builder("namespace_one")
            .errorMetadata(emOne)
            .version(1L)
            .putHandler(hmOne)
            .build();

        Metadata metadata = Metadata.builder().put(operatorMetadata).build();
        ClusterState state = ClusterState.builder(new ClusterName("test")).metadata(metadata).build();

        assertFalse(ReservedStateErrorTask.isNewError(operatorMetadata, 2L, ReservedStateVersionCheck.HIGHER_VERSION_ONLY));
        assertFalse(ReservedStateErrorTask.isNewError(operatorMetadata, 1L, ReservedStateVersionCheck.HIGHER_VERSION_ONLY));
        assertTrue(ReservedStateErrorTask.isNewError(operatorMetadata, 2L, ReservedStateVersionCheck.HIGHER_OR_SAME_VERSION));
        assertTrue(ReservedStateErrorTask.isNewError(operatorMetadata, 3L, ReservedStateVersionCheck.HIGHER_VERSION_ONLY));
        assertTrue(ReservedStateErrorTask.isNewError(null, 1L, ReservedStateVersionCheck.HIGHER_VERSION_ONLY));
        assertTrue(ReservedStateErrorTask.isNewError(null, 1L, ReservedStateVersionCheck.HIGHER_OR_SAME_VERSION));

        var chunk = new ReservedStateChunk(Map.of("one", "two", "maker", "three"), new ReservedStateVersion(2L, Version.CURRENT));
        var orderedHandlers = List.of(exceptionThrower.name(), newStateMaker.name());

        // We submit a task with two handler, one will cause an exception, the other will create a new state.
        // When we fail to update the metadata because of version, we ensure that the returned state is equal to the
        // original state by pointer reference to avoid cluster state update task to run.
        ReservedStateUpdateTask task = new ReservedStateUpdateTask(
            "namespace_one",
            chunk,
            ReservedStateVersionCheck.HIGHER_VERSION_ONLY,
            Map.of(exceptionThrower.name(), exceptionThrower, newStateMaker.name(), newStateMaker),
            orderedHandlers,
            errorState -> assertFalse(ReservedStateErrorTask.isNewError(operatorMetadata, errorState.version(), errorState.versionCheck())),
            ActionListener.noop()
        );

        ClusterService clusterService = mock(ClusterService.class);
        final var controller = spy(
            new ReservedClusterStateService(clusterService, mock(RerouteService.class), List.of(newStateMaker, exceptionThrower))
        );

        var trialRunErrors = controller.trialRun("namespace_one", state, chunk, new LinkedHashSet<>(orderedHandlers));
        assertThat(trialRunErrors, contains(containsString("Error processing one state change:")));

        // We exit on duplicate errors before we update the cluster state error metadata
        assertThat(
            expectThrows(IllegalStateException.class, () -> task.execute(state)).getMessage(),
            containsString("Error processing state change request for namespace_one")
        );

        emOne = new ReservedStateErrorMetadata(
            0L,
            ReservedStateErrorMetadata.ErrorKind.VALIDATION,
            List.of("Test error 1", "Test error 2")
        );

        // If we are writing with older error metadata, we should get proper IllegalStateException
        ReservedStateMetadata opMetadata = ReservedStateMetadata.builder("namespace_one")
            .errorMetadata(emOne)
            .version(0L)
            .putHandler(hmOne)
            .build();

        metadata = Metadata.builder().put(opMetadata).build();
        ClusterState newState = ClusterState.builder(new ClusterName("test")).metadata(metadata).build();

        // We exit on duplicate errors before we update the cluster state error metadata
        assertThat(
            expectThrows(IllegalStateException.class, () -> task.execute(newState)).getMessage(),
            containsString("Error processing state change request for namespace_one")
        );
    }

    public void testCheckMetadataVersion() {
        ReservedStateMetadata operatorMetadata = ReservedStateMetadata.builder("test").version(123L).build();

        ClusterState state = ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().put(operatorMetadata)).build();

        ReservedStateUpdateTask task = new ReservedStateUpdateTask(
            "test",
            new ReservedStateChunk(Map.of(), new ReservedStateVersion(124L, Version.CURRENT)),
            ReservedStateVersionCheck.HIGHER_VERSION_ONLY,
            Map.of(),
            List.of(),
            e -> {},
            ActionListener.noop()
        );
        assertThat("Cluster state should be modified", task.execute(state), not(sameInstance(state)));
        task = new ReservedStateUpdateTask(
            "test",
            new ReservedStateChunk(Map.of(), new ReservedStateVersion(124L, Version.CURRENT)),
            ReservedStateVersionCheck.HIGHER_VERSION_ONLY,
            Map.of(),
            List.of(),
            e -> {},
            ActionListener.noop()
        );
        assertThat("Cluster state should be modified", task.execute(state), not(sameInstance(state)));

        task = new ReservedStateUpdateTask(
            "test",
            new ReservedStateChunk(Map.of(), new ReservedStateVersion(123L, Version.CURRENT)),
            ReservedStateVersionCheck.HIGHER_VERSION_ONLY,
            Map.of(),
            List.of(),
            e -> {},
            ActionListener.noop()
        );
        assertThat("Cluster state should not be modified", task.execute(state), sameInstance(state));
        task = new ReservedStateUpdateTask(
            "test",
            new ReservedStateChunk(Map.of(), new ReservedStateVersion(123L, Version.CURRENT)),
            ReservedStateVersionCheck.HIGHER_OR_SAME_VERSION,
            Map.of(),
            List.of(),
            e -> {},
            ActionListener.noop()
        );
        assertThat("Cluster state should be modified", task.execute(state), not(sameInstance(state)));

        task = new ReservedStateUpdateTask(
            "test",
            new ReservedStateChunk(Map.of(), new ReservedStateVersion(122L, Version.CURRENT)),
            ReservedStateVersionCheck.HIGHER_VERSION_ONLY,
            Map.of(),
            List.of(),
            e -> {},
            ActionListener.noop()
        );
        assertThat("Cluster state should not be modified", task.execute(state), sameInstance(state));
        task = new ReservedStateUpdateTask(
            "test",
            new ReservedStateChunk(Map.of(), new ReservedStateVersion(122L, Version.CURRENT)),
            ReservedStateVersionCheck.HIGHER_OR_SAME_VERSION,
            Map.of(),
            List.of(),
            e -> {},
            ActionListener.noop()
        );
        assertThat("Cluster state should not be modified", task.execute(state), sameInstance(state));

        task = new ReservedStateUpdateTask(
            "test",
            new ReservedStateChunk(Map.of(), new ReservedStateVersion(124L, Version.fromId(Version.CURRENT.id + 1))),
            ReservedStateVersionCheck.HIGHER_VERSION_ONLY,
            Map.of(),
            List.of(),
            e -> {},
            ActionListener.noop()
        );
        assertThat("Cluster state should not be modified", task.execute(state), sameInstance(state));
        task = new ReservedStateUpdateTask(
            "test",
            new ReservedStateChunk(Map.of(), new ReservedStateVersion(124L, Version.fromId(Version.CURRENT.id + 1))),
            ReservedStateVersionCheck.HIGHER_OR_SAME_VERSION,
            Map.of(),
            List.of(),
            e -> {},
            ActionListener.noop()
        );
        assertThat("Cluster state should not be modified", task.execute(state), sameInstance(state));
    }

    private ReservedClusterStateHandler<Map<String, Object>> makeHandlerHelper(String name, List<String> deps) {
        return new TestStateHandler(name) {
            @Override
            public TransformState transform(Object source, TransformState prevState) throws Exception {
                return null;
            }

            @Override
            public Collection<String> dependencies() {
                return deps;
            }
        };
    }

    public void testHandlerOrdering() {
        ReservedClusterStateHandler<Map<String, Object>> oh1 = makeHandlerHelper("one", List.of("two", "three"));
        ReservedClusterStateHandler<Map<String, Object>> oh2 = makeHandlerHelper("two", List.of());
        ReservedClusterStateHandler<Map<String, Object>> oh3 = makeHandlerHelper("three", List.of("two"));

        ClusterService clusterService = mock(ClusterService.class);
        final var controller = new ReservedClusterStateService(clusterService, mock(RerouteService.class), List.of(oh1, oh2, oh3));
        Collection<String> ordered = controller.orderedStateHandlers(Set.of("one", "two", "three"));
        assertThat(ordered, contains("two", "three", "one"));

        // assure that we bail on unknown handler
        assertThat(
            expectThrows(IllegalStateException.class, () -> controller.orderedStateHandlers(Set.of("one", "two", "three", "four")))
                .getMessage(),
            is("Unknown handler type: four")
        );

        // assure that we bail on missing dependency link
        assertThat(
            expectThrows(IllegalStateException.class, () -> controller.orderedStateHandlers(Set.of("one", "two"))).getMessage(),
            is("Missing handler dependency definition: one -> three")
        );

        // Change the second handler so that we create cycle
        oh2 = makeHandlerHelper("two", List.of("one"));

        final var controller1 = new ReservedClusterStateService(clusterService, mock(RerouteService.class), List.of(oh1, oh2));

        assertThat(
            expectThrows(IllegalStateException.class, () -> controller1.orderedStateHandlers(Set.of("one", "two"))).getMessage(),
            anyOf(
                is("Cycle found in settings dependencies: one -> two -> one"),
                is("Cycle found in settings dependencies: two -> one -> two")
            )
        );
    }

    public void testDuplicateHandlerNames() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = mock(ClusterService.class);
        final ClusterName clusterName = new ClusterName("elasticsearch");

        ClusterState state = ClusterState.builder(clusterName).build();
        when(clusterService.state()).thenReturn(state);

        assertThat(
            expectThrows(
                IllegalStateException.class,
                () -> new ReservedClusterStateService(
                    clusterService,
                    mock(RerouteService.class),
                    List.of(new ReservedClusterSettingsAction(clusterSettings), new TestStateHandler(ReservedClusterSettingsAction.NAME) {
                        @Override
                        public TransformState transform(Object source, TransformState prevState) throws Exception {
                            return prevState;
                        }
                    })
                )
            ).getMessage(),
            startsWith("Duplicate key cluster_settings")
        );
    }

    public void testCheckAndReportError() {
        ClusterService clusterService = mock(ClusterService.class);
        var state = ClusterState.builder(new ClusterName("elasticsearch")).build();
        when(clusterService.state()).thenReturn(state);
        when(clusterService.createTaskQueue(any(), any(), any())).thenReturn(mockTaskQueue());

        final var controller = spy(new ReservedClusterStateService(clusterService, mock(RerouteService.class), List.of()));

        assertNull(controller.checkAndReportError("test", List.of(), null, ReservedStateVersionCheck.HIGHER_VERSION_ONLY));
        verify(controller, times(0)).updateErrorState(any());

        var version = new ReservedStateVersion(2L, Version.CURRENT);
        var error = controller.checkAndReportError("test", List.of("test error"), version, ReservedStateVersionCheck.HIGHER_VERSION_ONLY);
        assertThat(error, instanceOf(IllegalStateException.class));
        assertThat(error.getMessage(), is("Error processing state change request for test, errors: test error"));
        verify(controller, times(1)).updateErrorState(any());
    }

    public void testTrialRunExtractsNonStateActions() {
        ReservedClusterStateHandler<Map<String, Object>> newStateMaker = new TestStateHandler("maker");
        ReservedClusterStateHandler<Map<String, Object>> exceptionThrower = new TestStateHandler("non-state") {
            @Override
            public TransformState transform(Object source, TransformState prevState) {
                return new TransformState(prevState.state(), prevState.keys());
            }
        };

        ReservedStateHandlerMetadata hmOne = new ReservedStateHandlerMetadata("non-state", Set.of("a", "b"));
        ReservedStateErrorMetadata emOne = new ReservedStateErrorMetadata(
            2L,
            ReservedStateErrorMetadata.ErrorKind.VALIDATION,
            List.of("Test error 1", "Test error 2")
        );

        final ReservedStateMetadata operatorMetadata = ReservedStateMetadata.builder("namespace_one")
            .errorMetadata(emOne)
            .version(1L)
            .putHandler(hmOne)
            .build();

        Metadata metadata = Metadata.builder().put(operatorMetadata).build();
        ClusterState state = ClusterState.builder(new ClusterName("test")).metadata(metadata).build();

        var chunk = new ReservedStateChunk(Map.of("non-state", "two", "maker", "three"), new ReservedStateVersion(2L, Version.CURRENT));
        var orderedHandlers = List.of(exceptionThrower.name(), newStateMaker.name());

        ClusterService clusterService = mock(ClusterService.class);
        final var controller = spy(
            new ReservedClusterStateService(clusterService, mock(RerouteService.class), List.of(newStateMaker, exceptionThrower))
        );

        var trialRunErrors = controller.trialRun("namespace_one", state, chunk, new LinkedHashSet<>(orderedHandlers));
        assertThat(trialRunErrors, empty());
    }
}
