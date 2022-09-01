/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reservedstate.service;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ReservedStateErrorMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateHandlerMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.service.ClusterService;
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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.reservedstate.service.ReservedStateUpdateTask.checkMetadataVersion;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReservedClusterStateServiceTests extends ESTestCase {

    public void testOperatorController() throws IOException {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = mock(ClusterService.class);
        final ClusterName clusterName = new ClusterName("elasticsearch");

        ClusterState state = ClusterState.builder(clusterName).build();
        when(clusterService.state()).thenReturn(state);

        ReservedClusterStateService controller = new ReservedClusterStateService(
            clusterService,
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
            controller.process("operator", parser, (e) -> x.set(e));

            assertTrue(x.get() instanceof IllegalStateException);
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
            controller.process("operator", parser, (e) -> {
                if (e != null) {
                    fail("Should not fail");
                }
            });
        }
    }

    public void testUpdateStateTasks() throws Exception {
        ClusterService clusterService = mock(ClusterService.class);
        RerouteService rerouteService = mock(RerouteService.class);

        when(clusterService.getRerouteService()).thenReturn(rerouteService);
        ClusterState state = ClusterState.builder(new ClusterName("test")).build();

        ReservedStateUpdateTaskExecutor taskExecutor = new ReservedStateUpdateTaskExecutor(clusterService.getRerouteService());

        AtomicBoolean successCalled = new AtomicBoolean(false);

        ReservedStateUpdateTask task = spy(
            new ReservedStateUpdateTask(
                "test",
                null,
                Collections.emptyMap(),
                Collections.emptySet(),
                (clusterState, errorState) -> {},
                new ActionListener<>() {
                    @Override
                    public void onResponse(ActionResponse.Empty empty) {}

                    @Override
                    public void onFailure(Exception e) {}
                }
            )
        );

        doReturn(state).when(task).execute(any());

        ClusterStateTaskExecutor.TaskContext<ReservedStateUpdateTask> taskContext = new ClusterStateTaskExecutor.TaskContext<>() {
            @Override
            public ReservedStateUpdateTask getTask() {
                return task;
            }

            @Override
            public void success(Runnable onPublicationSuccess) {
                onPublicationSuccess.run();
                successCalled.set(true);
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

    public void testErrorStateTask() throws Exception {
        ClusterState state = ClusterState.builder(new ClusterName("test")).build();

        final var listenerCompleted = new AtomicBoolean(false);

        ReservedStateErrorTask task = spy(
            new ReservedStateErrorTask(
                new ErrorState("test", 1L, List.of("some parse error", "some io error"), ReservedStateErrorMetadata.ErrorKind.PARSING),
                new ActionListener<>() {
                    @Override
                    public void onResponse(ActionResponse.Empty empty) {
                        listenerCompleted.set(true);
                    }

                    @Override
                    public void onFailure(Exception e) {}
                }
            )
        );

        ReservedStateErrorTaskExecutor.TaskContext<ReservedStateErrorTask> taskContext =
            new ReservedStateErrorTaskExecutor.TaskContext<>() {
                @Override
                public ReservedStateErrorTask getTask() {
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
            };

        ReservedStateErrorTaskExecutor executor = new ReservedStateErrorTaskExecutor();

        ClusterState newState = executor.execute(
            new ClusterStateTaskExecutor.BatchExecutionContext<>(state, List.of(taskContext), () -> null)
        );

        verify(task, times(1)).execute(any());

        ReservedStateMetadata operatorMetadata = newState.metadata().reservedStateMetadata().get("test");
        assertNotNull(operatorMetadata);
        assertNotNull(operatorMetadata.errorMetadata());
        assertEquals(1L, (long) operatorMetadata.errorMetadata().version());
        assertEquals(ReservedStateErrorMetadata.ErrorKind.PARSING, operatorMetadata.errorMetadata().errorKind());
        assertThat(operatorMetadata.errorMetadata().errors(), contains("some parse error", "some io error"));
        assertTrue(listenerCompleted.get());
    }

    public void testUpdateTaskDuplicateError() {
        ReservedClusterStateHandler<Map<String, Object>> newStateMaker = new ReservedClusterStateHandler<>() {
            @Override
            public String name() {
                return "maker";
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
        };

        ReservedClusterStateHandler<Map<String, Object>> exceptionThrower = new ReservedClusterStateHandler<>() {
            @Override
            public String name() {
                return "one";
            }

            @Override
            public TransformState transform(Object source, TransformState prevState) throws Exception {
                throw new Exception("anything");
            }

            @Override
            public Map<String, Object> fromXContent(XContentParser parser) throws IOException {
                return parser.map();
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

        assertFalse(ReservedClusterStateService.isNewError(operatorMetadata, 2L));
        assertFalse(ReservedClusterStateService.isNewError(operatorMetadata, 1L));
        assertTrue(ReservedClusterStateService.isNewError(operatorMetadata, 3L));
        assertTrue(ReservedClusterStateService.isNewError(null, 1L));

        // We submit a task with two handler, one will cause an exception, the other will create a new state.
        // When we fail to update the metadata because of version, we ensure that the returned state is equal to the
        // original state by pointer reference to avoid cluster state update task to run.
        ReservedStateUpdateTask task = new ReservedStateUpdateTask(
            "namespace_one",
            new ReservedStateChunk(Map.of("one", "two", "maker", "three"), new ReservedStateVersion(2L, Version.CURRENT)),
            Map.of(exceptionThrower.name(), exceptionThrower, newStateMaker.name(), newStateMaker),
            List.of(exceptionThrower.name(), newStateMaker.name()),
            (clusterState, errorState) -> { assertFalse(ReservedClusterStateService.isNewError(operatorMetadata, errorState.version())); },
            new ActionListener<>() {
                @Override
                public void onResponse(ActionResponse.Empty empty) {}

                @Override
                public void onFailure(Exception e) {}
            }
        );

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

        assertTrue(checkMetadataVersion("operator", operatorMetadata, new ReservedStateVersion(124L, Version.CURRENT)));

        assertFalse(checkMetadataVersion("operator", operatorMetadata, new ReservedStateVersion(123L, Version.CURRENT)));

        assertFalse(
            checkMetadataVersion("operator", operatorMetadata, new ReservedStateVersion(124L, Version.fromId(Version.CURRENT.id + 1)))
        );
    }

    private ReservedClusterStateHandler<Map<String, Object>> makeHandlerHelper(final String name, final List<String> deps) {
        return new ReservedClusterStateHandler<>() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public TransformState transform(Object source, TransformState prevState) throws Exception {
                return null;
            }

            @Override
            public Collection<String> dependencies() {
                return deps;
            }

            @Override
            public Map<String, Object> fromXContent(XContentParser parser) throws IOException {
                return parser.map();
            }
        };
    }

    public void testHandlerOrdering() {
        ReservedClusterStateHandler<Map<String, Object>> oh1 = makeHandlerHelper("one", List.of("two", "three"));
        ReservedClusterStateHandler<Map<String, Object>> oh2 = makeHandlerHelper("two", Collections.emptyList());
        ReservedClusterStateHandler<Map<String, Object>> oh3 = makeHandlerHelper("three", List.of("two"));

        ClusterService clusterService = mock(ClusterService.class);
        final var controller = new ReservedClusterStateService(clusterService, List.of(oh1, oh2, oh3));
        Collection<String> ordered = controller.orderedStateHandlers(Set.of("one", "two", "three"));
        assertThat(ordered, contains("two", "three", "one"));

        // assure that we bail on unknown handler
        assertEquals(
            "Unknown handler type: four",
            expectThrows(IllegalStateException.class, () -> controller.orderedStateHandlers(Set.of("one", "two", "three", "four")))
                .getMessage()
        );

        // assure that we bail on missing dependency link
        assertEquals(
            "Missing handler dependency definition: one -> three",
            expectThrows(IllegalStateException.class, () -> controller.orderedStateHandlers(Set.of("one", "two"))).getMessage()
        );

        // Change the second handler so that we create cycle
        oh2 = makeHandlerHelper("two", List.of("one"));

        final var controller1 = new ReservedClusterStateService(clusterService, List.of(oh1, oh2));

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

        assertTrue(
            expectThrows(
                IllegalStateException.class,
                () -> new ReservedClusterStateService(
                    clusterService,
                    List.of(new ReservedClusterSettingsAction(clusterSettings), new TestHandler())
                )
            ).getMessage().startsWith("Duplicate key cluster_settings")
        );
    }

    class TestHandler implements ReservedClusterStateHandler<Map<String, Object>> {

        @Override
        public String name() {
            return ReservedClusterSettingsAction.NAME;
        }

        @Override
        public TransformState transform(Object source, TransformState prevState) {
            return prevState;
        }

        @Override
        public Map<String, Object> fromXContent(XContentParser parser) throws IOException {
            return parser.map();
        }
    }
}
