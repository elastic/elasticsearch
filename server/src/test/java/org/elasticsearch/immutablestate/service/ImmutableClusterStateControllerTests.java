/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.immutablestate.service;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.metadata.ImmutableStateErrorMetadata;
import org.elasticsearch.cluster.metadata.ImmutableStateHandlerMetadata;
import org.elasticsearch.cluster.metadata.ImmutableStateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.immutablestate.ImmutableClusterStateHandler;
import org.elasticsearch.immutablestate.TransformState;
import org.elasticsearch.immutablestate.action.ImmutableClusterSettingsAction;
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

public class ImmutableClusterStateControllerTests extends ESTestCase {

    public void testOperatorController() throws IOException {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = mock(ClusterService.class);
        final ClusterName clusterName = new ClusterName("elasticsearch");

        ClusterState state = ClusterState.builder(clusterName).build();
        when(clusterService.state()).thenReturn(state);

        ImmutableClusterStateController controller = new ImmutableClusterStateController(clusterService);
        controller.initHandlers(List.of(new ImmutableClusterSettingsAction(clusterSettings)));

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
            assertThat(
                x.get().getMessage(),
                containsString(
                    "Error processing state change request for operator, with errors: [12:1] "
                        + "Unexpected end-of-input: expected close marker for Object (start marker at [Source: (String)"
                )
            );
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

        ImmutableStateUpdateStateTask.ImmutableUpdateStateTaskExecutor taskExecutor =
            new ImmutableStateUpdateStateTask.ImmutableUpdateStateTaskExecutor("test", clusterService.getRerouteService());

        AtomicBoolean successCalled = new AtomicBoolean(false);

        ImmutableStateUpdateStateTask task = spy(
            new ImmutableStateUpdateStateTask(
                "test",
                null,
                Collections.emptyMap(),
                Collections.emptySet(),
                (errorState) -> {},
                new ActionListener<>() {
                    @Override
                    public void onResponse(ActionResponse.Empty empty) {}

                    @Override
                    public void onFailure(Exception e) {}
                }
            )
        );

        doReturn(state).when(task).execute(any());

        ClusterStateTaskExecutor.TaskContext<ImmutableStateUpdateStateTask> taskContext = new ClusterStateTaskExecutor.TaskContext<>() {
            @Override
            public ImmutableStateUpdateStateTask getTask() {
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
        };

        ClusterState newState = taskExecutor.execute(state, List.of(taskContext));
        assertEquals(state, newState);
        assertTrue(successCalled.get());
        verify(task, times(1)).execute(any());

        taskExecutor.clusterStatePublished(state);
        verify(rerouteService, times(1)).reroute(anyString(), any(), any());
    }

    public void testErrorStateTask() throws Exception {
        ClusterState state = ClusterState.builder(new ClusterName("test")).build();

        ImmutableStateUpdateErrorTask task = spy(
            new ImmutableStateUpdateErrorTask(
                new ImmutableClusterStateController.ImmutableUpdateErrorState(
                    "test",
                    1L,
                    List.of("some parse error", "some io error"),
                    ImmutableStateErrorMetadata.ErrorKind.PARSING
                ),
                new ActionListener<>() {
                    @Override
                    public void onResponse(ActionResponse.Empty empty) {}

                    @Override
                    public void onFailure(Exception e) {}
                }
            )
        );

        ImmutableStateUpdateErrorTask.ImmutableUpdateErrorTaskExecutor.TaskContext<ImmutableStateUpdateErrorTask> taskContext =
            new ImmutableStateUpdateErrorTask.ImmutableUpdateErrorTaskExecutor.TaskContext<>() {
                @Override
                public ImmutableStateUpdateErrorTask getTask() {
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
            };

        ImmutableStateUpdateErrorTask.ImmutableUpdateErrorTaskExecutor executor =
            new ImmutableStateUpdateErrorTask.ImmutableUpdateErrorTaskExecutor();

        ClusterState newState = executor.execute(state, List.of(taskContext));

        verify(task, times(1)).execute(any());

        ImmutableStateMetadata operatorMetadata = newState.metadata().immutableStateMetadata().get("test");
        assertNotNull(operatorMetadata);
        assertNotNull(operatorMetadata.errorMetadata());
        assertEquals(1L, (long) operatorMetadata.errorMetadata().version());
        assertEquals(ImmutableStateErrorMetadata.ErrorKind.PARSING, operatorMetadata.errorMetadata().errorKind());
        assertThat(operatorMetadata.errorMetadata().errors(), contains("some parse error", "some io error"));
    }

    public void testUpdateTaskDuplicateError() {
        ImmutableClusterStateHandler<Map<String, Object>> dummy = new ImmutableClusterStateHandler<>() {
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

        ImmutableStateUpdateStateTask task = spy(
            new ImmutableStateUpdateStateTask(
                "namespace_one",
                new ImmutableClusterStateController.Package(Map.of("one", "two"), new PackageVersion(1L, Version.CURRENT)),
                Map.of("one", dummy),
                List.of(dummy.name()),
                (errorState) -> {},
                new ActionListener<>() {
                    @Override
                    public void onResponse(ActionResponse.Empty empty) {}

                    @Override
                    public void onFailure(Exception e) {}
                }
            )
        );

        ImmutableStateHandlerMetadata hmOne = new ImmutableStateHandlerMetadata("one", Set.of("a", "b"));
        ImmutableStateErrorMetadata emOne = new ImmutableStateErrorMetadata(
            1L,
            ImmutableStateErrorMetadata.ErrorKind.VALIDATION,
            List.of("Test error 1", "Test error 2")
        );

        ImmutableStateMetadata operatorMetadata = ImmutableStateMetadata.builder("namespace_one")
            .errorMetadata(emOne)
            .version(1L)
            .putHandler(hmOne)
            .build();

        Metadata metadata = Metadata.builder().put(operatorMetadata).build();
        ClusterState state = ClusterState.builder(new ClusterName("test")).metadata(metadata).build();

        // We exit on duplicate errors before we update the cluster state error metadata
        assertEquals(
            "Not updating error state because version [1] is less or equal to the last state error version [1]",
            expectThrows(ImmutableClusterStateController.IncompatibleVersionException.class, () -> task.execute(state)).getMessage()
        );

        emOne = new ImmutableStateErrorMetadata(
            0L,
            ImmutableStateErrorMetadata.ErrorKind.VALIDATION,
            List.of("Test error 1", "Test error 2")
        );

        // If we are writing with older error metadata, we should get proper IllegalStateException
        operatorMetadata = ImmutableStateMetadata.builder("namespace_one").errorMetadata(emOne).version(0L).putHandler(hmOne).build();

        metadata = Metadata.builder().put(operatorMetadata).build();
        ClusterState newState = ClusterState.builder(new ClusterName("test")).metadata(metadata).build();

        // We exit on duplicate errors before we update the cluster state error metadata
        assertEquals(
            "Error processing state change request for namespace_one",
            expectThrows(IllegalStateException.class, () -> task.execute(newState)).getMessage()
        );
    }

    public void testCheckMetadataVersion() {
        ImmutableStateMetadata operatorMetadata = ImmutableStateMetadata.builder("test").version(123L).build();

        assertTrue(
            ImmutableClusterStateController.checkMetadataVersion(operatorMetadata, new PackageVersion(124L, Version.CURRENT), (e) -> {})
        );

        AtomicReference<Exception> x = new AtomicReference<>();

        assertFalse(
            ImmutableClusterStateController.checkMetadataVersion(
                operatorMetadata,
                new PackageVersion(123L, Version.CURRENT),
                (e) -> x.set(e)
            )
        );

        assertTrue(x.get() instanceof ImmutableClusterStateController.IncompatibleVersionException);
        assertTrue(x.get().getMessage().contains("is less or equal to the current metadata version"));

        assertFalse(
            ImmutableClusterStateController.checkMetadataVersion(
                operatorMetadata,
                new PackageVersion(124L, Version.fromId(Version.CURRENT.id + 1)),
                (e) -> x.set(e)
            )
        );

        assertEquals(ImmutableClusterStateController.IncompatibleVersionException.class, x.get().getClass());
        assertTrue(x.get().getMessage().contains("is not compatible with this Elasticsearch node"));
    }

    public void testHandlerOrdering() {
        ImmutableClusterStateHandler<Map<String, Object>> oh1 = new ImmutableClusterStateHandler<>() {
            @Override
            public String name() {
                return "one";
            }

            @Override
            public TransformState transform(Object source, TransformState prevState) throws Exception {
                return null;
            }

            @Override
            public Collection<String> dependencies() {
                return List.of("two", "three");
            }

            @Override
            public Map<String, Object> fromXContent(XContentParser parser) throws IOException {
                return parser.map();
            }
        };

        ImmutableClusterStateHandler<Map<String, Object>> oh2 = new ImmutableClusterStateHandler<>() {
            @Override
            public String name() {
                return "two";
            }

            @Override
            public TransformState transform(Object source, TransformState prevState) throws Exception {
                return null;
            }

            @Override
            public Map<String, Object> fromXContent(XContentParser parser) throws IOException {
                return parser.map();
            }
        };

        ImmutableClusterStateHandler<Map<String, Object>> oh3 = new ImmutableClusterStateHandler<>() {
            @Override
            public String name() {
                return "three";
            }

            @Override
            public TransformState transform(Object source, TransformState prevState) throws Exception {
                return null;
            }

            @Override
            public Collection<String> dependencies() {
                return List.of("two");
            }

            @Override
            public Map<String, Object> fromXContent(XContentParser parser) throws IOException {
                return parser.map();
            }
        };

        ClusterService clusterService = mock(ClusterService.class);
        ImmutableClusterStateController controller = new ImmutableClusterStateController(clusterService);

        controller.initHandlers(List.of(oh1, oh2, oh3));
        Collection<String> ordered = controller.orderedStateHandlers(Set.of("one", "two", "three"));
        assertThat(ordered, contains("two", "three", "one"));

        // assure that we bail on unknown handler
        assertEquals(
            "Unknown settings definition type: four",
            expectThrows(IllegalStateException.class, () -> controller.orderedStateHandlers(Set.of("one", "two", "three", "four")))
                .getMessage()
        );

        // assure that we bail on missing dependency link
        assertEquals(
            "Missing settings dependency definition: one -> three",
            expectThrows(IllegalStateException.class, () -> controller.orderedStateHandlers(Set.of("one", "two"))).getMessage()
        );

        // Change the second handler so that we create cycle
        oh2 = new ImmutableClusterStateHandler<>() {
            @Override
            public String name() {
                return "two";
            }

            @Override
            public TransformState transform(Object source, TransformState prevState) throws Exception {
                return null;
            }

            @Override
            public Collection<String> dependencies() {
                return List.of("one");
            }

            @Override
            public Map<String, Object> fromXContent(XContentParser parser) throws IOException {
                return parser.map();
            }
        };

        controller.initHandlers(List.of(oh1, oh2));
        assertThat(
            expectThrows(IllegalStateException.class, () -> controller.orderedStateHandlers(Set.of("one", "two"))).getMessage(),
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

        ImmutableClusterStateController controller = new ImmutableClusterStateController(clusterService);

        assertTrue(
            expectThrows(
                IllegalStateException.class,
                () -> controller.initHandlers(List.of(new ImmutableClusterSettingsAction(clusterSettings), new TestHandler()))
            ).getMessage().startsWith("Duplicate key cluster_settings")
        );
    }

    class TestHandler implements ImmutableClusterStateHandler<Map<String, Object>> {

        @Override
        public String name() {
            return ImmutableClusterSettingsAction.NAME;
        }

        @Override
        public TransformState transform(Object source, TransformState prevState) throws Exception {
            return prevState;
        }

        @Override
        public Map<String, Object> fromXContent(XContentParser parser) throws IOException {
            return parser.map();
        }
    }
}
