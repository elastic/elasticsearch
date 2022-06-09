/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.operator.service;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.metadata.OperatorErrorMetadata;
import org.elasticsearch.cluster.metadata.OperatorMetadata;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.operator.OperatorHandler;
import org.elasticsearch.operator.TransformState;
import org.elasticsearch.operator.action.OperatorClusterUpdateSettingsAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OperatorClusterStateControllerTests extends ESTestCase {

    public void testOperatorController() throws IOException {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = mock(ClusterService.class);
        final ClusterName clusterName = new ClusterName("elasticsearch");

        ClusterState state = ClusterState.builder(clusterName).build();
        when(clusterService.state()).thenReturn(state);

        OperatorClusterStateController controller = new OperatorClusterStateController(clusterService);
        controller.initHandlers(List.of(new OperatorClusterUpdateSettingsAction(clusterSettings)));

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
            assertEquals("Error processing state change request for operator", x.get().getMessage());
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

        OperatorUpdateStateTask.OperatorUpdateStateTaskExecutor taskExecutor = new OperatorUpdateStateTask.OperatorUpdateStateTaskExecutor(
            "test",
            clusterService.getRerouteService()
        );

        AtomicBoolean successCalled = new AtomicBoolean(false);

        OperatorUpdateStateTask task = spy(
            new OperatorUpdateStateTask(
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

        ClusterStateTaskExecutor.TaskContext<OperatorUpdateStateTask> taskContext = new ClusterStateTaskExecutor.TaskContext<>() {
            @Override
            public OperatorUpdateStateTask getTask() {
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

        OperatorUpdateErrorTask task = spy(
            new OperatorUpdateErrorTask(
                new OperatorClusterStateController.OperatorErrorState(
                    "test",
                    1L,
                    List.of("some parse error", "some io error"),
                    OperatorErrorMetadata.ErrorKind.PARSING
                ),
                new ActionListener<>() {
                    @Override
                    public void onResponse(ActionResponse.Empty empty) {}

                    @Override
                    public void onFailure(Exception e) {}
                }
            )
        );

        OperatorUpdateErrorTask.OperatorUpdateErrorTaskExecutor.TaskContext<OperatorUpdateErrorTask> taskContext =
            new OperatorUpdateErrorTask.OperatorUpdateErrorTaskExecutor.TaskContext<>() {
                @Override
                public OperatorUpdateErrorTask getTask() {
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

        OperatorUpdateErrorTask.OperatorUpdateErrorTaskExecutor executor = new OperatorUpdateErrorTask.OperatorUpdateErrorTaskExecutor();

        ClusterState newState = executor.execute(state, List.of(taskContext));

        verify(task, times(1)).execute(any());

        OperatorMetadata operatorMetadata = newState.metadata().operatorState("test");
        assertNotNull(operatorMetadata);
        assertNotNull(operatorMetadata.errorMetadata());
        assertEquals(1L, (long) operatorMetadata.errorMetadata().version());
        assertEquals(OperatorErrorMetadata.ErrorKind.PARSING, operatorMetadata.errorMetadata().errorKind());
        assertThat(operatorMetadata.errorMetadata().errors(), contains("some parse error", "some io error"));
    }

    public void testcheckMetadataVersion() {
        OperatorMetadata operatorMetadata = OperatorMetadata.builder("test").version(123L).build();

        assertTrue(
            OperatorClusterStateController.checkMetadataVersion(
                operatorMetadata,
                new OperatorStateVersionMetadata(124L, Version.CURRENT),
                (e) -> {}
            )
        );

        AtomicReference<Exception> x = new AtomicReference<>();

        assertFalse(
            OperatorClusterStateController.checkMetadataVersion(
                operatorMetadata,
                new OperatorStateVersionMetadata(123L, Version.CURRENT),
                (e) -> x.set(e)
            )
        );

        assertTrue(x.get() instanceof OperatorClusterStateController.IncompatibleVersionException);
        assertTrue(x.get().getMessage().contains("is less or equal to the current metadata version"));

        assertFalse(
            OperatorClusterStateController.checkMetadataVersion(
                operatorMetadata,
                new OperatorStateVersionMetadata(124L, Version.fromId(Version.CURRENT.id + 1)),
                (e) -> x.set(e)
            )
        );

        assertEquals(OperatorClusterStateController.IncompatibleVersionException.class, x.get().getClass());
        assertTrue(x.get().getMessage().contains("is not compatible with this Elasticsearch node"));
    }

    public void testHandlerOrdering() {
        OperatorHandler<?> oh1 = new OperatorHandler<>() {
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
        };

        OperatorHandler<?> oh2 = new OperatorHandler<>() {
            @Override
            public String name() {
                return "two";
            }

            @Override
            public TransformState transform(Object source, TransformState prevState) throws Exception {
                return null;
            }
        };

        OperatorHandler<?> oh3 = new OperatorHandler<>() {
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
        };

        ClusterService clusterService = mock(ClusterService.class);
        OperatorClusterStateController controller = new OperatorClusterStateController(clusterService);

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
        oh2 = new OperatorHandler<>() {
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

        OperatorClusterStateController controller = new OperatorClusterStateController(clusterService);

        assertTrue(
            expectThrows(
                IllegalStateException.class,
                () -> controller.initHandlers(List.of(new OperatorClusterUpdateSettingsAction(clusterSettings), new TestHandler()))
            ).getMessage().startsWith("Duplicate key cluster_settings")
        );
    }

    class TestHandler implements OperatorHandler<ClusterUpdateSettingsRequest> {

        @Override
        public String name() {
            return OperatorClusterUpdateSettingsAction.NAME;
        }

        @Override
        public TransformState transform(Object source, TransformState prevState) throws Exception {
            return prevState;
        }
    }
}
