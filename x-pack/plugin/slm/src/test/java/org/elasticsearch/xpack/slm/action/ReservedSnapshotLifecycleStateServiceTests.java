/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm.action;

import org.elasticsearch.action.admin.cluster.repositories.reservedstate.ReservedRepositoryAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.reservedstate.action.ReservedClusterSettingsAction;
import org.elasticsearch.reservedstate.service.ReservedClusterStateService;
import org.elasticsearch.reservedstate.service.ReservedStateUpdateTask;
import org.elasticsearch.reservedstate.service.ReservedStateUpdateTaskExecutor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadataTests;
import org.elasticsearch.xpack.core.slm.action.DeleteSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.PutSnapshotLifecycleAction;
import org.junit.Assert;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.slm.SnapshotInvocationRecordTests.randomSnapshotInvocationRecord;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the basic functionality of the reserved handler operations for {@link ReservedSnapshotAction},
 * {@link TransportPutSnapshotLifecycleAction} and {@link TransportDeleteSnapshotLifecycleAction}
 */
public class ReservedSnapshotLifecycleStateServiceTests extends ESTestCase {

    private TransformState processJSON(ReservedSnapshotAction action, TransformState prevState, String json) throws Exception {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            return action.transform(action.fromXContent(parser), prevState);
        }
    }

    public void testDependencies() {
        var action = new ReservedSnapshotAction();
        assertThat(action.optionalDependencies(), contains(ReservedRepositoryAction.NAME));
    }

    public void testValidationFails() {
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        final ClusterName clusterName = new ClusterName("elasticsearch");

        ClusterState state = ClusterState.builder(clusterName).build();
        ReservedSnapshotAction action = new ReservedSnapshotAction();
        TransformState prevState = new TransformState(state, Set.of());

        String badPolicyJSON = """
            {
              "daily-snapshots": {
                "no_schedule": "0 1 2 3 4 ?",
                "name": "<production-snap-{now/d}>",
                "repository": "repo",
                "config": {
                  "indices": ["foo-*", "important"],
                  "ignore_unavailable": true,
                  "include_global_state": false
                },
                "retention": {
                  "expire_after": "30d",
                  "min_count": 1,
                  "max_count": 50
                }
              }
            }""";

        assertThat(
            expectThrows(IllegalArgumentException.class, () -> processJSON(action, prevState, badPolicyJSON)).getMessage(),
            is("Required [schedule]")
        );
    }

    public void testActionAddRemove() throws Exception {
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        final ClusterName clusterName = new ClusterName("elasticsearch");

        List<RepositoryMetadata> repositoriesMetadata = List.of(new RepositoryMetadata("repo", "fs", Settings.EMPTY));

        Metadata.Builder mdBuilder = Metadata.builder();
        mdBuilder.putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(repositoriesMetadata));
        ClusterState state = ClusterState.builder(clusterName).metadata(mdBuilder).build();

        ReservedSnapshotAction action = new ReservedSnapshotAction();

        String emptyJSON = "";

        TransformState prevState = new TransformState(state, Set.of());

        TransformState updatedState = processJSON(action, prevState, emptyJSON);
        assertThat(updatedState.keys(), empty());
        assertEquals(prevState.state(), updatedState.state());

        String twoPoliciesJSON = """
            {
              "daily-snapshots": {
                "schedule": "0 1 2 3 4 ?",
                "name": "<production-snap-{now/d}>",
                "repository": "repo",
                "config": {
                  "indices": ["foo-*", "important"],
                  "ignore_unavailable": true,
                  "include_global_state": false
                },
                "retention": {
                  "expire_after": "30d",
                  "min_count": 1,
                  "max_count": 50
                }
              },
              "daily-snapshots1": {
                "schedule": "0 1 2 3 4 ?",
                "name": "<production-snap-{now/d}>",
                "repository": "repo",
                "config": {
                  "indices": ["bar-*", "not-important"],
                  "ignore_unavailable": true,
                  "include_global_state": false
                },
                "retention": {
                  "expire_after": "30d",
                  "min_count": 1,
                  "max_count": 50
                }
              }
            }""";

        prevState = updatedState;
        updatedState = processJSON(action, prevState, twoPoliciesJSON);
        assertThat(updatedState.keys(), containsInAnyOrder("daily-snapshots", "daily-snapshots1"));
        SnapshotLifecycleMetadata slmMetadata = updatedState.state().metadata().custom(SnapshotLifecycleMetadata.TYPE);
        assertThat(slmMetadata.getSnapshotConfigurations().keySet(), containsInAnyOrder("daily-snapshots", "daily-snapshots1"));

        String onePolicyRemovedJSON = """
            {
              "daily-snapshots": {
                "schedule": "0 1 2 3 4 ?",
                "name": "<production-snap-{now/d}>",
                "repository": "repo",
                "config": {
                  "indices": ["foo-*", "important"],
                  "ignore_unavailable": true,
                  "include_global_state": false
                },
                "retention": {
                  "expire_after": "30d",
                  "min_count": 1,
                  "max_count": 50
                }
              }
            }""";

        prevState = updatedState;
        updatedState = processJSON(action, prevState, onePolicyRemovedJSON);
        assertThat(updatedState.keys(), containsInAnyOrder("daily-snapshots"));
        slmMetadata = updatedState.state().metadata().custom(SnapshotLifecycleMetadata.TYPE);
        assertThat(slmMetadata.getSnapshotConfigurations().keySet(), containsInAnyOrder("daily-snapshots"));

        String onePolicyRenamedJSON = """
            {
              "daily-snapshots-2": {
                "schedule": "0 1 2 3 4 ?",
                "name": "<production-snap-{now/d}>",
                "repository": "repo",
                "config": {
                  "indices": ["foo-*", "important"],
                  "ignore_unavailable": true,
                  "include_global_state": false
                },
                "retention": {
                  "expire_after": "30d",
                  "min_count": 1,
                  "max_count": 50
                }
              }
            }""";

        prevState = updatedState;
        updatedState = processJSON(action, prevState, onePolicyRenamedJSON);
        assertThat(updatedState.keys(), containsInAnyOrder("daily-snapshots-2"));
        slmMetadata = updatedState.state().metadata().custom(SnapshotLifecycleMetadata.TYPE);
        assertThat(slmMetadata.getSnapshotConfigurations().keySet(), containsInAnyOrder("daily-snapshots-2"));
    }

    private void setupTaskMock(ClusterService clusterService) {
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        when(clusterService.state()).thenReturn(state);
        when(clusterService.createTaskQueue(anyString(), any(), any())).thenAnswer(getQueueInvocation -> {
            Object[] getQueueArgs = getQueueInvocation.getArguments();
            @SuppressWarnings("unchecked")
            final MasterServiceTaskQueue<ClusterStateTaskListener> taskQueue = mock(MasterServiceTaskQueue.class);

            if ((getQueueArgs[2] instanceof ReservedStateUpdateTaskExecutor executor)) {
                doAnswer(submitTaskInvocation -> {
                    Object[] submitTaskArgs = submitTaskInvocation.getArguments();
                    ClusterStateTaskExecutor.TaskContext<ReservedStateUpdateTask> context = new ClusterStateTaskExecutor.TaskContext<>() {
                        @Override
                        public ReservedStateUpdateTask getTask() {
                            return (ReservedStateUpdateTask) submitTaskArgs[1];
                        }

                        @Override
                        public void success(Runnable onPublicationSuccess) {}

                        @Override
                        public void success(Consumer<ClusterState> publishedStateConsumer) {}

                        @Override
                        public void success(Runnable onPublicationSuccess, ClusterStateAckListener clusterStateAckListener) {}

                        @Override
                        public void success(
                            Consumer<ClusterState> publishedStateConsumer,
                            ClusterStateAckListener clusterStateAckListener
                        ) {}

                        @Override
                        public void onFailure(Exception failure) {
                            fail("Shouldn't fail here");
                        }

                        @Override
                        public Releasable captureResponseHeaders() {
                            return null;
                        }
                    };
                    executor.execute(new ClusterStateTaskExecutor.BatchExecutionContext<>(state, List.of(context), () -> null));
                    return null;
                }).when(taskQueue).submitTask(anyString(), any(), any());
            }
            return taskQueue;
        });
    }

    public void testOperatorControllerFromJSONContent() throws IOException {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = mock(ClusterService.class);
        setupTaskMock(clusterService);

        var repositoriesService = mock(RepositoriesService.class);

        ReservedClusterStateService controller = new ReservedClusterStateService(
            clusterService,
            null,
            List.of(new ReservedClusterSettingsAction(clusterSettings), new ReservedRepositoryAction(repositoriesService))
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
                },
                "snapshot_repositories": {
                  "repo": {
                    "type": "fs",
                    "settings": {
                      "location": "my_backup_location"
                    }
                  }
                },
                "slm": {
                  "daily-snapshots": {
                    "schedule": "0 1 2 3 4 ?",
                    "name": "<production-snap-{now/d}>",
                    "repository": "repo",
                    "config": {
                      "indices": ["foo-*", "important"],
                      "ignore_unavailable": true,
                      "include_global_state": false
                    },
                    "retention": {
                      "expire_after": "30d",
                      "min_count": 1,
                      "max_count": 50
                    }
                  },
                  "daily-snapshots1": {
                    "schedule": "0 1 2 3 4 ?",
                    "name": "<production-snap-{now/d}>",
                    "repository": "repo",
                    "config": {
                      "indices": ["bar-*", "not-important"],
                      "ignore_unavailable": true,
                      "include_global_state": false
                    },
                    "retention": {
                      "expire_after": "30d",
                      "min_count": 1,
                      "max_count": 50
                    }
                  }
                }
              }
            }""";

        AtomicReference<Exception> x = new AtomicReference<>();

        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, testJSON)) {
            controller.process("operator", parser, x::set);

            assertThat(x.get(), instanceOf(IllegalStateException.class));
            assertThat(x.get().getMessage(), containsString("Error processing state change request for operator"));
        }

        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);

        controller = new ReservedClusterStateService(
            clusterService,
            null,
            List.of(
                new ReservedClusterSettingsAction(clusterSettings),
                new ReservedSnapshotAction(),
                new ReservedRepositoryAction(repositoriesService)
            )
        );

        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, testJSON)) {
            controller.process("operator", parser, Assert::assertNull);
        }
    }

    public void testDeleteSLMReservedStateHandler() {
        ThreadPool threadPool = mock(ThreadPool.class);
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(threadPool);
        var deleteAction = new TransportDeleteSnapshotLifecycleAction(
            transportService,
            mock(ClusterService.class),
            threadPool,
            mock(ActionFilters.class),
            mock(IndexNameExpressionResolver.class)
        );
        assertThat(deleteAction.reservedStateHandlerName().get(), equalTo(ReservedSnapshotAction.NAME));

        var request = new DeleteSnapshotLifecycleAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "daily-snapshots1");
        assertThat(deleteAction.modifiedKeys(request), containsInAnyOrder("daily-snapshots1"));
    }

    public void testPutSLMReservedStateHandler() throws Exception {
        ThreadPool threadPool = mock(ThreadPool.class);
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(threadPool);
        var putAction = new TransportPutSnapshotLifecycleAction(
            transportService,
            mock(ClusterService.class),
            threadPool,
            mock(ActionFilters.class),
            mock(IndexNameExpressionResolver.class)
        );
        assertThat(putAction.reservedStateHandlerName().get(), equalTo(ReservedSnapshotAction.NAME));

        String json = """
            {
              "schedule": "0 1 2 3 4 ?",
              "name": "<production-snap-{now/d}>",
              "repository": "repo",
              "config": {
                "indices": ["foo-*", "important"],
                "ignore_unavailable": true,
                "include_global_state": false
              },
              "retention": {
                "expire_after": "30d",
                "min_count": 1,
                "max_count": 50
              }
            }""";

        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            var request = PutSnapshotLifecycleAction.Request.parseRequest(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                "daily-snapshots",
                parser
            );

            assertThat(putAction.modifiedKeys(request), containsInAnyOrder("daily-snapshots"));
        }
    }

    public void testIsNoop() {
        SnapshotLifecyclePolicy existingPolicy = SnapshotLifecyclePolicyMetadataTests.randomSnapshotLifecyclePolicy("id1");
        SnapshotLifecyclePolicy newPolicy = randomValueOtherThan(
            existingPolicy,
            () -> SnapshotLifecyclePolicyMetadataTests.randomSnapshotLifecyclePolicy("id2")
        );

        Map<String, String> existingHeaders = Map.of("foo", "bar");
        Map<String, String> newHeaders = Map.of("foo", "eggplant");

        SnapshotLifecyclePolicyMetadata existingPolicyMeta = new SnapshotLifecyclePolicyMetadata(
            existingPolicy,
            existingHeaders,
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomSnapshotInvocationRecord(),
            randomSnapshotInvocationRecord(),
            randomNonNegativeLong()
        );

        assertTrue(TransportPutSnapshotLifecycleAction.isNoopUpdate(existingPolicyMeta, existingPolicy, existingHeaders));
        assertFalse(TransportPutSnapshotLifecycleAction.isNoopUpdate(existingPolicyMeta, newPolicy, existingHeaders));
        assertFalse(TransportPutSnapshotLifecycleAction.isNoopUpdate(existingPolicyMeta, existingPolicy, newHeaders));
        assertFalse(TransportPutSnapshotLifecycleAction.isNoopUpdate(null, existingPolicy, existingHeaders));
    }
}
