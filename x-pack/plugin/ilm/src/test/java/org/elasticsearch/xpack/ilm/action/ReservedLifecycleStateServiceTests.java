/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.Version;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.reservedstate.action.ReservedClusterSettingsAction;
import org.elasticsearch.reservedstate.service.ReservedClusterStateService;
import org.elasticsearch.reservedstate.service.ReservedStateChunk;
import org.elasticsearch.reservedstate.service.ReservedStateUpdateTask;
import org.elasticsearch.reservedstate.service.ReservedStateUpdateTaskExecutor;
import org.elasticsearch.reservedstate.service.ReservedStateVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ilm.AllocateAction;
import org.elasticsearch.xpack.core.ilm.DeleteAction;
import org.elasticsearch.xpack.core.ilm.DownsampleAction;
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.FreezeAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleType;
import org.elasticsearch.xpack.core.ilm.MigrateAction;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.ReadOnlyAction;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction;
import org.elasticsearch.xpack.core.ilm.SetPriorityAction;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType;
import org.elasticsearch.xpack.core.ilm.UnfollowAction;
import org.elasticsearch.xpack.core.ilm.WaitForSnapshotAction;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReservedLifecycleStateServiceTests extends ESTestCase {

    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedXWriteables());
        entries.addAll(
            Arrays.asList(
                new NamedXContentRegistry.Entry(
                    LifecycleType.class,
                    new ParseField(TimeseriesLifecycleType.TYPE),
                    (p) -> TimeseriesLifecycleType.INSTANCE
                ),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(AllocateAction.NAME), AllocateAction::parse),
                new NamedXContentRegistry.Entry(
                    LifecycleAction.class,
                    new ParseField(WaitForSnapshotAction.NAME),
                    WaitForSnapshotAction::parse
                ),
                new NamedXContentRegistry.Entry(
                    LifecycleAction.class,
                    new ParseField(SearchableSnapshotAction.NAME),
                    SearchableSnapshotAction::parse
                ),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ForceMergeAction.NAME), ForceMergeAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ReadOnlyAction.NAME), ReadOnlyAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(RolloverAction.NAME), RolloverAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ShrinkAction.NAME), ShrinkAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(FreezeAction.NAME), FreezeAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(SetPriorityAction.NAME), SetPriorityAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(MigrateAction.NAME), MigrateAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(UnfollowAction.NAME), UnfollowAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DownsampleAction.NAME), DownsampleAction::parse)
            )
        );
        return new NamedXContentRegistry(entries);
    }

    private TransformState processJSON(ReservedLifecycleAction action, TransformState prevState, String json) throws Exception {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            return action.transform(action.fromXContent(parser), prevState);
        }
    }

    public void testValidationFails() {
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        final ClusterName clusterName = new ClusterName("elasticsearch");

        ClusterState state = ClusterState.builder(clusterName).build();
        ReservedLifecycleAction action = new ReservedLifecycleAction(xContentRegistry(), client, mock(XPackLicenseState.class));
        TransformState prevState = new TransformState(state, Collections.emptySet());

        String badPolicyJSON = """
            {
                "my_timeseries_lifecycle": {
                    "phase": {
                        "warm": {
                            "min_age": "10s",
                            "actions": {
                            }
                        }
                    }
                }
            }""";

        assertEquals(
            "[1:2] [lifecycle_policy] unknown field [phase] did you mean [phases]?",
            expectThrows(XContentParseException.class, () -> processJSON(action, prevState, badPolicyJSON)).getMessage()
        );
    }

    public void testActionAddRemove() throws Exception {
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        final ClusterName clusterName = new ClusterName("elasticsearch");

        ClusterState state = ClusterState.builder(clusterName).build();

        ReservedLifecycleAction action = new ReservedLifecycleAction(xContentRegistry(), client, mock(XPackLicenseState.class));

        String emptyJSON = "";

        TransformState prevState = new TransformState(state, Collections.emptySet());

        TransformState updatedState = processJSON(action, prevState, emptyJSON);
        assertEquals(0, updatedState.keys().size());
        assertEquals(prevState.state(), updatedState.state());

        String twoPoliciesJSON = """
            {
                "my_timeseries_lifecycle": {
                    "phases": {
                        "warm": {
                            "min_age": "10s",
                            "actions": {
                            }
                        }
                    }
                },
                "my_timeseries_lifecycle1": {
                    "phases": {
                        "warm": {
                            "min_age": "10s",
                            "actions": {
                            }
                        },
                        "delete": {
                            "min_age": "30s",
                            "actions": {
                            }
                        }
                    }
                }
            }""";

        prevState = updatedState;
        updatedState = processJSON(action, prevState, twoPoliciesJSON);
        assertThat(updatedState.keys(), containsInAnyOrder("my_timeseries_lifecycle", "my_timeseries_lifecycle1"));
        IndexLifecycleMetadata ilmMetadata = updatedState.state()
            .metadata()
            .custom(IndexLifecycleMetadata.TYPE, IndexLifecycleMetadata.EMPTY);
        assertThat(ilmMetadata.getPolicyMetadatas().keySet(), containsInAnyOrder("my_timeseries_lifecycle", "my_timeseries_lifecycle1"));

        String onePolicyRemovedJSON = """
            {
                "my_timeseries_lifecycle": {
                    "phases": {
                        "warm": {
                            "min_age": "10s",
                            "actions": {
                            }
                        }
                    }
                }
            }""";

        prevState = updatedState;
        updatedState = processJSON(action, prevState, onePolicyRemovedJSON);
        assertThat(updatedState.keys(), containsInAnyOrder("my_timeseries_lifecycle"));
        ilmMetadata = updatedState.state().metadata().custom(IndexLifecycleMetadata.TYPE, IndexLifecycleMetadata.EMPTY);
        assertThat(ilmMetadata.getPolicyMetadatas().keySet(), containsInAnyOrder("my_timeseries_lifecycle"));

        String onePolicyRenamedJSON = """
            {
                "my_timeseries_lifecycle2": {
                    "phases": {
                        "warm": {
                            "min_age": "10s",
                            "actions": {
                            }
                        }
                    }
                }
            }""";

        prevState = updatedState;
        updatedState = processJSON(action, prevState, onePolicyRenamedJSON);
        assertThat(updatedState.keys(), containsInAnyOrder("my_timeseries_lifecycle2"));
        ilmMetadata = updatedState.state().metadata().custom(IndexLifecycleMetadata.TYPE, IndexLifecycleMetadata.EMPTY);
        assertThat(ilmMetadata.getPolicyMetadatas().keySet(), containsInAnyOrder("my_timeseries_lifecycle2"));
    }

    private void setupTaskMock(ClusterService clusterService, ClusterState state) {
        doAnswer((Answer<Object>) invocation -> {
            Object[] args = invocation.getArguments();

            if ((args[3] instanceof ReservedStateUpdateTaskExecutor) == false) {
                fail("Should have gotten a state update task to execute, instead got: " + args[3].getClass().getName());
            }

            ReservedStateUpdateTaskExecutor task = (ReservedStateUpdateTaskExecutor) args[3];

            ClusterStateTaskExecutor.TaskContext<ReservedStateUpdateTask> context = new ClusterStateTaskExecutor.TaskContext<>() {
                @Override
                public ReservedStateUpdateTask getTask() {
                    return (ReservedStateUpdateTask) args[1];
                }

                @Override
                public void success(Runnable onPublicationSuccess) {}

                @Override
                public void success(Consumer<ClusterState> publishedStateConsumer) {}

                @Override
                public void success(Runnable onPublicationSuccess, ClusterStateAckListener clusterStateAckListener) {}

                @Override
                public void success(Consumer<ClusterState> publishedStateConsumer, ClusterStateAckListener clusterStateAckListener) {}

                @Override
                public void onFailure(Exception failure) {
                    fail("Shouldn't fail here");
                }

                @Override
                public Releasable captureResponseHeaders() {
                    return null;
                }
            };

            task.execute(new ClusterStateTaskExecutor.BatchExecutionContext<>(state, List.of(context), () -> null));

            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any(), any(), any());
    }

    public void testOperatorControllerFromJSONContent() throws IOException {
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
                     },
                     "ilm": {
                         "my_timeseries_lifecycle": {
                             "phases": {
                                 "hot": {
                                     "min_age": "10s",
                                     "actions": {
                                        "rollover": {
                                           "max_primary_shard_size": "50gb",
                                           "max_age": "30d"
                                        }
                                     }
                                 },
                                 "delete": {
                                     "min_age": "30s",
                                     "actions": {
                                     }
                                 }
                             }
                         },
                         "my_timeseries_lifecycle1": {
                             "phases": {
                                 "warm": {
                                     "min_age": "10s",
                                     "actions": {
                                        "shrink": {
                                          "number_of_shards": 1
                                        },
                                        "forcemerge": {
                                          "max_num_segments": 1
                                        }
                                     }
                                 },
                                 "delete": {
                                     "min_age": "30s",
                                     "actions": {
                                     }
                                 }
                             }
                         }
                     }
                 }
            }""";

        AtomicReference<Exception> x = new AtomicReference<>();

        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, testJSON)) {
            controller.process("operator", parser, (e) -> x.set(e));

            assertTrue(x.get() instanceof IllegalStateException);
            assertThat(x.get().getMessage(), containsString("Error processing state change request for operator"));
        }

        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);

        XPackLicenseState licenseState = mock(XPackLicenseState.class);

        controller = new ReservedClusterStateService(
            clusterService,
            List.of(
                new ReservedClusterSettingsAction(clusterSettings),
                new ReservedLifecycleAction(xContentRegistry(), client, licenseState)
            )
        );

        setupTaskMock(clusterService, state);

        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, testJSON)) {
            controller.process("operator", parser, (e) -> {
                if (e != null) {
                    fail("Should not fail");
                }
            });
        }
    }

    public void testOperatorControllerWithPluginPackage() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = mock(ClusterService.class);
        final ClusterName clusterName = new ClusterName("elasticsearch");

        ClusterState state = ClusterState.builder(clusterName).build();
        when(clusterService.state()).thenReturn(state);

        ReservedClusterStateService controller = new ReservedClusterStateService(
            clusterService,
            List.of(new ReservedClusterSettingsAction(clusterSettings))
        );

        AtomicReference<Exception> x = new AtomicReference<>();

        ReservedStateChunk pack = new ReservedStateChunk(
            Map.of(
                ReservedClusterSettingsAction.NAME,
                Map.of("indices.recovery.max_bytes_per_sec", "50mb"),
                ReservedLifecycleAction.NAME,
                List.of(
                    new LifecyclePolicy(
                        "my_timeseries_lifecycle",
                        Map.of(
                            "warm",
                            new Phase("warm", new TimeValue(10, TimeUnit.SECONDS), Collections.emptyMap()),
                            "delete",
                            new Phase("delete", new TimeValue(30, TimeUnit.SECONDS), Collections.emptyMap())
                        )
                    )
                )
            ),
            new ReservedStateVersion(123L, Version.CURRENT)
        );

        controller.process("operator", pack, (e) -> x.set(e));

        assertTrue(x.get() instanceof IllegalStateException);
        assertThat(x.get().getMessage(), containsString("Error processing state change request for operator"));

        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);

        XPackLicenseState licenseState = mock(XPackLicenseState.class);

        controller = new ReservedClusterStateService(
            clusterService,
            List.of(
                new ReservedClusterSettingsAction(clusterSettings),
                new ReservedLifecycleAction(xContentRegistry(), client, licenseState)
            )
        );

        setupTaskMock(clusterService, state);

        controller.process("operator", pack, (e) -> {
            if (e != null) {
                fail("Should not fail");
            }
        });
    }
}
