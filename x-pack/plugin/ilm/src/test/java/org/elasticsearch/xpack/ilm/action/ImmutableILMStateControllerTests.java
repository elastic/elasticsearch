/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.immutablestate.TransformState;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ilm.AllocateAction;
import org.elasticsearch.xpack.core.ilm.DeleteAction;
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.FreezeAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecycleType;
import org.elasticsearch.xpack.core.ilm.MigrateAction;
import org.elasticsearch.xpack.core.ilm.ReadOnlyAction;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.RollupILMAction;
import org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction;
import org.elasticsearch.xpack.core.ilm.SetPriorityAction;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType;
import org.elasticsearch.xpack.core.ilm.UnfollowAction;
import org.elasticsearch.xpack.core.ilm.WaitForSnapshotAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ImmutableILMStateControllerTests extends ESTestCase {

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
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(RollupILMAction.NAME), RollupILMAction::parse)
            )
        );
        return new NamedXContentRegistry(entries);
    }

    private TransformState processJSON(ImmutableLifecycleAction action, TransformState prevState, String json) throws Exception {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            return action.transform(parser.map(), prevState);
        }
    }

    public void testValidationFails() {
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        final ClusterName clusterName = new ClusterName("elasticsearch");

        ClusterState state = ClusterState.builder(clusterName).build();
        ImmutableLifecycleAction action = new ImmutableLifecycleAction(xContentRegistry(), client, mock(XPackLicenseState.class));
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

        ImmutableLifecycleAction action = new ImmutableLifecycleAction(xContentRegistry(), client, mock(XPackLicenseState.class));

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
}
