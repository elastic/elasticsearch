/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.xpack.core.ilm.PhaseCacheManagement.eligibleToCheckForRefresh;
import static org.elasticsearch.xpack.core.ilm.PhaseCacheManagement.isIndexPhaseDefinitionUpdatable;
import static org.elasticsearch.xpack.core.ilm.PhaseCacheManagement.readStepKeys;
import static org.elasticsearch.xpack.core.ilm.PhaseCacheManagement.refreshPhaseDefinition;
import static org.elasticsearch.xpack.core.ilm.PhaseCacheManagement.updateIndicesForPolicy;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class PhaseCacheManagementTests extends ESTestCase {

    private static final NamedXContentRegistry REGISTRY;
    private static final Client client = mock(Client.class);
    private static final String index = "eggplant";

    static {
        REGISTRY = new NamedXContentRegistry(
            List.of(
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(RolloverAction.NAME), RolloverAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(SetPriorityAction.NAME), SetPriorityAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ForceMergeAction.NAME), ForceMergeAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(AllocateAction.NAME), AllocateAction::parse)
            )
        );
    }

    public void testRefreshPhaseJson() throws IOException {
        LifecycleExecutionState.Builder exState = LifecycleExecutionState.builder()
            .setPhase("hot")
            .setAction("rollover")
            .setStep("check-rollover-ready")
            .setPhaseDefinition("""
                {
                  "policy" : "my-policy",
                  "phase_definition" : {
                    "min_age" : "20m",
                    "actions" : {
                      "rollover" : {
                        "max_age" : "5s"
                      },
                      "set_priority" : {
                        "priority" : 150
                      }
                    }
                  },
                  "version" : 1,
                  "modified_date_in_millis" : 1578521007076
                }""");

        IndexMetadata meta = buildIndexMetadata("my-policy", exState);
        String indexName = meta.getIndex().getName();

        Map<String, LifecycleAction> actions = new HashMap<>();
        actions.put("rollover", new RolloverAction(null, null, null, 1L, null, null, null, null, null, null));
        actions.put("set_priority", new SetPriorityAction(100));
        Phase hotPhase = new Phase("hot", TimeValue.ZERO, actions);
        Map<String, Phase> phases = Collections.singletonMap("hot", hotPhase);
        LifecyclePolicy newPolicy = new LifecyclePolicy("my-policy", phases);
        LifecyclePolicyMetadata policyMetadata = new LifecyclePolicyMetadata(newPolicy, Collections.emptyMap(), 2L, 2L);

        ClusterState existingState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder(Metadata.EMPTY_METADATA).put(meta, false).build())
            .build();

        ClusterState changedState = refreshPhaseDefinition(existingState, indexName, policyMetadata);

        IndexMetadata newIdxMeta = changedState.metadata().index(indexName);
        LifecycleExecutionState afterExState = newIdxMeta.getLifecycleExecutionState();
        Map<String, String> beforeState = new HashMap<>(exState.build().asMap());
        beforeState.remove("phase_definition");
        Map<String, String> afterState = new HashMap<>(afterExState.asMap());
        afterState.remove("phase_definition");
        // Check that no other execution state changes have been made
        assertThat(beforeState, equalTo(afterState));

        // Check that the phase definition has been refreshed
        assertThat(afterExState.phaseDefinition(), equalTo(XContentHelper.stripWhitespace("""
            {
              "policy": "my-policy",
              "phase_definition": {
                "min_age": "0ms",
                "actions": {
                  "rollover": {
                    "max_docs": 1
                  },
                  "set_priority": {
                    "priority": 100
                  }
                }
              },
              "version": 2,
              "modified_date_in_millis": 2
            }""")));
    }

    public void testEligibleForRefresh() {
        IndexMetadata meta = IndexMetadata.builder("index")
            .settings(
                indexSettings(Version.CURRENT, randomIntBetween(1, 10), randomIntBetween(0, 5)).put(
                    IndexMetadata.SETTING_INDEX_UUID,
                    randomAlphaOfLength(5)
                )
            )
            .build();
        assertFalse(eligibleToCheckForRefresh(meta));

        LifecycleExecutionState state = LifecycleExecutionState.builder().build();
        meta = IndexMetadata.builder("index")
            .settings(
                indexSettings(Version.CURRENT, randomIntBetween(1, 10), randomIntBetween(0, 5)).put(
                    IndexMetadata.SETTING_INDEX_UUID,
                    randomAlphaOfLength(5)
                )
            )
            .putCustom(ILM_CUSTOM_METADATA_KEY, state.asMap())
            .build();
        assertFalse(eligibleToCheckForRefresh(meta));

        state = LifecycleExecutionState.builder().setPhase("phase").setAction("action").setStep("step").build();
        meta = IndexMetadata.builder("index")
            .settings(
                indexSettings(Version.CURRENT, randomIntBetween(1, 10), randomIntBetween(0, 5)).put(
                    IndexMetadata.SETTING_INDEX_UUID,
                    randomAlphaOfLength(5)
                )
            )
            .putCustom(ILM_CUSTOM_METADATA_KEY, state.asMap())
            .build();
        assertFalse(eligibleToCheckForRefresh(meta));

        state = LifecycleExecutionState.builder().setPhaseDefinition("{}").build();
        meta = IndexMetadata.builder("index")
            .settings(
                indexSettings(Version.CURRENT, randomIntBetween(1, 10), randomIntBetween(0, 5)).put(
                    IndexMetadata.SETTING_INDEX_UUID,
                    randomAlphaOfLength(5)
                )
            )
            .putCustom(ILM_CUSTOM_METADATA_KEY, state.asMap())
            .build();
        assertFalse(eligibleToCheckForRefresh(meta));

        state = LifecycleExecutionState.builder()
            .setPhase("phase")
            .setAction("action")
            .setStep(ErrorStep.NAME)
            .setPhaseDefinition("{}")
            .build();
        meta = IndexMetadata.builder("index")
            .settings(
                indexSettings(Version.CURRENT, randomIntBetween(1, 10), randomIntBetween(0, 5)).put(
                    IndexMetadata.SETTING_INDEX_UUID,
                    randomAlphaOfLength(5)
                )
            )
            .putCustom(ILM_CUSTOM_METADATA_KEY, state.asMap())
            .build();
        assertFalse(eligibleToCheckForRefresh(meta));

        state = LifecycleExecutionState.builder().setPhase("phase").setAction("action").setStep("step").setPhaseDefinition("{}").build();
        meta = IndexMetadata.builder("index")
            .settings(
                indexSettings(Version.CURRENT, randomIntBetween(1, 10), randomIntBetween(0, 5)).put(
                    IndexMetadata.SETTING_INDEX_UUID,
                    randomAlphaOfLength(5)
                )
            )
            .putCustom(ILM_CUSTOM_METADATA_KEY, state.asMap())
            .build();
        assertTrue(eligibleToCheckForRefresh(meta));
    }

    public void testReadStepKeys() {
        assertNull(readStepKeys(REGISTRY, client, null, "phase", null));
        assertNull(readStepKeys(REGISTRY, client, "{}", "phase", null));
        assertNull(readStepKeys(REGISTRY, client, "aoeu", "phase", null));
        assertNull(readStepKeys(REGISTRY, client, "", "phase", null));

        assertThat(
            readStepKeys(REGISTRY, client, """
                {
                  "policy": "my_lifecycle3",
                  "phase_definition": {
                    "min_age": "0ms",
                    "actions": {
                      "rollover": {
                        "max_age": "30s"
                      }
                    }
                  },
                  "version": 3,
                  "modified_date_in_millis": 1539609701576
                }""", "phase", null),
            contains(
                new Step.StepKey("phase", "rollover", WaitForRolloverReadyStep.NAME),
                new Step.StepKey("phase", "rollover", RolloverStep.NAME),
                new Step.StepKey("phase", "rollover", WaitForActiveShardsStep.NAME),
                new Step.StepKey("phase", "rollover", UpdateRolloverLifecycleDateStep.NAME),
                new Step.StepKey("phase", "rollover", RolloverAction.INDEXING_COMPLETE_STEP_NAME)
            )
        );

        assertThat(
            readStepKeys(REGISTRY, client, """
                {
                  "policy" : "my_lifecycle3",
                  "phase_definition" : {
                    "min_age" : "20m",
                    "actions" : {
                      "rollover" : {
                        "max_age" : "5s"
                      },
                      "set_priority" : {
                        "priority" : 150
                      }
                    }
                  },
                  "version" : 1,
                  "modified_date_in_millis" : 1578521007076
                }""", "phase", null),
            containsInAnyOrder(
                new Step.StepKey("phase", "rollover", WaitForRolloverReadyStep.NAME),
                new Step.StepKey("phase", "rollover", RolloverStep.NAME),
                new Step.StepKey("phase", "rollover", WaitForActiveShardsStep.NAME),
                new Step.StepKey("phase", "rollover", UpdateRolloverLifecycleDateStep.NAME),
                new Step.StepKey("phase", "rollover", RolloverAction.INDEXING_COMPLETE_STEP_NAME),
                new Step.StepKey("phase", "set_priority", SetPriorityAction.NAME)
            )
        );

        Map<String, LifecycleAction> actions = new HashMap<>();
        actions.put("forcemerge", new ForceMergeAction(5, null));
        actions.put("allocate", new AllocateAction(1, 20, null, null, null));
        PhaseExecutionInfo pei = new PhaseExecutionInfo("policy", new Phase("wonky", TimeValue.ZERO, actions), 1, 1);
        String phaseDef = Strings.toString(pei);
        logger.info("--> phaseDef: {}", phaseDef);

        assertThat(
            readStepKeys(REGISTRY, client, phaseDef, "phase", null),
            contains(
                new Step.StepKey("phase", "allocate", AllocateAction.NAME),
                new Step.StepKey("phase", "allocate", AllocationRoutedStep.NAME),
                new Step.StepKey("phase", "forcemerge", ForceMergeAction.CONDITIONAL_SKIP_FORCE_MERGE_STEP),
                new Step.StepKey("phase", "forcemerge", CheckNotDataStreamWriteIndexStep.NAME),
                // This read-only key is now a noop step but we preserved it for backwards compatibility
                new Step.StepKey("phase", "forcemerge", ReadOnlyAction.NAME),
                new Step.StepKey("phase", "forcemerge", ForceMergeAction.NAME),
                new Step.StepKey("phase", "forcemerge", SegmentCountStep.NAME)
            )
        );
    }

    public void testIndexCanBeSafelyUpdated() {

        // Success case, it can be updated even though the configuration for the
        // rollover and set_priority actions has changed
        {
            LifecycleExecutionState exState = LifecycleExecutionState.builder()
                .setPhase("hot")
                .setAction("rollover")
                .setStep("check-rollover-ready")
                .setPhaseDefinition("""
                    {
                      "policy" : "my-policy",
                      "phase_definition" : {
                        "min_age" : "20m",
                        "actions" : {
                          "rollover" : {
                            "max_age" : "5s"
                          },
                          "set_priority" : {
                            "priority" : 150
                          }
                        }
                      },
                      "version" : 1,
                      "modified_date_in_millis" : 1578521007076
                    }""")
                .build();

            IndexMetadata meta = mkMeta().putCustom(ILM_CUSTOM_METADATA_KEY, exState.asMap()).build();

            Map<String, LifecycleAction> actions = new HashMap<>();
            actions.put("rollover", new RolloverAction(null, null, null, 1L, null, null, null, null, null, null));
            actions.put("set_priority", new SetPriorityAction(100));
            Phase hotPhase = new Phase("hot", TimeValue.ZERO, actions);
            Map<String, Phase> phases = Collections.singletonMap("hot", hotPhase);
            LifecyclePolicy newPolicy = new LifecyclePolicy("my-policy", phases);

            assertTrue(isIndexPhaseDefinitionUpdatable(REGISTRY, client, meta, newPolicy, null));
        }

        // Failure case, can't update because the step we're currently on has been removed in the new policy
        {
            LifecycleExecutionState exState = LifecycleExecutionState.builder()
                .setPhase("hot")
                .setAction("rollover")
                .setStep("check-rollover-ready")
                .setPhaseDefinition("""
                    {
                      "policy" : "my-policy",
                      "phase_definition" : {
                        "min_age" : "20m",
                        "actions" : {
                          "rollover" : {
                            "max_age" : "5s"
                          },
                          "set_priority" : {
                            "priority" : 150
                          }
                        }
                      },
                      "version" : 1,
                      "modified_date_in_millis" : 1578521007076
                    }""")
                .build();

            IndexMetadata meta = mkMeta().putCustom(ILM_CUSTOM_METADATA_KEY, exState.asMap()).build();

            Map<String, LifecycleAction> actions = new HashMap<>();
            actions.put("set_priority", new SetPriorityAction(150));
            Phase hotPhase = new Phase("hot", TimeValue.ZERO, actions);
            Map<String, Phase> phases = Collections.singletonMap("hot", hotPhase);
            LifecyclePolicy newPolicy = new LifecyclePolicy("my-policy", phases);

            assertFalse(isIndexPhaseDefinitionUpdatable(REGISTRY, client, meta, newPolicy, null));
        }

        // Failure case, can't update because the future step has been deleted
        {
            LifecycleExecutionState exState = LifecycleExecutionState.builder()
                .setPhase("hot")
                .setAction("rollover")
                .setStep("check-rollover-ready")
                .setPhaseDefinition("""
                    {
                      "policy" : "my-policy",
                      "phase_definition" : {
                        "min_age" : "20m",
                        "actions" : {
                          "rollover" : {
                            "max_age" : "5s"
                          },
                          "set_priority" : {
                            "priority" : 150
                          }
                        }
                      },
                      "version" : 1,
                      "modified_date_in_millis" : 1578521007076
                    }""")
                .build();

            IndexMetadata meta = mkMeta().putCustom(ILM_CUSTOM_METADATA_KEY, exState.asMap()).build();

            Map<String, LifecycleAction> actions = new HashMap<>();
            actions.put(
                "rollover",
                new RolloverAction(null, null, TimeValue.timeValueSeconds(5), null, null, null, null, null, null, null)
            );
            Phase hotPhase = new Phase("hot", TimeValue.ZERO, actions);
            Map<String, Phase> phases = Collections.singletonMap("hot", hotPhase);
            LifecyclePolicy newPolicy = new LifecyclePolicy("my-policy", phases);

            assertFalse(isIndexPhaseDefinitionUpdatable(REGISTRY, client, meta, newPolicy, null));
        }

        // Failure case, index doesn't have enough info to check
        {
            LifecycleExecutionState exState = LifecycleExecutionState.builder().setPhaseDefinition("""
                {
                  "policy" : "my-policy",
                  "phase_definition" : {
                    "min_age" : "20m",
                    "actions" : {
                      "rollover" : {
                        "max_age" : "5s"
                      },
                      "set_priority" : {
                        "priority" : 150
                      }
                    }
                  },
                  "version" : 1,
                  "modified_date_in_millis" : 1578521007076
                }""").build();

            IndexMetadata meta = mkMeta().putCustom(ILM_CUSTOM_METADATA_KEY, exState.asMap()).build();

            Map<String, LifecycleAction> actions = new HashMap<>();
            actions.put("rollover", new RolloverAction(null, null, null, 1L, null, null, null, null, null, null));
            actions.put("set_priority", new SetPriorityAction(100));
            Phase hotPhase = new Phase("hot", TimeValue.ZERO, actions);
            Map<String, Phase> phases = Collections.singletonMap("hot", hotPhase);
            LifecyclePolicy newPolicy = new LifecyclePolicy("my-policy", phases);

            assertFalse(isIndexPhaseDefinitionUpdatable(REGISTRY, client, meta, newPolicy, null));
        }

        // Failure case, the phase JSON is unparseable
        {
            LifecycleExecutionState exState = LifecycleExecutionState.builder()
                .setPhase("hot")
                .setAction("rollover")
                .setStep("check-rollover-ready")
                .setPhaseDefinition("potato")
                .build();

            IndexMetadata meta = mkMeta().putCustom(ILM_CUSTOM_METADATA_KEY, exState.asMap()).build();

            Map<String, LifecycleAction> actions = new HashMap<>();
            actions.put("rollover", new RolloverAction(null, null, null, 1L, null, null, null, null, null, null));
            actions.put("set_priority", new SetPriorityAction(100));
            Phase hotPhase = new Phase("hot", TimeValue.ZERO, actions);
            Map<String, Phase> phases = Collections.singletonMap("hot", hotPhase);
            LifecyclePolicy newPolicy = new LifecyclePolicy("my-policy", phases);

            assertFalse(isIndexPhaseDefinitionUpdatable(REGISTRY, client, meta, newPolicy, null));
        }
    }

    public void testUpdateIndicesForPolicy() throws IOException {
        LifecycleExecutionState exState = LifecycleExecutionState.builder()
            .setPhase("hot")
            .setAction("rollover")
            .setStep("check-rollover-ready")
            .setPhaseDefinition("""
                {
                  "policy": "my-policy",
                  "phase_definition": {
                    "min_age": "0ms",
                    "actions": {
                      "rollover": {
                        "max_docs": 1
                      },
                      "set_priority": {
                        "priority": 100
                      }
                    }
                  },
                  "version": 1,
                  "modified_date_in_millis": 1578521007076
                }""")
            .build();

        IndexMetadata meta = mkMeta().putCustom(ILM_CUSTOM_METADATA_KEY, exState.asMap()).build();

        assertTrue(eligibleToCheckForRefresh(meta));

        Map<String, LifecycleAction> oldActions = new HashMap<>();
        oldActions.put("rollover", new RolloverAction(null, null, null, 1L, null, null, null, null, null, null));
        oldActions.put("set_priority", new SetPriorityAction(100));
        Phase oldHotPhase = new Phase("hot", TimeValue.ZERO, oldActions);
        Map<String, Phase> oldPhases = Collections.singletonMap("hot", oldHotPhase);
        LifecyclePolicy oldPolicy = new LifecyclePolicy("my-policy", oldPhases);

        Map<String, LifecycleAction> actions = new HashMap<>();
        actions.put("rollover", new RolloverAction(null, null, null, 1L, null, null, null, null, null, null));
        actions.put("set_priority", new SetPriorityAction(100));
        Phase hotPhase = new Phase("hot", TimeValue.ZERO, actions);
        Map<String, Phase> phases = Collections.singletonMap("hot", hotPhase);
        LifecyclePolicy newPolicy = new LifecyclePolicy("my-policy", phases);
        LifecyclePolicyMetadata policyMetadata = new LifecyclePolicyMetadata(newPolicy, Collections.emptyMap(), 2L, 2L);

        assertTrue(isIndexPhaseDefinitionUpdatable(REGISTRY, client, meta, newPolicy, null));

        ClusterState existingState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder(Metadata.EMPTY_METADATA).put(meta, false).build())
            .build();

        logger.info("--> update for unchanged policy");
        ClusterState updatedState = updateIndicesForPolicy(existingState, REGISTRY, client, oldPolicy, policyMetadata, null);

        // No change, because the policies were identical
        assertThat(updatedState, equalTo(existingState));

        actions = new HashMap<>();
        actions.put("rollover", new RolloverAction(null, null, null, 2L, null, null, null, null, null, null));
        actions.put("set_priority", new SetPriorityAction(150));
        hotPhase = new Phase("hot", TimeValue.ZERO, actions);
        phases = Collections.singletonMap("hot", hotPhase);
        newPolicy = new LifecyclePolicy("my-policy", phases);
        policyMetadata = new LifecyclePolicyMetadata(newPolicy, Collections.emptyMap(), 2L, 2L);

        logger.info("--> update with changed policy, but not configured in settings");
        updatedState = updateIndicesForPolicy(existingState, REGISTRY, client, oldPolicy, policyMetadata, null);

        // No change, because the index doesn't have a lifecycle.name setting for this policy
        assertThat(updatedState, equalTo(existingState));

        meta = IndexMetadata.builder(index)
            .settings(
                indexSettings(Version.CURRENT, randomIntBetween(1, 10), randomIntBetween(0, 5)).put(
                    LifecycleSettings.LIFECYCLE_NAME,
                    "my-policy"
                ).put(IndexMetadata.SETTING_INDEX_UUID, randomAlphaOfLength(5))
            )
            .putCustom(ILM_CUSTOM_METADATA_KEY, exState.asMap())
            .build();
        existingState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder(Metadata.EMPTY_METADATA).put(meta, false).build())
            .build();

        logger.info("--> update with changed policy and this index has the policy");
        updatedState = updateIndicesForPolicy(existingState, REGISTRY, client, oldPolicy, policyMetadata, null);

        IndexMetadata newIdxMeta = updatedState.metadata().index(index);
        LifecycleExecutionState afterExState = newIdxMeta.getLifecycleExecutionState();
        Map<String, String> beforeState = new HashMap<>(exState.asMap());
        beforeState.remove("phase_definition");
        Map<String, String> afterState = new HashMap<>(afterExState.asMap());
        afterState.remove("phase_definition");
        // Check that no other execution state changes have been made
        assertThat(beforeState, equalTo(afterState));

        // Check that the phase definition has been refreshed
        assertThat(afterExState.phaseDefinition(), equalTo(XContentHelper.stripWhitespace("""
            {
              "policy": "my-policy",
              "phase_definition": {
                "min_age": "0ms",
                "actions": {
                  "rollover": {
                    "max_docs": 2
                  },
                  "set_priority": {
                    "priority": 150
                  }
                }
              },
              "version": 2,
              "modified_date_in_millis": 2
            }""")));
    }

    private IndexMetadata buildIndexMetadata(String policy, LifecycleExecutionState.Builder lifecycleState) {
        return IndexMetadata.builder("index")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policy))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .build();
    }

    private static IndexMetadata.Builder mkMeta() {
        return IndexMetadata.builder(index)
            .settings(
                indexSettings(Version.CURRENT, randomIntBetween(1, 10), randomIntBetween(0, 5)).put(
                    IndexMetadata.SETTING_INDEX_UUID,
                    randomAlphaOfLength(5)
                )
            );
    }

}
