/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ilm.DownsampleAction;
import org.elasticsearch.xpack.core.ilm.ExplainLifecycleRequest;
import org.elasticsearch.xpack.core.ilm.ExplainLifecycleResponse;
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleExplainResponse;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.PhaseCompleteStep;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.WaitForIndexColorStep;
import org.elasticsearch.xpack.core.ilm.WaitForRolloverReadyStep;
import org.elasticsearch.xpack.core.ilm.action.ExplainLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.test.NodeRoles.onlyRole;
import static org.elasticsearch.test.NodeRoles.onlyRoles;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.ilm.DownsampleAction.DOWNSAMPLED_INDEX_PREFIX;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleActionsRegistry.COLD_PHASE;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleActionsRegistry.CURRENT_VERSION;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleActionsRegistry.HOT_PHASE;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleActionsRegistry.VERSION_ONE;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleActionsRegistry.WARM_PHASE;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ActionsOrderVersioningTests extends ESIntegTestCase {

    private String policy;
    private String managedIndex;

    @Before
    public void refreshDataStreamAndPolicy() {
        policy = "policy-" + randomAlphaOfLengthBetween(10, 15).toLowerCase(Locale.ROOT);
        managedIndex = "index-" + randomAlphaOfLengthBetween(10, 15).toLowerCase(Locale.ROOT) + "-000001";
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class, IndexLifecycle.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        settings.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        settings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        settings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        settings.put(XPackSettings.GRAPH_ENABLED.getKey(), false);
        settings.put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL, "1s");
        settings.put(LifecycleSettings.SLM_HISTORY_INDEX_ENABLED_SETTING.getKey(), false);
        settings.put(LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED, false);
        return settings.build();
    }

    public static Settings hotDataContentNode(final Settings settings) {
        return onlyRoles(settings, Set.of(DiscoveryNodeRole.DATA_HOT_NODE_ROLE, DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE));
    }

    public static Settings warmNode(final Settings settings) {
        return onlyRole(settings, DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
    }

    public static Settings coldNode(final Settings settings) {
        return onlyRole(settings, DiscoveryNodeRole.DATA_COLD_NODE_ROLE);
    }

    public void testWarmExecutesVersionOneAndColdLatestVersion() throws Exception {
        // this test makes sure that if we are in a phase with actions_order_version 1 we will execute
        // the actions as ordered in version one. when transitioning to the next phase, we will check the
        // lifecycle execution state to make sure we switched to the latest actions_order_version

        // this is not trivial to do in an integration test but the way we achieve is it that we take a managed index
        // and update its cluster state to "move" it to the warm phase in the middle of the downsample action whilst also setting the
        // actions_order_version to 1 (we chose downsample action because in version 2 it executes at a different point than in version 1)

        // after this lifecycle "teleport" into warm/downsample/wait-for-index-color (note we don't actually execute the downsampling, we
        // just create the target index so that the downsample action can complete) we wait until we get to warm/complete/complete and
        // assert that the downsample index is located in the warm tier (this means that the version 1 of actions order was indeed
        // executed as `migrate` executes after `downsample` in version 1)

        internalCluster().startMasterOnlyNodes(1, Settings.EMPTY);
        logger.info("-> starting a data_hot/data_content node");
        internalCluster().startNode(hotDataContentNode(Settings.EMPTY));

        logger.info("-> starting a warm data node");
        String warmNodeName = internalCluster().startNode(warmNode(Settings.EMPTY));

        logger.info("-> starting a cold data node");
        String coldNodeName = internalCluster().startNode(coldNode(Settings.EMPTY));

        RolloverAction rolloverIlmAction = new RolloverAction(RolloverConditions.newBuilder().addMaxIndexDocsCondition(1L).build());
        Phase hotPhase = new Phase(HOT_PHASE, TimeValue.ZERO, Map.of(rolloverIlmAction.getWriteableName(), rolloverIlmAction));
        DownsampleAction warmDownsampleAction = new DownsampleAction(new DateHistogramInterval("1d"));
        ForceMergeAction forceMergeAction = new ForceMergeAction(1, null);
        Phase warmPhase = new Phase(
            WARM_PHASE,
            TimeValue.ZERO,
            Map.of(warmDownsampleAction.getWriteableName(), warmDownsampleAction, forceMergeAction.getWriteableName(), forceMergeAction)
        );
        Phase coldPhase = new Phase(COLD_PHASE, TimeValue.timeValueDays(365), Map.of());
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(
            policy,
            Map.of(HOT_PHASE, hotPhase, WARM_PHASE, warmPhase, COLD_PHASE, coldPhase)
        );
        PutLifecycleAction.Request putLifecycleRequest = new PutLifecycleAction.Request(lifecyclePolicy);
        assertAcked(client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get());

        String alias = "aliasName" + randomAlphaOfLengthBetween(5, 10).toLowerCase(Locale.ROOT);
        createIndex(managedIndex, alias, true);

        // allow ILM to pick up the managed index (it'll idle in hot/check-rollover-ready)
        assertBusy(() -> {
            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(managedIndex);
            ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE, explainRequest).get();

            IndexLifecycleExplainResponse indexLifecycleExplainResponse = explainResponse.getIndexResponses().get(managedIndex);
            assertThat(indexLifecycleExplainResponse.getPhase(), is(HOT_PHASE));
            assertThat(indexLifecycleExplainResponse.getStep(), is(WaitForRolloverReadyStep.NAME));
        }, 30, TimeUnit.SECONDS);

        String downsampleIndexName = DOWNSAMPLED_INDEX_PREFIX + managedIndex;
        // creating another index that'll server as the target index for the downsample action
        createIndex(downsampleIndexName, alias, false);
        ensureGreen();

        // oh boy
        // manipulating the lifecycle execution state for the managed index to move it into the warm phase, downsample action,
        // wait-for-index-colour step, and actions order version ONE
        PlainActionFuture.get(
            fut -> internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
                .submitUnbatchedStateUpdateTask(
                    "move index to warm/downsample and actions order version ONE",
                    new ClusterStateUpdateTask() {

                        @Override
                        public ClusterState execute(ClusterState state) {
                            Metadata.Builder builder = Metadata.builder(state.metadata());
                            IndexMetadata originalManagedIndex = state.metadata().index(managedIndex);
                            LifecycleExecutionState originalIndexLifecycleState = LifecycleExecutionState.builder(
                                originalManagedIndex.getLifecycleExecutionState()
                            )
                                .setPhase(WARM_PHASE)
                                .setAction(DownsampleAction.NAME)
                                .setStep(WaitForIndexColorStep.NAME)
                                .setDownsampleIndexName(downsampleIndexName)
                                // NOTE that in this version the downsample action executed _before_ the migrate action (so moving outside
                                // the downsample action should migrate the index to the warm tier using the migrate action)
                                .setActionsOrderVersion(VERSION_ONE)
                                .setPhaseDefinition("""
                                    {
                                      "policy" : "%s",
                                      "phase_definition" : {
                                        "min_age" : "0ms",
                                        "actions" : {
                                          "downsample": {
                                            "fixed_interval": "1d"
                                          }
                                        }
                                      },
                                      "version" : 1,
                                      "modified_date_in_millis" : 1578521007076
                                    }""".formatted(policy))
                                .build();

                            IndexMetadata.Builder managedIndexBuilder = IndexMetadata.builder(originalManagedIndex)
                                .putCustom(ILM_CUSTOM_METADATA_KEY, originalIndexLifecycleState.asMap());

                            builder.put(managedIndexBuilder);
                            return ClusterState.builder(state).metadata(builder).build();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.error(e.getMessage(), e);
                            fail("unable to manipulate the cluster state due to [" + e.getMessage() + "]");
                        }

                        @Override
                        public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                            fut.onResponse(null);
                        }
                    }
                ),
            10,
            TimeUnit.SECONDS
        );

        // the downsample index should end up in WARM/COMPLETE/COMPLETE
        assertBusy(() -> {
            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(downsampleIndexName);
            ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE, explainRequest).get();

            IndexLifecycleExplainResponse indexLifecycleExplainResponse = explainResponse.getIndexResponses().get(downsampleIndexName);
            assertThat(indexLifecycleExplainResponse.getPhase(), is(WARM_PHASE));
            assertThat(indexLifecycleExplainResponse.getStep(), is(PhaseCompleteStep.NAME));

            // actions order version must remain ONE until we transition to COLD
            assertThat(indexLifecycleExplainResponse.getActionsOrderVersion(), is(VERSION_ONE));
        }, 30, TimeUnit.SECONDS);

        {
            // let's check that the migrate action actually did migrate the downsample index to the warm phase (i.e. executed after the
            // downsample action, as it should in VERSION_ONE)
            ClusterAllocationExplainRequest explainDownsampleIndexShard = new ClusterAllocationExplainRequest().setIndex(
                downsampleIndexName
            ).setPrimary(true).setShard(0);
            ClusterAllocationExplainResponse response = clusterAdmin().allocationExplain(explainDownsampleIndexShard).actionGet();
            assertThat(response.getExplanation().getCurrentNode().getName(), is(warmNodeName));
        }

        // changing the min_age for the COLD phase so we transition and assert that the new phase executes the latest actions order version
        lifecyclePolicy = new LifecyclePolicy(
            policy,
            Map.of(HOT_PHASE, hotPhase, WARM_PHASE, warmPhase, COLD_PHASE, new Phase(COLD_PHASE, TimeValue.ZERO, Map.of()))
        );
        putLifecycleRequest = new PutLifecycleAction.Request(lifecyclePolicy);
        assertAcked(client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get());

        assertBusy(() -> {
            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(downsampleIndexName);
            ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE, explainRequest).get();

            IndexLifecycleExplainResponse indexLifecycleExplainResponse = explainResponse.getIndexResponses().get(downsampleIndexName);
            assertThat(indexLifecycleExplainResponse.getPhase(), is(COLD_PHASE));
            assertThat(indexLifecycleExplainResponse.getStep(), is(PhaseCompleteStep.NAME));

            // when we transitioned to cold we should've run the latest actions order version
            assertThat(indexLifecycleExplainResponse.getActionsOrderVersion(), is(CURRENT_VERSION));
        }, 30, TimeUnit.SECONDS);

        {
            ClusterAllocationExplainRequest explainDownsampleIndexShard = new ClusterAllocationExplainRequest().setIndex(
                downsampleIndexName
            ).setPrimary(true).setShard(0);
            ClusterAllocationExplainResponse response = clusterAdmin().allocationExplain(explainDownsampleIndexShard).actionGet();
            assertThat(response.getExplanation().getCurrentNode().getName(), is(coldNodeName));
        }
    }

    private void createIndex(String indexName, String alias, boolean isWriteIndex) {
        Settings settings = Settings.builder()
            .put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 0)
            .put(LifecycleSettings.LIFECYCLE_NAME, policy)
            .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias)
            .build();
        CreateIndexResponse res = indicesAdmin().prepareCreate(indexName).setAliases("""
            {
                    "%s" : { "is_write_index": %b }
            }""".formatted(alias, isWriteIndex)).setSettings(settings).get();
        assertTrue(res.isAcknowledged());
    }
}
