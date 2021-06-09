/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ilm.ExplainLifecycleRequest;
import org.elasticsearch.xpack.core.ilm.ExplainLifecycleResponse;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleExplainResponse;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.MigrateAction;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.PhaseCompleteStep;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.ShrunkShardsAllocatedStep;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.action.ExplainLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ClusterStateWaitThresholdBreachTests extends ESIntegTestCase {

    private String policy;
    private String managedIndex;

    @Before
    public void refreshDataStreamAndPolicy() {
        policy = "policy-" + randomAlphaOfLength(5);
        managedIndex = "index-" + randomAlphaOfLengthBetween(10, 15).toLowerCase(Locale.ROOT);
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

    public void testWaitInShrunkShardsAllocatedExceedsThreshold() throws Exception {
        List<String> masterOnlyNodes = internalCluster().startMasterOnlyNodes(1, Settings.EMPTY);
        internalCluster().startDataOnlyNode();

        int numShards = 2;
        {
            Phase warmPhase = new Phase("warm", TimeValue.ZERO, Map.of(MigrateAction.NAME, new MigrateAction(false), ShrinkAction.NAME,
                new ShrinkAction(numShards + randomIntBetween(1, numShards), null)));
            LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, Map.of("warm", warmPhase));
            PutLifecycleAction.Request putLifecycleRequest = new PutLifecycleAction.Request(lifecyclePolicy);
            assertAcked(client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get());
        }

        Settings settings = Settings.builder().put(indexSettings()).put(SETTING_NUMBER_OF_SHARDS, numShards)
            .put(SETTING_NUMBER_OF_REPLICAS, 0).put(LifecycleSettings.LIFECYCLE_NAME, policy)
            // configuring the threshold to the minimum value
            .put(LifecycleSettings.LIFECYCLE_STEP_WAIT_TIME_THRESHOLD, "1h")
            .build();
        CreateIndexResponse res = client().admin().indices().prepareCreate(managedIndex).setSettings(settings).get();
        assertTrue(res.isAcknowledged());

        String[] firstAttemptShrinkIndexName = new String[1];
        // ILM will retry the shrink step because the number of shards to shrink to is gt the current number of shards
        assertBusy(() -> {
            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(managedIndex);
            ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE,
                explainRequest).get();

            IndexLifecycleExplainResponse indexLifecycleExplainResponse = explainResponse.getIndexResponses().get(managedIndex);
            assertThat(indexLifecycleExplainResponse.getFailedStepRetryCount(), greaterThanOrEqualTo(1));

            firstAttemptShrinkIndexName[0] = indexLifecycleExplainResponse.getShrinkIndexName();
            assertThat(firstAttemptShrinkIndexName[0], is(notNullValue()));
        }, 30, TimeUnit.SECONDS);


        // we're manually shrinking the index but configuring a very high number of replicas and waiting for all active shards
        // this will make ths shrunk index unable to allocate successfully, so ILM will wait in the `shrunk-shards-allocated` step
        ResizeRequest resizeRequest = new ResizeRequest(firstAttemptShrinkIndexName[0], managedIndex);
        Settings.Builder builder = Settings.builder();
        // a very high number of replicas, coupled with an `all` wait for active shards configuration will block the shrink action in the
        // `shrunk-shards-allocated` step.
        builder.put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 42)
            .put("index.write.wait_for_active_shards", "all")
            .put(LifecycleSettings.LIFECYCLE_NAME, policy)
            .put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id", (String) null)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);
        Settings relevantTargetSettings = builder.build();
        resizeRequest.getTargetIndexRequest().settings(relevantTargetSettings);
        client().admin().indices().resizeIndex(resizeRequest).get();
        ensureYellow(firstAttemptShrinkIndexName[0]);

        // let's check ILM for the managed index is waiting in the `shrunk-shards-allocated` step
        assertBusy(() -> {
            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(managedIndex);
            ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE,
                explainRequest).get();

            IndexLifecycleExplainResponse indexLifecycleExplainResponse = explainResponse.getIndexResponses().get(managedIndex);
            assertThat(indexLifecycleExplainResponse.getStep(), is(ShrunkShardsAllocatedStep.NAME));
        }, 30, TimeUnit.SECONDS);

        // now to the tricky bit
        // we'll use the cluster service to issue a move-to-step task in order to manipulate the ILM execution state `step_time` value to
        // a very low value (in order to trip the LIFECYCLE_STEP_WAIT_TIME_THRESHOLD threshold and retry the shrink cycle)
        IndexMetadata managedIndexMetadata = clusterService().state().metadata().index(managedIndex);
        Step.StepKey currentStepKey = new Step.StepKey("warm", ShrinkAction.NAME, ShrunkShardsAllocatedStep.NAME);

        String masterNode = masterOnlyNodes.get(0);
        IndexLifecycleService indexLifecycleService = internalCluster().getInstance(IndexLifecycleService.class, masterNode);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, masterNode);
        // moving the step from the current step to the same step - this is only so we update the step time in the ILM execution state to
        // an old timestamp so the `1h` wait threshold we configured using LIFECYCLE_STEP_WAIT_TIME_THRESHOLD is breached and a new
        // shrink cycle is started
        LongSupplier nowWayBackInThePastSupplier = () -> 1234L;
        clusterService.submitStateUpdateTask("testing-move-to-step-to-manipulate-step-time",
            new MoveToNextStepUpdateTask(managedIndexMetadata.getIndex(), policy, currentStepKey, currentStepKey,
                nowWayBackInThePastSupplier, indexLifecycleService.getPolicyRegistry(), state -> {
            }));

        String[] secondCycleShrinkIndexName = new String[1];
        assertBusy(() -> {
            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(managedIndex);
            ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE,
                explainRequest).get();

            IndexLifecycleExplainResponse indexLifecycleExplainResponse = explainResponse.getIndexResponses().get(managedIndex);
            secondCycleShrinkIndexName[0] = indexLifecycleExplainResponse.getShrinkIndexName();
            assertThat(secondCycleShrinkIndexName[0], notNullValue());
            // ILM generated another shrink index name
            assertThat(secondCycleShrinkIndexName[0], not(equalTo(firstAttemptShrinkIndexName[0])));
        }, 30, TimeUnit.SECONDS);

        // the shrink index generated in the first attempt must've been deleted!
        assertBusy(() -> assertFalse(indexExists(firstAttemptShrinkIndexName[0])));

        // at this point, the manged index is looping into the `shrink` step as the action is trying to shrink to a higher number of
        // shards than the source index has. we'll update the policy to shrink to 1 shard and this should unblock the policy and it
        // should successfully shrink the managed index to the second cycle shrink index name
        {
            Phase warmPhase = new Phase("warm", TimeValue.ZERO, Map.of(MigrateAction.NAME, new MigrateAction(false), ShrinkAction.NAME,
                new ShrinkAction(1, null)));
            LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, Map.of("warm", warmPhase));
            PutLifecycleAction.Request putLifecycleRequest = new PutLifecycleAction.Request(lifecyclePolicy);
            assertAcked(client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get());
        }

        assertBusy(() -> assertTrue(indexExists(secondCycleShrinkIndexName[0])), 30, TimeUnit.SECONDS);
        assertBusy(() -> {
            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(secondCycleShrinkIndexName[0]);
            ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE,
                explainRequest).get();
            IndexLifecycleExplainResponse indexLifecycleExplainResponse = explainResponse.getIndexResponses()
                .get(secondCycleShrinkIndexName[0]);
            assertThat(indexLifecycleExplainResponse.getStep(), equalTo(PhaseCompleteStep.NAME));
        }, 30, TimeUnit.SECONDS);
    }
}
