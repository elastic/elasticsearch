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
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ilm.DataTierMigrationRoutedStep;
import org.elasticsearch.xpack.core.ilm.ExplainLifecycleRequest;
import org.elasticsearch.xpack.core.ilm.ExplainLifecycleResponse;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleExplainResponse;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.action.ExplainLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.NodeRoles.onlyRole;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DataTiersMigrationsTests extends ESIntegTestCase {

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

    public static Settings hotNode(final Settings settings) {
        return onlyRole(settings, DiscoveryNodeRole.DATA_HOT_NODE_ROLE);
    }

    public static Settings warmNode(final Settings settings) {
        return onlyRole(settings, DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
    }

    public static Settings coldNode(final Settings settings) {
        return onlyRole(settings, DiscoveryNodeRole.DATA_COLD_NODE_ROLE);
    }

    public void testIndexDataTierMigration() throws Exception {
        internalCluster().startMasterOnlyNodes(1, Settings.EMPTY);
        logger.info("starting 2 hot data nodes");
        internalCluster().startNode(hotNode(Settings.EMPTY));
        internalCluster().startNode(hotNode(Settings.EMPTY));

        // it's important we start one node of each tear as otherwise all phases will be allocated on the 2 available hot nodes (as our
        // tier preference configuration will not detect any available warm/cold tier node and will fallback to the available hot tier)
        // we want ILM to stop in the check-migration step in the warm and cold phase so we can unblock it manually by starting another
        // node in the corresponding tier (so that the index replica is allocated)
        logger.info("starting a warm data node");
        internalCluster().startNode(warmNode(Settings.EMPTY));

        logger.info("starting a cold data node");
        internalCluster().startNode(coldNode(Settings.EMPTY));

        Phase hotPhase = new Phase("hot", TimeValue.ZERO, Collections.emptyMap());
        Phase warmPhase = new Phase("warm", TimeValue.ZERO, Collections.emptyMap());
        Phase coldPhase = new Phase("cold", TimeValue.ZERO, Collections.emptyMap());
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, Map.of("hot", hotPhase, "warm", warmPhase, "cold", coldPhase));
        PutLifecycleAction.Request putLifecycleRequest = new PutLifecycleAction.Request(lifecyclePolicy);
        assertAcked(client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get());

        Settings settings = Settings.builder().put(indexSettings()).put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1).put(LifecycleSettings.LIFECYCLE_NAME, policy).build();
        CreateIndexResponse res = client().admin().indices().prepareCreate(managedIndex).setSettings(settings).get();
        assertTrue(res.isAcknowledged());

        assertBusy(() -> {
            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(managedIndex);
            ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE,
                explainRequest).get();

            IndexLifecycleExplainResponse indexLifecycleExplainResponse = explainResponse.getIndexResponses().get(managedIndex);
            assertThat(indexLifecycleExplainResponse.getPhase(), is("warm"));
            assertThat(indexLifecycleExplainResponse.getStep(), is(DataTierMigrationRoutedStep.NAME));
        }, 30, TimeUnit.SECONDS);

        logger.info("starting a warm data node");
        internalCluster().startNode(warmNode(Settings.EMPTY));
        assertBusy(() -> {
            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(managedIndex);
            ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE,
                explainRequest).get();

            IndexLifecycleExplainResponse indexLifecycleExplainResponse = explainResponse.getIndexResponses().get(managedIndex);
            assertThat(indexLifecycleExplainResponse.getPhase(), is("cold"));
            assertThat(indexLifecycleExplainResponse.getStep(), is(DataTierMigrationRoutedStep.NAME));
        }, 30, TimeUnit.SECONDS);

        logger.info("starting a cold data node");
        internalCluster().startNode(coldNode(Settings.EMPTY));

        // wait for lifecycle to complete in the cold phase after the index has been migrated to the cold node
        assertBusy(() -> {
            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(managedIndex);
            ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE,
                explainRequest).get();

            IndexLifecycleExplainResponse indexLifecycleExplainResponse = explainResponse.getIndexResponses().get(managedIndex);
            assertThat(indexLifecycleExplainResponse.getPhase(), is("cold"));
            assertThat(indexLifecycleExplainResponse.getStep(), is("complete"));
        }, 30, TimeUnit.SECONDS);
    }

    public void testUserOptsOutOfTierMigration() throws Exception {
        internalCluster().startMasterOnlyNodes(1, Settings.EMPTY);
        logger.info("starting a hot data node");
        internalCluster().startNode(hotNode(Settings.EMPTY));

        logger.info("starting a warm data node");
        internalCluster().startNode(warmNode(Settings.EMPTY));

        logger.info("starting a cold data node");
        internalCluster().startNode(coldNode(Settings.EMPTY));

        Phase hotPhase = new Phase("hot", TimeValue.ZERO, Collections.emptyMap());
        Phase warmPhase = new Phase("warm", TimeValue.ZERO, Collections.emptyMap());
        Phase coldPhase = new Phase("cold", TimeValue.ZERO, Collections.emptyMap());
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, Map.of("hot", hotPhase, "warm", warmPhase, "cold", coldPhase));
        PutLifecycleAction.Request putLifecycleRequest = new PutLifecycleAction.Request(lifecyclePolicy);
        assertAcked(client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get());

        Settings settings = Settings.builder().put(indexSettings()).put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1).put(LifecycleSettings.LIFECYCLE_NAME, policy).build();
        CreateIndexResponse res = client().admin().indices().prepareCreate(managedIndex).setSettings(settings).get();
        assertTrue(res.isAcknowledged());

        assertBusy(() -> {
            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(managedIndex);
            ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE,
                explainRequest).get();

            IndexLifecycleExplainResponse indexLifecycleExplainResponse = explainResponse.getIndexResponses().get(managedIndex);
            assertThat(indexLifecycleExplainResponse.getPhase(), is("warm"));
            assertThat(indexLifecycleExplainResponse.getStep(), is(DataTierMigrationRoutedStep.NAME));
            assertReplicaIsUnassigned();
        }, 30, TimeUnit.SECONDS);

        Settings removeTierRoutingSetting = Settings.builder().putNull(DataTierAllocationDecider.INDEX_ROUTING_PREFER).build();
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(managedIndex).settings(removeTierRoutingSetting);
        assertAcked(client().admin().indices().updateSettings(updateSettingsRequest).actionGet());

        // the index should successfully allocate on any nodes
        ensureGreen(managedIndex);

        // the index is successfully allocated but the migrate action from the cold phase re-configured the tier migration setting to the
        // cold tier so ILM is stuck in `check-migration` in the cold phase this time
        // we have 2 options to resume the ILM execution:
        //  1. start another cold node so both the primary and replica can relocate to the cold nodes
        //  2. remove the tier routing setting from the index again (we're doing this below)
        assertBusy(() -> {
            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(managedIndex);
            ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE,
                explainRequest).get();

            IndexLifecycleExplainResponse indexLifecycleExplainResponse = explainResponse.getIndexResponses().get(managedIndex);
            assertThat(indexLifecycleExplainResponse.getPhase(), is("cold"));
            assertThat(indexLifecycleExplainResponse.getStep(), is(DataTierMigrationRoutedStep.NAME));
        }, 30, TimeUnit.SECONDS);

        // remove the tier routing setting again
        assertAcked(client().admin().indices().updateSettings(updateSettingsRequest).actionGet());

        // wait for lifecycle to complete in the cold phase
        assertBusy(() -> {
            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(managedIndex);
            ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE,
                explainRequest).get();

            IndexLifecycleExplainResponse indexLifecycleExplainResponse = explainResponse.getIndexResponses().get(managedIndex);
            assertThat(indexLifecycleExplainResponse.getPhase(), is("cold"));
            assertThat(indexLifecycleExplainResponse.getStep(), is("complete"));
        }, 30, TimeUnit.SECONDS);
    }

    private void assertReplicaIsUnassigned() {
        ClusterAllocationExplainRequest explainReplicaShard =
            new ClusterAllocationExplainRequest().setIndex(managedIndex).setPrimary(false).setShard(0);
        ClusterAllocationExplainResponse response = client().admin().cluster().allocationExplain(explainReplicaShard).actionGet();
        assertThat(response.getExplanation().getShardState(), is(ShardRoutingState.UNASSIGNED));
    }
}
