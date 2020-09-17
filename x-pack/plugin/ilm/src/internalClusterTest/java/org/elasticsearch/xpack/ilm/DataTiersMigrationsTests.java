/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.DataTier;
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
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal));
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
        return onlyRole(settings, DataTier.DATA_HOT_NODE_ROLE);
    }

    public static Settings warmNode(final Settings settings) {
        return onlyRole(settings, DataTier.DATA_WARM_NODE_ROLE);
    }

    public static Settings coldNode(final Settings settings) {
        return onlyRole(settings, DataTier.DATA_COLD_NODE_ROLE);
    }

    public void testIndexDataTierMigration() throws Exception {
        internalCluster().startMasterOnlyNodes(1, Settings.EMPTY);
        logger.info("starting hot data node");
        internalCluster().startNode(hotNode(Settings.EMPTY));

        Phase hotPhase = new Phase("hot", TimeValue.ZERO, Collections.emptyMap());
        Phase warmPhase = new Phase("warm", TimeValue.ZERO, Collections.emptyMap());
        Phase coldPhase = new Phase("cold", TimeValue.ZERO, Collections.emptyMap());
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, Map.of("hot", hotPhase, "warm", warmPhase, "cold", coldPhase));
        PutLifecycleAction.Request putLifecycleRequest = new PutLifecycleAction.Request(lifecyclePolicy);
        PutLifecycleAction.Response putLifecycleResponse = client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get();
        assertAcked(putLifecycleResponse);

        Settings settings = Settings.builder().put(indexSettings()).put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 0).put(LifecycleSettings.LIFECYCLE_NAME, policy).build();
        CreateIndexResponse res = client().admin().indices().prepareCreate(managedIndex).setSettings(settings).get();
        assertTrue(res.isAcknowledged());

        assertBusy(() -> {
            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(managedIndex);
            ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE,
                explainRequest).get();

            IndexLifecycleExplainResponse indexLifecycleExplainResponse = explainResponse.getIndexResponses().get(managedIndex);
            assertThat(indexLifecycleExplainResponse.getPhase(), is("warm"));
            assertThat(indexLifecycleExplainResponse.getStep(), is(DataTierMigrationRoutedStep.NAME));
        });

        logger.info("starting warm data node");
        internalCluster().startNode(warmNode(Settings.EMPTY));
        assertBusy(() -> {
            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(managedIndex);
            ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE,
                explainRequest).get();

            IndexLifecycleExplainResponse indexLifecycleExplainResponse = explainResponse.getIndexResponses().get(managedIndex);
            assertThat(indexLifecycleExplainResponse.getPhase(), is("cold"));
            assertThat(indexLifecycleExplainResponse.getStep(), is(DataTierMigrationRoutedStep.NAME));
        });

        logger.info("starting cold data node");
        internalCluster().startNode(coldNode(Settings.EMPTY));

        // wait for lifecycle to complete in the cold phase after the index has been migrated to the cold node
        assertBusy(() -> {
            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(managedIndex);
            ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE,
                explainRequest).get();

            IndexLifecycleExplainResponse indexLifecycleExplainResponse = explainResponse.getIndexResponses().get(managedIndex);
            assertThat(indexLifecycleExplainResponse.getPhase(), is("cold"));
            assertThat(indexLifecycleExplainResponse.getStep(), is("complete"));
        });
    }
}
