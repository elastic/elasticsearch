/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.existence;

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.blobcache.BlobCachePlugin;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.xpack.autoscaling.AbstractFrozenAutoscalingIntegTestCase;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.core.ilm.ExplainLifecycleRequest;
import org.elasticsearch.xpack.core.ilm.ExplainLifecycleResponse;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleExplainResponse;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction;
import org.elasticsearch.xpack.core.ilm.WaitForDataTierStep;
import org.elasticsearch.xpack.core.ilm.action.ExplainLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.ILMActions;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleRequest;
import org.elasticsearch.xpack.slm.SnapshotLifecycle;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class FrozenExistenceDeciderIT extends AbstractFrozenAutoscalingIntegTestCase {

    private static final String INDEX_NAME = "index";
    private static final String PARTIAL_INDEX_NAME = "partial-index";

    @Override
    protected String deciderName() {
        return FrozenExistenceDeciderService.NAME;
    }

    @Override
    protected Settings.Builder addDeciderSettings(Settings.Builder builder) {
        return builder;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        settings.put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL, "1s");
        settings.put(LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED, false);
        settings.put(LifecycleSettings.SLM_HISTORY_INDEX_ENABLED_SETTING.getKey(), false);
        return settings.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            BlobCachePlugin.class,
            LocalStateAutoscalingAndSearchableSnapshotsAndIndexLifecycle.class,
            SnapshotLifecycle.class,
            Ccr.class
        );
    }

    public void testZeroToOne() throws Exception {
        internalCluster().startMasterOnlyNode();
        setupRepoAndPolicy();
        logger.info("starting 2 content data nodes");
        internalCluster().startNode(NodeRoles.onlyRole(DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE));
        internalCluster().startNode(NodeRoles.onlyRole(DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE));
        // create an ignored snapshot to initialize the latest-N file.
        createFullSnapshot(fsRepoName, snapshotName);

        Phase hotPhase = new Phase("hot", TimeValue.ZERO, Collections.emptyMap());
        Phase frozenPhase = new Phase(
            "frozen",
            TimeValue.ZERO,
            singletonMap(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(fsRepoName, randomBoolean()))
        );
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy("policy", Map.of("hot", hotPhase, "frozen", frozenPhase));
        PutLifecycleRequest putLifecycleRequest = new PutLifecycleRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, lifecyclePolicy);
        assertAcked(client().execute(ILMActions.PUT, putLifecycleRequest).get());

        Settings settings = Settings.builder()
            .put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(LifecycleSettings.LIFECYCLE_NAME, "policy")
            .build();
        CreateIndexResponse res = indicesAdmin().prepareCreate(INDEX_NAME).setSettings(settings).get();
        assertTrue(res.isAcknowledged());
        logger.info("-> created index");

        assertBusy(() -> assertMinimumCapacity(capacity().results().get("frozen").requiredCapacity().total()));
        assertMinimumCapacity(capacity().results().get("frozen").requiredCapacity().node());

        assertThat(
            clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).get().getStatus(),
            anyOf(equalTo(ClusterHealthStatus.YELLOW), equalTo(ClusterHealthStatus.GREEN))
        );

        assertBusy(() -> {
            ExplainLifecycleResponse response = client().execute(
                ExplainLifecycleAction.INSTANCE,
                new ExplainLifecycleRequest(TEST_REQUEST_TIMEOUT).indices(INDEX_NAME)
            ).actionGet();
            IndexLifecycleExplainResponse indexResponse = response.getIndexResponses().get(INDEX_NAME);
            assertNotNull(indexResponse);
            assertThat(indexResponse.getStep(), equalTo(WaitForDataTierStep.NAME));
        });

        // verify that SearchableSnapshotAction uses WaitForDataTierStep and that it waits.
        assertThat(indices(), not(arrayContaining(PARTIAL_INDEX_NAME)));

        logger.info("-> starting dedicated frozen node");
        internalCluster().startNode(NodeRoles.onlyRole(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE));

        // we've seen a case where bootstrapping a node took just over 60 seconds in the test environment, so using an (excessive) 90
        // seconds max wait time to avoid flakiness
        assertBusy(() -> {
            // cause a bit of cluster activity using an empty reroute call in case the `wait-for-index-colour` ILM step missed the
            // notification that partial-index is now GREEN.
            ClusterRerouteUtils.reroute(client());
            String[] indices = indices();
            assertThat(indices, arrayContaining(PARTIAL_INDEX_NAME));
            assertThat(indices, not(arrayContaining(INDEX_NAME)));
        }, 90, TimeUnit.SECONDS);
        ensureGreen();
    }

    private String[] indices() {
        return indicesAdmin().prepareGetIndex(TEST_REQUEST_TIMEOUT).addIndices("index").get().indices();
    }

    private void assertMinimumCapacity(AutoscalingCapacity.AutoscalingResources resources) {
        assertThat(resources.memory(), equalTo(FrozenExistenceDeciderService.MINIMUM_FROZEN_MEMORY));
        assertThat(resources.storage(), equalTo(FrozenExistenceDeciderService.MINIMUM_FROZEN_STORAGE));
    }
}
