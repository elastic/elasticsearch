/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;


import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.coordination.FollowersChecker;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class SearchServiceCleanupOnLostMasterIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockTransportService.TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), 1)
            .put(FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "100ms").build();
    }

    public void testMasterRestart() throws Exception {
        testLostMaster((master, dataNode) -> internalCluster().restartNode(master));
    }

    public void testDroppedOutNode() throws Exception {
        testLostMaster((master, dataNode) -> {
            final MockTransportService masterTransportService
                = (MockTransportService) internalCluster().getInstance(TransportService.class, master);
            final TransportService dataTransportService = internalCluster().getInstance(TransportService.class, dataNode);
            masterTransportService.addFailToSendNoConnectRule(dataTransportService, FollowersChecker.FOLLOWER_CHECK_ACTION_NAME);

            assertBusy(() -> {
                final ClusterHealthStatus indexHealthStatus = client(master).admin().cluster()
                    .health(Requests.clusterHealthRequest("test")).actionGet().getStatus();
                assertThat(indexHealthStatus, Matchers.is(ClusterHealthStatus.RED));
            });
            masterTransportService.clearAllRules();
        });
    }

    private void testLostMaster(CheckedBiConsumer<String, String, Exception> loseMaster) throws Exception {
        final String master = internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();

        index("test", "test", "{}");

        assertThat(client().prepareSearch("test").setScroll("30m").get().getScrollId(), is(notNullValue()));

        loseMaster.accept(master, dataNode);
        // in the past, this failed because the search context for the scroll would prevent the shard lock from being released.
        ensureYellow();
    }
}
