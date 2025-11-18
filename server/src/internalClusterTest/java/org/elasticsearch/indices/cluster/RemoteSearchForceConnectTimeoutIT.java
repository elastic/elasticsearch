/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.cluster;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class RemoteSearchForceConnectTimeoutIT extends AbstractMultiClustersTestCase {
    private static final String REMOTE_CLUSTER_1 = "cluster-a";

    public static class ForceConnectTimeoutPlugin extends Plugin implements ClusterPlugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(ForceConnectTimeoutSetting);
        }
    }

    private static final Setting<String> ForceConnectTimeoutSetting = Setting.simpleString(
        "search.ccs.force_connect_timeout",
        Setting.Property.NodeScope
    );

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER_1);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return CollectionUtils.appendToCopy(super.nodePlugins(clusterAlias), ForceConnectTimeoutPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        /*
         * This is the setting that controls how long TransportSearchAction will wait for establishing a connection
         * with a remote. At present, we set it to low 1s to prevent stalling the test for too long -- this is consistent
         * with what we've done in other tests.
         */
        return Settings.builder().put(super.nodeSettings()).put("search.ccs.force_connect_timeout", "1s").build();
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_1, true);
    }

    public void testTimeoutSetting() {
        var latch = new CountDownLatch(1);
        for (String localNodeName : cluster(LOCAL_CLUSTER).getNodeNames()) {
            MockTransportService localMts = (MockTransportService) cluster(LOCAL_CLUSTER).getInstance(
                TransportService.class,
                localNodeName
            );
            for (String remoteNodeName : cluster(REMOTE_CLUSTER_1).getNodeNames()) {
                TransportService remoteTs = cluster(REMOTE_CLUSTER_1).getInstance(TransportService.class, remoteNodeName);
                localMts.addConnectBehavior(remoteTs, ((transport, discoveryNode, profile, listener) -> {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }

                    transport.openConnection(discoveryNode, profile, listener);
                }));
            }
        }

        // Add some dummy data to prove we are communicating fine with the remote.
        assertAcked(client(REMOTE_CLUSTER_1).admin().indices().prepareCreate("test-index"));
        client(REMOTE_CLUSTER_1).prepareIndex("test-index").setSource("sample-field", "sample-value").get();
        client(REMOTE_CLUSTER_1).admin().indices().prepareRefresh("test-index").get();

        /*
         * Do a full restart so that our custom connect behaviour takes effect since it does not apply to
         * pre-existing connections -- they're already established by the time this test runs.
         */
        try {
            cluster(REMOTE_CLUSTER_1).fullRestart();
        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            var searchRequest = new SearchRequest("*", "*:*");
            searchRequest.allowPartialSearchResults(false);
            var result = safeGet(client().execute(TransportSearchAction.TYPE, searchRequest));

            // The remote cluster should've failed.
            var failures = result.getClusters().getCluster(REMOTE_CLUSTER_1).getFailures();
            assertThat(failures.size(), Matchers.equalTo(1));

            /*
             * Reason should be a timed out exception. The timeout should be equal to what we've set and there should
             * be a reference to the subscribable listener -- which is what we use to listen for a valid connection.
             */
            var failureReason = failures.getFirst().reason();
            assertThat(
                failureReason,
                Matchers.containsString("org.elasticsearch.ElasticsearchTimeoutException: timed out after [1s/1000ms]")
            );
            assertThat(failureReason, Matchers.containsString("SubscribableListener"));
            latch.countDown();
            result.decRef();
        }
    }
}
