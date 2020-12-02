/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.ccs;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class CrossClusterSearchIT extends AbstractMultiClustersTestCase {

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of("cluster_a");
    }

    private int indexDocs(Client client, String index) {
        int numDocs = between(1, 10);
        for (int i = 0; i < numDocs; i++) {
            client.prepareIndex(index).setSource("f", "v").get();
        }
        client.admin().indices().prepareRefresh(index).get();
        return numDocs;
    }

    public void testRemoteClusterClientRole() throws Exception {
        assertAcked(client(LOCAL_CLUSTER).admin().indices().prepareCreate("demo"));
        final int demoDocs = indexDocs(client(LOCAL_CLUSTER), "demo");
        assertAcked(client("cluster_a").admin().indices().prepareCreate("prod"));
        final int prodDocs = indexDocs(client("cluster_a"), "prod");
        final InternalTestCluster localCluster = cluster(LOCAL_CLUSTER);
        final String pureDataNode = randomBoolean() ? localCluster.startDataOnlyNode() : null;
        final String nodeWithoutRemoteClusterClientRole = localCluster.startNode(NodeRoles.onlyRole(DiscoveryNodeRole.DATA_ROLE));
        ElasticsearchAssertions.assertFutureThrows(
            localCluster.client(nodeWithoutRemoteClusterClientRole)
                .prepareSearch("demo", "cluster_a:prod")
                .setQuery(new MatchAllQueryBuilder())
                .setAllowPartialSearchResults(false)
                .setSize(1000)
                .execute(),
            IllegalArgumentException.class,
            RestStatus.BAD_REQUEST,
            "node [" + nodeWithoutRemoteClusterClientRole + "] does not have the remote cluster client role enabled"
        );

        final String nodeWithRemoteClusterClientRole = randomFrom(
            StreamSupport.stream(localCluster.clusterService().state().nodes().spliterator(), false)
                .map(DiscoveryNode::getName)
                .filter(nodeName -> nodeWithoutRemoteClusterClientRole.equals(nodeName) == false)
                .filter(nodeName -> nodeName.equals(pureDataNode) == false)
                .collect(Collectors.toList()));

        final SearchResponse resp = localCluster.client(nodeWithRemoteClusterClientRole)
            .prepareSearch("demo", "cluster_a:prod")
            .setQuery(new MatchAllQueryBuilder())
            .setAllowPartialSearchResults(false)
            .setSize(1000)
            .get();
        assertHitCount(resp, demoDocs + prodDocs);
    }

    public void testProxyConnectionDisconnect() throws Exception {
        assertAcked(client(LOCAL_CLUSTER).admin().indices().prepareCreate("demo"));
        indexDocs(client(LOCAL_CLUSTER), "demo");
        final String remoteNode = cluster("cluster_a").startDataOnlyNode();
        assertAcked(client("cluster_a").admin().indices().prepareCreate("prod")
            .setSettings(Settings.builder().put("index.routing.allocation.require._name", remoteNode)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()));
        indexDocs(client("cluster_a"), "prod");
        SearchListenerPlugin.blockQueryPhase();
        try {
            PlainActionFuture<SearchResponse> future = new PlainActionFuture<>();
            SearchRequest searchRequest = new SearchRequest("demo", "cluster_a:prod");
            searchRequest.allowPartialSearchResults(false);
            searchRequest.setCcsMinimizeRoundtrips(false);
            searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(1000));
            client(LOCAL_CLUSTER).search(searchRequest, future);
            SearchListenerPlugin.waitSearchStarted();
            disconnectFromRemoteClusters();
            assertBusy(() -> assertTrue(future.isDone()));
            configureAndConnectsToRemoteClusters();
        } finally {
            SearchListenerPlugin.allowQueryPhase();
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        if (clusterAlias.equals(LOCAL_CLUSTER)) {
            return super.nodePlugins(clusterAlias);
        } else {
            return CollectionUtils.appendToCopy(super.nodePlugins(clusterAlias), SearchListenerPlugin.class);
        }
    }

    @Before
    public void resetSearchListenerPlugin() throws Exception {
        SearchListenerPlugin.reset();
    }

    public static class SearchListenerPlugin extends Plugin {
        private static final AtomicReference<CountDownLatch> startedLatch = new AtomicReference<>();
        private static final AtomicReference<CountDownLatch> queryLatch = new AtomicReference<>();

        static void reset() {
            startedLatch.set(new CountDownLatch(1));
        }

        static void blockQueryPhase() {
            queryLatch.set(new CountDownLatch(1));
        }

        static void allowQueryPhase() {
            final CountDownLatch latch = queryLatch.get();
            if (latch != null) {
                latch.countDown();
            }
        }

        static void waitSearchStarted() throws InterruptedException {
            assertTrue(startedLatch.get().await(60, TimeUnit.SECONDS));
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addSearchOperationListener(new SearchOperationListener() {
                @Override
                public void onPreQueryPhase(SearchContext searchContext) {
                    startedLatch.get().countDown();
                    final CountDownLatch latch = queryLatch.get();
                    if (latch != null) {
                        try {
                            assertTrue(latch.await(60, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                    }
                }
            });
            super.onIndexModule(indexModule);
        }
    }
}
