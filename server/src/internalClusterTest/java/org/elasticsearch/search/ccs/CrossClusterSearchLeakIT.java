/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.ccs;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

public class CrossClusterSearchLeakIT extends AbstractMultiClustersTestCase {

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of("cluster_a");
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    private int indexDocs(Client client, String field, String index) {
        int numDocs = between(1, 200);
        for (int i = 0; i < numDocs; i++) {
            client.prepareIndex(index).setSource(field, "v" + i).get();
        }
        client.admin().indices().prepareRefresh(index).get();
        return numDocs;
    }

    /**
     * This test validates that we do not leak any memory when running CCS in various modes, actual validation is done by test framework
     * (leak detection)
     * <ul>
     *     <li>proxy vs non-proxy</li>
     *     <li>single-phase query-fetch or multi-phase</li>
     *     <li>minimize roundtrip vs not</li>
     *     <li>scroll vs no scroll</li>
     * </ul>
     */
    public void testSearch() throws Exception {
        assertAcked(
            client(LOCAL_CLUSTER).admin()
                .indices()
                .prepareCreate("demo")
                .setMapping("f", "type=keyword")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 3)))
        );
        indexDocs(client(LOCAL_CLUSTER), "ignored", "demo");
        final InternalTestCluster remoteCluster = cluster("cluster_a");
        int minRemotes = between(2, 5);
        remoteCluster.ensureAtLeastNumDataNodes(minRemotes);
        List<String> remoteDataNodes = remoteCluster.clusterService()
            .state()
            .nodes()
            .stream()
            .filter(DiscoveryNode::canContainData)
            .map(DiscoveryNode::getName)
            .toList();
        assertThat(remoteDataNodes.size(), Matchers.greaterThanOrEqualTo(minRemotes));
        List<String> seedNodes = randomSubsetOf(between(1, remoteDataNodes.size() - 1), remoteDataNodes);
        disconnectFromRemoteClusters();
        configureRemoteCluster("cluster_a", seedNodes);
        final Settings.Builder allocationFilter = Settings.builder();
        if (rarely()) {
            allocationFilter.put("index.routing.allocation.include._name", String.join(",", seedNodes));
        } else {
            // Provoke using proxy connections
            allocationFilter.put("index.routing.allocation.exclude._name", String.join(",", seedNodes));
        }
        assertAcked(
            client("cluster_a").admin()
                .indices()
                .prepareCreate("prod")
                .setMapping("f", "type=keyword")
                .setSettings(
                    Settings.builder()
                        .put(allocationFilter.build())
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 3))
                )
        );
        assertFalse(
            client("cluster_a").admin()
                .cluster()
                .prepareHealth(TEST_REQUEST_TIMEOUT, "prod")
                .setWaitForYellowStatus()
                .setTimeout(TimeValue.timeValueSeconds(10))
                .get()
                .isTimedOut()
        );
        int docs = indexDocs(client("cluster_a"), "f", "prod");

        List<ActionFuture<SearchResponse>> futures = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            String[] indices = randomBoolean() ? new String[] { "demo", "cluster_a:prod" } : new String[] { "cluster_a:prod" };
            final SearchRequest searchRequest = new SearchRequest(indices);
            searchRequest.allowPartialSearchResults(false);
            boolean scroll = randomBoolean();
            searchRequest.source(
                new SearchSourceBuilder().query(new MatchAllQueryBuilder())
                    .aggregation(terms("f").field("f").size(docs + between(0, 10)))
                    .size(between(scroll ? 1 : 0, 1000))
            );
            if (scroll) {
                searchRequest.scroll(TimeValue.timeValueSeconds(30));
            }
            searchRequest.setCcsMinimizeRoundtrips(rarely());
            futures.add(client(LOCAL_CLUSTER).search(searchRequest));
        }

        for (ActionFuture<SearchResponse> future : futures) {
            assertResponse(future, response -> {
                if (response.getScrollId() != null) {
                    ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
                    clearScrollRequest.scrollIds(List.of(response.getScrollId()));
                    try {
                        client(LOCAL_CLUSTER).clearScroll(clearScrollRequest).get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                Terms terms = response.getAggregations().get("f");
                assertThat(terms.getBuckets().size(), equalTo(docs));
                for (Terms.Bucket bucket : terms.getBuckets()) {
                    assertThat(bucket.getDocCount(), equalTo(1L));
                }
            });
        }
    }

    @Override
    protected void configureRemoteCluster(String clusterAlias, Collection<String> seedNodes) throws Exception {
        if (rarely()) {
            super.configureRemoteCluster(clusterAlias, seedNodes);
        } else {
            final Settings.Builder settings = Settings.builder();
            final String seedNode = randomFrom(seedNodes);
            final TransportService transportService = cluster(clusterAlias).getInstance(TransportService.class, seedNode);
            final String seedAddress = transportService.boundAddress().publishAddress().toString();

            settings.put("cluster.remote." + clusterAlias + ".mode", "proxy");
            settings.put("cluster.remote." + clusterAlias + ".proxy_address", seedAddress);
            client().admin()
                .cluster()
                .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .setPersistentSettings(settings)
                .get();
        }
    }
}
