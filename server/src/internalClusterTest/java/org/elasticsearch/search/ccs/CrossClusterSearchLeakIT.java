/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.ccs;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class CrossClusterSearchLeakIT extends AbstractMultiClustersTestCase {
    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of("cluster_a");
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    private int indexDocs(Client client, String index) {
        int numDocs = between(1, 200);
        for (int i = 0; i < numDocs; i++) {
            client.prepareIndex(index).setSource("f", "v" + i).get();
        }
        client.admin().indices().prepareRefresh(index).get();
        return numDocs;
    }

    /**
     * This test mainly validates that we do not leak any memory when running CCS in various modes
     * <ul>
     *     <li>proxy vs non-proxy</li>
     *     <li>single-phase query-fetch or multi-phase</li>
     *     <li>minimize roundtrip vs not</li>
     *     <li>scroll vs no scroll</li>
     * </ul>
     */
    public void testSearch() throws Exception {
        assertAcked(client(LOCAL_CLUSTER).admin().indices().prepareCreate("demo")
            .setMapping("f", "type=keyword")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 3))));
        indexDocs(client(LOCAL_CLUSTER), "demo");
        final InternalTestCluster remoteCluster = cluster("cluster_a");
        remoteCluster.ensureAtLeastNumDataNodes(3);
        List<String> remoteDataNodes = StreamSupport.stream(remoteCluster.clusterService().state().nodes().spliterator(), false)
            .filter(DiscoveryNode::canContainData)
            .map(DiscoveryNode::getName)
            .collect(Collectors.toList());
        assertThat(remoteDataNodes.size(), Matchers.greaterThanOrEqualTo(3));
        List<String> seedNodes = randomSubsetOf(between(1, remoteDataNodes.size() - 1), remoteDataNodes);
        disconnectFromRemoteClusters();
        configureRemoteCluster("cluster_a", seedNodes);
        final Settings.Builder allocationFilter = Settings.builder();
        if (randomBoolean()) {
            // Using proxy connections
            allocationFilter.put("index.routing.allocation.exclude._name", String.join(",", seedNodes));
        } else {
            allocationFilter.put("index.routing.allocation.include._name", String.join(",", seedNodes));
        }
        assertAcked(client("cluster_a").admin().indices().prepareCreate("prod")
            .setMapping("f", "type=keyword")
            .setSettings(Settings.builder().put(allocationFilter.build()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 3))));
        assertFalse(client("cluster_a").admin().cluster().prepareHealth("prod")
            .setWaitForYellowStatus().setTimeout(TimeValue.timeValueSeconds(10)).get().isTimedOut());
        indexDocs(client("cluster_a"), "prod");

        String[] indices = randomBoolean() ? new String[] { "demo", "cluster_a:prod" } : new String[] { "cluster_a:prod" };
        final SearchRequest templateSearch = new SearchRequest(indices);
        templateSearch.allowPartialSearchResults(false);
        templateSearch.setCcsMinimizeRoundtrips(randomBoolean());
        templateSearch.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).aggregation(terms("f").field("f")).size(1000));

        List<ActionFuture<SearchResponse>> futures = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {

            final SearchRequest searchRequest = new SearchRequest(templateSearch);
            if (randomBoolean()) {
                searchRequest.scroll("30s");
            }
            searchRequest.setCcsMinimizeRoundtrips(randomBoolean());
            futures.add(client(LOCAL_CLUSTER).search(searchRequest));
        }

        for (ActionFuture<SearchResponse> future : futures) {
            SearchResponse searchResponse = future.get();
            if (searchResponse.getScrollId() != null) {
                ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
                clearScrollRequest.scrollIds(List.of(searchResponse.getScrollId()));
                client(LOCAL_CLUSTER).clearScroll(clearScrollRequest).get();
            }
        }
        futures.forEach(ActionFuture::actionGet);
    }
}
