/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Assert;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.oneOf;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class ClusterStatsRemoteIT extends AbstractMultiClustersTestCase {
    private static final String REMOTE1 = "cluster-a";
    private static final String REMOTE2 = "cluster-b";

    private static final String INDEX_NAME = "demo";

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of(REMOTE1, REMOTE2);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE1, false, REMOTE2, true);
    }

    public void testRemoteClusterStats() throws ExecutionException, InterruptedException {
        setupClusters();
        final Client client = client(LOCAL_CLUSTER);
        SearchRequest searchRequest = new SearchRequest("*", "*:*");
        searchRequest.allowPartialSearchResults(false);
        searchRequest.setCcsMinimizeRoundtrips(randomBoolean());
        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10));

        // do a search
        assertResponse(cluster(LOCAL_CLUSTER).client().search(searchRequest), Assert::assertNotNull);
        // collect stats without remotes
        ClusterStatsResponse response = client.admin().cluster().prepareClusterStats().get();
        assertNotNull(response.getCcsMetrics());
        var remotesUsage = response.getCcsMetrics().getByRemoteCluster();
        assertThat(remotesUsage.size(), equalTo(3));
        assertNull(response.getRemoteClustersStats());
        // collect stats with remotes
        response = client.admin().cluster().execute(TransportClusterStatsAction.TYPE, new ClusterStatsRequest(true)).get();
        assertNotNull(response.getCcsMetrics());
        remotesUsage = response.getCcsMetrics().getByRemoteCluster();
        assertThat(remotesUsage.size(), equalTo(3));
        assertNotNull(response.getRemoteClustersStats());
        var remoteStats = response.getRemoteClustersStats();
        assertThat(remoteStats.size(), equalTo(2));
        for (String clusterAlias : remoteClusterAlias()) {
            assertThat(remoteStats, hasKey(clusterAlias));
            assertThat(remotesUsage, hasKey(clusterAlias));
            assertThat(remoteStats.get(clusterAlias).status(), equalToIgnoringCase(ClusterHealthStatus.GREEN.name()));
            assertThat(remoteStats.get(clusterAlias).indicesCount(), greaterThan(0L));
            assertThat(remoteStats.get(clusterAlias).nodesCount(), greaterThan(0L));
            assertThat(remoteStats.get(clusterAlias).shardsCount(), greaterThan(0L));
            assertThat(remoteStats.get(clusterAlias).heapBytes(), greaterThan(0L));
            assertThat(remoteStats.get(clusterAlias).memBytes(), greaterThan(0L));
            assertThat(remoteStats.get(clusterAlias).indicesBytes(), greaterThan(0L));
            assertThat(remoteStats.get(clusterAlias).versions(), hasItem(Version.CURRENT.toString()));
            assertThat(remoteStats.get(clusterAlias).clusterUUID(), not(equalTo("")));
            assertThat(remoteStats.get(clusterAlias).mode(), oneOf("sniff", "proxy"));
        }
        assertFalse(remoteStats.get(REMOTE1).skipUnavailable());
        assertTrue(remoteStats.get(REMOTE2).skipUnavailable());
    }

    private void setupClusters() {
        int numShardsLocal = randomIntBetween(2, 5);
        Settings localSettings = indexSettings(numShardsLocal, randomIntBetween(0, 1)).build();
        assertAcked(
            client(LOCAL_CLUSTER).admin()
                .indices()
                .prepareCreate(INDEX_NAME)
                .setSettings(localSettings)
                .setMapping("@timestamp", "type=date", "f", "type=text")
        );
        indexDocs(client(LOCAL_CLUSTER));

        int numShardsRemote = randomIntBetween(2, 10);
        for (String clusterAlias : remoteClusterAlias()) {
            final InternalTestCluster remoteCluster = cluster(clusterAlias);
            remoteCluster.ensureAtLeastNumDataNodes(randomIntBetween(2, 3));
            assertAcked(
                client(clusterAlias).admin()
                    .indices()
                    .prepareCreate(INDEX_NAME)
                    .setSettings(indexSettings(numShardsRemote, randomIntBetween(0, 1)))
                    .setMapping("@timestamp", "type=date", "f", "type=text")
            );
            assertFalse(
                client(clusterAlias).admin()
                    .cluster()
                    .prepareHealth(TEST_REQUEST_TIMEOUT, INDEX_NAME)
                    .setWaitForGreenStatus()
                    .setTimeout(TimeValue.timeValueSeconds(30))
                    .get()
                    .isTimedOut()
            );
            indexDocs(client(clusterAlias));
        }

    }

    private void indexDocs(Client client) {
        int numDocs = between(5, 20);
        for (int i = 0; i < numDocs; i++) {
            client.prepareIndex(INDEX_NAME).setSource("f", "v", "@timestamp", randomNonNegativeLong()).get();
        }
        client.admin().indices().prepareRefresh(INDEX_NAME).get();
    }

}
