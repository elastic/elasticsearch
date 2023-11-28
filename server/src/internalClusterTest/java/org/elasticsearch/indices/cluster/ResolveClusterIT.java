/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.cluster;

import org.elasticsearch.Build;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.resolve.ResolveClusterAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class ResolveClusterIT extends AbstractMultiClustersTestCase {

    private static final String REMOTE_CLUSTER = "cluster_a";
    private static long EARLIEST_TIMESTAMP = 1691348810000L;
    private static long LATEST_TIMESTAMP = 1691348820000L;

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER, randomBoolean());
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    public void testClusterResolveWithMatchingIndices() throws Exception {
        Map<String, Object> testClusterInfo = setupThreeClusters(); // TODO: needs params for datastreams and aliases
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");
        boolean skipUnavailable = (Boolean) testClusterInfo.get("remote.skip_unavailable");

        String[] indexExpressions = new String[] { localIndex, REMOTE_CLUSTER + ":" + remoteIndex };
        ResolveClusterAction.Request request = new ResolveClusterAction.Request(indexExpressions);

        ActionFuture<ResolveClusterAction.Response> future = client(LOCAL_CLUSTER).admin().indices().resolveCluster(request);
        ResolveClusterAction.Response response = future.actionGet(5000, TimeUnit.MILLISECONDS);
        assertNotNull(response);
        System.err.println(response);
        System.err.println("-------------------------------");
        System.err.println(response.getResolveClusterInfo());
        System.err.println("-------------------------------");
        System.err.println(Strings.toString(response));

        Map<String, ResolveClusterAction.ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
        assertEquals(2, clusterInfo.size());
        assertThat(clusterInfo.keySet(), equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER)));

        ResolveClusterAction.ResolveClusterInfo remote = clusterInfo.get(REMOTE_CLUSTER);
        assertThat(remote.isConnected(), equalTo(true));
        assertThat(remote.getSkipUnavailable(), equalTo(skipUnavailable));
        assertThat(remote.getMatchingIndices(), equalTo(true));
        Build remoteBuild = remote.getBuild();
        assertNotNull(remoteBuild.version());
        System.err.println(remoteBuild.version());
    }

    // TODO: change this to actually set up 3 clusters => starting with 2 to get the basics working
    private Map<String, Object> setupThreeClusters() {
        String localIndex = "demo";
        int numShardsLocal = randomIntBetween(2, 10);
        Settings localSettings = indexSettings(numShardsLocal, randomIntBetween(0, 1)).build();
        assertAcked(
            client(LOCAL_CLUSTER).admin()
                .indices()
                .prepareCreate(localIndex)
                .setSettings(localSettings)
                .setMapping("@timestamp", "type=date", "f", "type=text")
        );
        indexDocs(client(LOCAL_CLUSTER), localIndex);

        String remoteIndex = "prod";
        int numShardsRemote = randomIntBetween(2, 10);
        final InternalTestCluster remoteCluster = cluster(REMOTE_CLUSTER);
        remoteCluster.ensureAtLeastNumDataNodes(randomIntBetween(1, 3));
        assertAcked(
            client(REMOTE_CLUSTER).admin()
                .indices()
                .prepareCreate(remoteIndex)
                .setSettings(indexSettings(numShardsRemote, randomIntBetween(0, 1)))
                .setMapping("@timestamp", "type=date", "f", "type=text")
        );
        assertFalse(
            client(REMOTE_CLUSTER).admin()
                .cluster()
                .prepareHealth(remoteIndex)
                .setWaitForYellowStatus()
                .setTimeout(TimeValue.timeValueSeconds(10))
                .get()
                .isTimedOut()
        );
        indexDocs(client(REMOTE_CLUSTER), remoteIndex);

        String skipUnavailableKey = Strings.format("cluster.remote.%s.skip_unavailable", REMOTE_CLUSTER);
        Setting<?> skipUnavailableSetting = cluster(REMOTE_CLUSTER).clusterService().getClusterSettings().get(skipUnavailableKey);
        boolean skipUnavailable = (boolean) cluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).clusterService()
            .getClusterSettings()
            .get(skipUnavailableSetting);

        Map<String, Object> clusterInfo = new HashMap<>();
        clusterInfo.put("local.num_shards", numShardsLocal);
        clusterInfo.put("local.index", localIndex);
        clusterInfo.put("remote.num_shards", numShardsRemote);
        clusterInfo.put("remote.index", remoteIndex);
        clusterInfo.put("remote.skip_unavailable", skipUnavailable);
        return clusterInfo;
    }

    private int indexDocs(Client client, String index) {
        int numDocs = between(500, 1200);
        for (int i = 0; i < numDocs; i++) {
            long ts = EARLIEST_TIMESTAMP + i;
            if (i == numDocs - 1) {
                ts = LATEST_TIMESTAMP;
            }
            client.prepareIndex(index).setSource("f", "v", "@timestamp", ts).get();
        }
        client.admin().indices().prepareRefresh(index).get();
        return numDocs;
    }
}
