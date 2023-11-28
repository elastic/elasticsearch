/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.cluster;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.resolve.ResolveClusterAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.Arrays;
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

    public void testClusterResolveWithIndices() {
        Map<String, Object> testClusterInfo = setupTwoClusters(false); // TODO: needs params for datastreams and aliases
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        boolean skipUnavailable = (Boolean) testClusterInfo.get("remote.skip_unavailable");

        // all clusters and both have matching indices
        {
            String[] indexExpressions = new String[] { localIndex, REMOTE_CLUSTER + ":" + remoteIndex };
            ResolveClusterAction.Request request = new ResolveClusterAction.Request(indexExpressions);

            ActionFuture<ResolveClusterAction.Response> future = client(LOCAL_CLUSTER).admin().indices().resolveCluster(request);
            ResolveClusterAction.Response response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterAction.ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            System.err.println(Strings.toString(response));  // FIXME - remove
            assertEquals(2, clusterInfo.size());
            assertThat(clusterInfo.keySet(), equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER)));

            ResolveClusterAction.ResolveClusterInfo remote = clusterInfo.get(REMOTE_CLUSTER);
            assertThat(remote.isConnected(), equalTo(true));
            assertThat(remote.getSkipUnavailable(), equalTo(skipUnavailable));
            assertThat(remote.getMatchingIndices(), equalTo(true));
            assertNotNull(remote.getBuild().version());

            ResolveClusterAction.ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertNull(local.getSkipUnavailable());
            assertThat(local.getMatchingIndices(), equalTo(true));
            assertNotNull(local.getBuild().version());
        }

        // only remote cluster has matching indices
        {
            String[] indexExpressions = new String[] { "foo*", REMOTE_CLUSTER + ":" + remoteIndex };
            ResolveClusterAction.Request request = new ResolveClusterAction.Request(indexExpressions);

            ActionFuture<ResolveClusterAction.Response> future = client(LOCAL_CLUSTER).admin().indices().resolveCluster(request);
            ResolveClusterAction.Response response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterAction.ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            System.err.println(Strings.toString(response));  // FIXME - remove
            assertEquals(2, clusterInfo.size());
            assertThat(clusterInfo.keySet(), equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER)));

            ResolveClusterAction.ResolveClusterInfo remote = clusterInfo.get(REMOTE_CLUSTER);
            assertThat(remote.isConnected(), equalTo(true));
            assertThat(remote.getSkipUnavailable(), equalTo(skipUnavailable));
            assertThat(remote.getMatchingIndices(), equalTo(true));
            assertNotNull(remote.getBuild().version());

            ResolveClusterAction.ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertNull(local.getSkipUnavailable());
            assertThat(local.getMatchingIndices(), equalTo(false));
            assertNotNull(local.getBuild().version());
        }

        // only local cluster has matching indices
        {
            String[] indexExpressions = new String[] { localIndex, REMOTE_CLUSTER + ":" + localIndex };
            ResolveClusterAction.Request request = new ResolveClusterAction.Request(indexExpressions);

            ActionFuture<ResolveClusterAction.Response> future = client(LOCAL_CLUSTER).admin().indices().resolveCluster(request);
            ResolveClusterAction.Response response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterAction.ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            System.err.println(Strings.toString(response));  // FIXME - remove
            assertEquals(2, clusterInfo.size());
            assertThat(clusterInfo.keySet(), equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER)));

            ResolveClusterAction.ResolveClusterInfo remote = clusterInfo.get(REMOTE_CLUSTER);
            assertThat(remote.isConnected(), equalTo(true));
            assertThat(remote.getSkipUnavailable(), equalTo(skipUnavailable));
            assertThat(remote.getMatchingIndices(), equalTo(false));
            assertNotNull(remote.getBuild().version());

            ResolveClusterAction.ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertNull(local.getSkipUnavailable());
            assertThat(local.getMatchingIndices(), equalTo(true));
            assertNotNull(local.getBuild().version());
        }

        // test with wildcard expressions in the index - all clusters should match
        {
            String[] indexExpressions = new String[] {
                localIndex.substring(0, 2) + "*",
                REMOTE_CLUSTER + ":" + remoteIndex.substring(0, 4) + "*" };
            ResolveClusterAction.Request request = new ResolveClusterAction.Request(indexExpressions);

            ActionFuture<ResolveClusterAction.Response> future = client(LOCAL_CLUSTER).admin().indices().resolveCluster(request);
            ResolveClusterAction.Response response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterAction.ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            System.err.println(Strings.toString(response));  // FIXME - remove
            assertEquals(2, clusterInfo.size());
            assertThat(clusterInfo.keySet(), equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER)));

            ResolveClusterAction.ResolveClusterInfo remote = clusterInfo.get(REMOTE_CLUSTER);
            assertThat(remote.isConnected(), equalTo(true));
            assertThat(remote.getSkipUnavailable(), equalTo(skipUnavailable));
            assertThat(remote.getMatchingIndices(), equalTo(true));
            assertNotNull(remote.getBuild().version());

            ResolveClusterAction.ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertNull(local.getSkipUnavailable());
            assertThat(local.getMatchingIndices(), equalTo(true));
            assertNotNull(local.getBuild().version());
        }

        // test with wildcard expressions in the cluster and index - all clusters should match
        {
            String[] indexExpressions = new String[] {
                localIndex.substring(0, 2) + "*",
                REMOTE_CLUSTER.substring(0, 4) + "*:" + remoteIndex.substring(0, 3) + "*" };
            System.err.println(Arrays.toString(indexExpressions)); // FIXME - remove
            ResolveClusterAction.Request request = new ResolveClusterAction.Request(indexExpressions);

            ActionFuture<ResolveClusterAction.Response> future = client(LOCAL_CLUSTER).admin().indices().resolveCluster(request);
            ResolveClusterAction.Response response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterAction.ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            System.err.println(Strings.toString(response));  // FIXME - remove
            assertEquals(2, clusterInfo.size());
            assertThat(clusterInfo.keySet(), equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER)));

            ResolveClusterAction.ResolveClusterInfo remote = clusterInfo.get(REMOTE_CLUSTER);
            assertThat(remote.isConnected(), equalTo(true));
            assertThat(remote.getSkipUnavailable(), equalTo(skipUnavailable));
            assertThat(remote.getMatchingIndices(), equalTo(true));
            assertNotNull(remote.getBuild().version());

            ResolveClusterAction.ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertNull(local.getSkipUnavailable());
            assertThat(local.getMatchingIndices(), equalTo(true));
            assertNotNull(local.getBuild().version());
        }

        // local cluster not included
        {
            String[] indexExpressions = new String[] { REMOTE_CLUSTER + ":*" };
            System.err.println(Arrays.toString(indexExpressions)); // FIXME - remove
            ResolveClusterAction.Request request = new ResolveClusterAction.Request(indexExpressions);

            ActionFuture<ResolveClusterAction.Response> future = client(LOCAL_CLUSTER).admin().indices().resolveCluster(request);
            ResolveClusterAction.Response response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterAction.ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            System.err.println(Strings.toString(response));  // FIXME - remove
            assertEquals(1, clusterInfo.size());
            assertThat(clusterInfo.keySet(), equalTo(Set.of(REMOTE_CLUSTER)));

            ResolveClusterAction.ResolveClusterInfo remote = clusterInfo.get(REMOTE_CLUSTER);
            assertThat(remote.isConnected(), equalTo(true));
            assertThat(remote.getSkipUnavailable(), equalTo(skipUnavailable));
            assertThat(remote.getMatchingIndices(), equalTo(true));
            assertNotNull(remote.getBuild().version());
        }
    }

    public void testClusterResolveWithMatchingAliases() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters(true); // TODO: needs params for datastreams and aliases
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        boolean skipUnavailable = (Boolean) testClusterInfo.get("remote.skip_unavailable");

        // TODO: LEFTOFF
    }

    private Map<String, Object> setupTwoClusters(boolean useAlias) {
        String localAlias = randomAlphaOfLengthBetween(5, 25);
        String remoteAlias = randomAlphaOfLengthBetween(5, 25);

        String localIndex = "demo";
        int numShardsLocal = randomIntBetween(3, 6);
        Settings localSettings = indexSettings(numShardsLocal, 0).build();

        assertAcked(
            client(LOCAL_CLUSTER).admin()
                .indices()
                .prepareCreate(localIndex)
                .setSettings(localSettings)
                .setMapping("@timestamp", "type=date", "f", "type=text")
        );
        if (useAlias) {
            // local index alias
            IndicesAliasesRequest.AliasActions addAction = new IndicesAliasesRequest.AliasActions(
                IndicesAliasesRequest.AliasActions.Type.ADD
            ).index(localIndex).aliases(localAlias);
            IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest();
            aliasesAddRequest.addAliasAction(addAction);

            assertAcked(client(LOCAL_CLUSTER).admin().indices().aliases(aliasesAddRequest));
        }
        indexDocs(client(LOCAL_CLUSTER), localIndex);

        String remoteIndex = "prod";
        int numShardsRemote = randomIntBetween(3, 6);
        final InternalTestCluster remoteCluster = cluster(REMOTE_CLUSTER);
        remoteCluster.ensureAtLeastNumDataNodes(randomIntBetween(1, 3));
        final Settings.Builder remoteSettings = Settings.builder();
        remoteSettings.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShardsRemote);

        assertAcked(
            client(REMOTE_CLUSTER).admin()
                .indices()
                .prepareCreate(remoteIndex)
                .setSettings(Settings.builder().put(remoteSettings.build()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
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
        if (useAlias) {
            clusterInfo.put("local.alias", localAlias);
            clusterInfo.put("remote.alias", remoteAlias);
        }
        return clusterInfo;
    }

    private int indexDocs(Client client, String index) {
        int numDocs = between(50, 100);
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
