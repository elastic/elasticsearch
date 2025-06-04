/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.cluster;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.resolve.ResolveClusterActionRequest;
import org.elasticsearch.action.admin.indices.resolve.ResolveClusterActionResponse;
import org.elasticsearch.action.admin.indices.resolve.ResolveClusterInfo;
import org.elasticsearch.action.admin.indices.resolve.TransportResolveClusterAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.transport.RemoteClusterAware;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests the ResolveClusterAction around all features except data streams
 * (since those are not reachable from this package/module).
 * ResolveClusterDataStreamIT is a sibling IT test that does additional testing
 * related to data streams.
 */
public class ResolveClusterIT extends AbstractMultiClustersTestCase {

    private static final String REMOTE_CLUSTER_1 = "remote1";
    private static final String REMOTE_CLUSTER_2 = "remote2";
    private static long EARLIEST_TIMESTAMP = 1691348810000L;
    private static long LATEST_TIMESTAMP = 1691348820000L;

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_1, randomBoolean(), REMOTE_CLUSTER_2, true);
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    public void testClusterResolveWithIndices() throws IOException {
        Map<String, Object> testClusterInfo = setupThreeClusters(false);
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex1 = (String) testClusterInfo.get("remote1.index");
        String remoteIndex2 = (String) testClusterInfo.get("remote2.index");
        boolean skipUnavailable1 = (Boolean) testClusterInfo.get("remote1.skip_unavailable");
        boolean skipUnavailable2 = true;

        // all clusters and both have matching indices
        {
            String[] indexExpressions = new String[] {
                localIndex,
                REMOTE_CLUSTER_1 + ":" + remoteIndex1,
                REMOTE_CLUSTER_2 + ":" + remoteIndex2 };
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(indexExpressions);

            ActionFuture<ResolveClusterActionResponse> future = client(LOCAL_CLUSTER).admin()
                .indices()
                .execute(TransportResolveClusterAction.TYPE, request);
            ResolveClusterActionResponse response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(3, clusterInfo.size());
            Set<String> expectedClusterNames = Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));

            ResolveClusterInfo remote1 = clusterInfo.get(REMOTE_CLUSTER_1);
            assertThat(remote1.isConnected(), equalTo(true));
            assertThat(remote1.getSkipUnavailable(), equalTo(skipUnavailable1));
            assertThat(remote1.getMatchingIndices(), equalTo(true));
            assertNotNull(remote1.getBuild().version());
            assertNull(remote1.getError());

            ResolveClusterInfo remote2 = clusterInfo.get(REMOTE_CLUSTER_2);
            assertThat(remote2.isConnected(), equalTo(true));
            assertThat(remote2.getSkipUnavailable(), equalTo(skipUnavailable2));
            assertThat(remote2.getMatchingIndices(), equalTo(true));
            assertNotNull(remote2.getBuild().version());
            assertNull(remote2.getError());

            ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertThat(local.getSkipUnavailable(), equalTo(false));
            assertThat(local.getMatchingIndices(), equalTo(true));
            assertNotNull(local.getBuild().version());
            assertNull(local.getError());
        }

        // include two indices (no wildcards), one exists and one does not -> should still show `matching_indices: true`
        {
            String[] indexExpressions = new String[] {
                localIndex,
                "doesnotexist",
                REMOTE_CLUSTER_1 + ":" + remoteIndex1,
                REMOTE_CLUSTER_2 + ":" + remoteIndex2 };
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(indexExpressions);

            ActionFuture<ResolveClusterActionResponse> future = client(LOCAL_CLUSTER).admin()
                .indices()
                .execute(TransportResolveClusterAction.TYPE, request);
            ResolveClusterActionResponse response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(3, clusterInfo.size());
            Set<String> expectedClusterNames = Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));

            ResolveClusterInfo remote1 = clusterInfo.get(REMOTE_CLUSTER_1);
            assertThat(remote1.isConnected(), equalTo(true));
            assertThat(remote1.getSkipUnavailable(), equalTo(skipUnavailable1));
            assertThat(remote1.getMatchingIndices(), equalTo(true));
            assertNotNull(remote1.getBuild().version());
            assertNull(remote1.getError());

            ResolveClusterInfo remote2 = clusterInfo.get(REMOTE_CLUSTER_2);
            assertThat(remote2.isConnected(), equalTo(true));
            assertThat(remote2.getSkipUnavailable(), equalTo(skipUnavailable2));
            assertThat(remote2.getMatchingIndices(), equalTo(true));
            assertNotNull(remote2.getBuild().version());
            assertNull(remote2.getError());

            ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertThat(local.getSkipUnavailable(), equalTo(false));
            assertNull(local.getMatchingIndices());
            assertNull(local.getBuild());
            assertThat(local.getError(), containsString("no such index [doesnotexist]"));
        }

        // only remote clusters have matching indices
        {
            String[] indexExpressions = new String[] { "f*", REMOTE_CLUSTER_1 + ":" + remoteIndex1, REMOTE_CLUSTER_2 + ":" + remoteIndex2 };
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(indexExpressions);

            ActionFuture<ResolveClusterActionResponse> future = client(LOCAL_CLUSTER).admin()
                .indices()
                .execute(TransportResolveClusterAction.TYPE, request);
            ResolveClusterActionResponse response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(3, clusterInfo.size());
            Set<String> expectedClusterNames = Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));

            ResolveClusterInfo remote1 = clusterInfo.get(REMOTE_CLUSTER_1);
            assertThat(remote1.isConnected(), equalTo(true));
            assertThat(remote1.getSkipUnavailable(), equalTo(skipUnavailable1));
            assertThat(remote1.getMatchingIndices(), equalTo(true));
            assertNotNull(remote1.getBuild().version());
            assertNull(remote1.getError());

            ResolveClusterInfo remote2 = clusterInfo.get(REMOTE_CLUSTER_2);
            assertThat(remote2.isConnected(), equalTo(true));
            assertThat(remote2.getSkipUnavailable(), equalTo(skipUnavailable2));
            assertThat(remote2.getMatchingIndices(), equalTo(true));
            assertNotNull(remote2.getBuild().version());
            assertNull(remote2.getError());

            ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertThat(local.getSkipUnavailable(), equalTo(false));
            assertThat(local.getMatchingIndices(), equalTo(false));
            assertNotNull(local.getBuild().version());
            assertNull(local.getError());
        }

        // only local cluster has matching indices
        {
            String[] indexExpressions = new String[] {
                localIndex,
                REMOTE_CLUSTER_1 + ":" + localIndex,
                REMOTE_CLUSTER_2 + ":" + localIndex };
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(indexExpressions);

            ActionFuture<ResolveClusterActionResponse> future = client(LOCAL_CLUSTER).admin()
                .indices()
                .execute(TransportResolveClusterAction.TYPE, request);
            ResolveClusterActionResponse response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(3, clusterInfo.size());
            Set<String> expectedClusterNames = Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));

            ResolveClusterInfo remote1 = clusterInfo.get(REMOTE_CLUSTER_1);
            assertThat(remote1.isConnected(), equalTo(true));
            assertThat(remote1.getSkipUnavailable(), equalTo(skipUnavailable1));
            assertNull(remote1.getMatchingIndices());
            assertNull(remote1.getBuild());
            assertNotNull(remote1.getError());
            assertThat(remote1.getError(), containsString("no such index [demo]"));

            ResolveClusterInfo remote2 = clusterInfo.get(REMOTE_CLUSTER_2);
            assertThat(remote2.isConnected(), equalTo(true));
            assertThat(remote2.getSkipUnavailable(), equalTo(skipUnavailable2));
            assertNull(remote2.getMatchingIndices());
            assertNull(remote2.getBuild());
            assertNotNull(remote2.getError());
            assertThat(remote2.getError(), containsString("no such index [demo]"));

            ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertThat(local.getSkipUnavailable(), equalTo(false));
            assertThat(local.getMatchingIndices(), equalTo(true));
            assertNotNull(local.getBuild().version());
            assertNull(local.getError());
        }

        // test with wildcard expressions in the index - all clusters should match
        {
            String[] indexExpressions = new String[] {
                localIndex.substring(0, 2) + "*",
                REMOTE_CLUSTER_1 + ":" + remoteIndex1.substring(0, 4) + "*",
                REMOTE_CLUSTER_2 + ":*" };
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(indexExpressions);

            ActionFuture<ResolveClusterActionResponse> future = client(LOCAL_CLUSTER).admin()
                .indices()
                .execute(TransportResolveClusterAction.TYPE, request);
            ResolveClusterActionResponse response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(3, clusterInfo.size());
            Set<String> expectedClusterNames = Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));

            ResolveClusterInfo remote1 = clusterInfo.get(REMOTE_CLUSTER_1);
            assertThat(remote1.isConnected(), equalTo(true));
            assertThat(remote1.getSkipUnavailable(), equalTo(skipUnavailable1));
            assertThat(remote1.getMatchingIndices(), equalTo(true));
            assertNotNull(remote1.getBuild().version());
            assertNull(remote1.getError());

            ResolveClusterInfo remote2 = clusterInfo.get(REMOTE_CLUSTER_2);
            assertThat(remote2.isConnected(), equalTo(true));
            assertThat(remote2.getSkipUnavailable(), equalTo(skipUnavailable2));
            assertThat(remote2.getMatchingIndices(), equalTo(true));
            assertNotNull(remote2.getBuild().version());
            assertNull(remote2.getError());

            ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertThat(local.getSkipUnavailable(), equalTo(false));
            assertThat(local.getMatchingIndices(), equalTo(true));
            assertNotNull(local.getBuild().version());
            assertNull(local.getError());
        }

        // test with wildcard expressions in the cluster and index - all clusters should match
        {
            String[] indexExpressions = new String[] {
                localIndex.substring(0, 2) + "*",
                REMOTE_CLUSTER_1.substring(0, 4) + "*:" + remoteIndex1.substring(0, 3) + "*" };
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(indexExpressions);

            ActionFuture<ResolveClusterActionResponse> future = client(LOCAL_CLUSTER).admin()
                .indices()
                .execute(TransportResolveClusterAction.TYPE, request);
            ResolveClusterActionResponse response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(3, clusterInfo.size());
            Set<String> expectedClusterNames = Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));

            ResolveClusterInfo remote1 = clusterInfo.get(REMOTE_CLUSTER_1);
            assertThat(remote1.isConnected(), equalTo(true));
            assertThat(remote1.getSkipUnavailable(), equalTo(skipUnavailable1));
            assertThat(remote1.getMatchingIndices(), equalTo(true));
            assertNotNull(remote1.getBuild().version());
            assertNull(remote1.getError());

            ResolveClusterInfo remote2 = clusterInfo.get(REMOTE_CLUSTER_2);
            assertThat(remote2.isConnected(), equalTo(true));
            assertThat(remote2.getSkipUnavailable(), equalTo(true));
            assertThat(remote2.getMatchingIndices(), equalTo(true));
            assertNotNull(remote2.getBuild().version());
            assertNull(remote2.getError());

            ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertThat(local.getSkipUnavailable(), equalTo(false));
            assertThat(local.getMatchingIndices(), equalTo(true));
            assertNotNull(local.getBuild().version());
            assertNull(local.getError());
        }

        // only remote1 included
        {
            String[] indexExpressions = new String[] { REMOTE_CLUSTER_1 + ":*" };
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(indexExpressions);

            ActionFuture<ResolveClusterActionResponse> future = client(LOCAL_CLUSTER).admin()
                .indices()
                .execute(TransportResolveClusterAction.TYPE, request);
            ResolveClusterActionResponse response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(1, clusterInfo.size());
            assertThat(clusterInfo.keySet(), equalTo(Set.of(REMOTE_CLUSTER_1)));

            ResolveClusterInfo remote = clusterInfo.get(REMOTE_CLUSTER_1);
            assertThat(remote.isConnected(), equalTo(true));
            assertThat(remote.getSkipUnavailable(), equalTo(skipUnavailable1));
            assertThat(remote.getMatchingIndices(), equalTo(true));
            assertNotNull(remote.getBuild().version());
        }

        // cluster exclusions
        {
            String[] indexExpressions = new String[] { "*", "rem*:*", "-remote1:*" };
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(indexExpressions);

            ActionFuture<ResolveClusterActionResponse> future = client(LOCAL_CLUSTER).admin()
                .indices()
                .execute(TransportResolveClusterAction.TYPE, request);
            ResolveClusterActionResponse response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(2, clusterInfo.size());
            Set<String> expectedClusterNames = Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));

            ResolveClusterInfo remote2 = clusterInfo.get(REMOTE_CLUSTER_2);
            assertThat(remote2.isConnected(), equalTo(true));
            assertThat(remote2.getSkipUnavailable(), equalTo(true));
            assertThat(remote2.getMatchingIndices(), equalTo(true));
            assertNotNull(remote2.getBuild().version());
            assertNull(remote2.getError());

            ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertThat(local.getSkipUnavailable(), equalTo(false));
            assertThat(local.getMatchingIndices(), equalTo(true));
            assertNotNull(local.getBuild().version());
            assertNull(local.getError());
        }

        // index exclusions
        {
            String[] indexExpressions = new String[] { "*", "rem*:*", "-remote1:*", "-" + localIndex };
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(indexExpressions);

            ActionFuture<ResolveClusterActionResponse> future = client(LOCAL_CLUSTER).admin()
                .indices()
                .execute(TransportResolveClusterAction.TYPE, request);
            ResolveClusterActionResponse response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(2, clusterInfo.size());
            Set<String> expectedClusterNames = Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));

            ResolveClusterInfo remote2 = clusterInfo.get(REMOTE_CLUSTER_2);
            assertThat(remote2.isConnected(), equalTo(true));
            assertThat(remote2.getSkipUnavailable(), equalTo(true));
            assertThat(remote2.getMatchingIndices(), equalTo(true));
            assertNotNull(remote2.getBuild().version());
            assertNull(remote2.getError());

            ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertThat(local.getSkipUnavailable(), equalTo(false));
            assertThat(local.getMatchingIndices(), equalTo(false));
            assertNotNull(local.getBuild().version());
            assertNull(local.getError());
        }
    }

    // corresponds to the GET _resolve/cluster endpoint with no index expression specified
    public void testClusterResolveWithNoIndexExpression() throws IOException {
        Map<String, Object> testClusterInfo = setupThreeClusters(false);
        boolean skipUnavailable1 = (Boolean) testClusterInfo.get("remote1.skip_unavailable");
        boolean skipUnavailable2 = true;

        {
            String[] noIndexSpecified = new String[0];
            boolean clusterInfoOnly = true;
            boolean runningOnQueryingCluster = true;
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(
                noIndexSpecified,
                IndicesOptions.DEFAULT,
                clusterInfoOnly,
                runningOnQueryingCluster
            );

            ActionFuture<ResolveClusterActionResponse> future = client(LOCAL_CLUSTER).admin()
                .indices()
                .execute(TransportResolveClusterAction.TYPE, request);
            ResolveClusterActionResponse response = future.actionGet(30, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(2, clusterInfo.size());

            // only remote clusters should be present (not local)
            Set<String> expectedClusterNames = Set.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));

            ResolveClusterInfo remote1 = clusterInfo.get(REMOTE_CLUSTER_1);
            assertThat(remote1.isConnected(), equalTo(true));
            assertThat(remote1.getSkipUnavailable(), equalTo(skipUnavailable1));
            assertThat(remote1.getMatchingIndices(), equalTo(null));  // should not be set
            assertNotNull(remote1.getBuild().version());
            assertNull(remote1.getError());

            ResolveClusterInfo remote2 = clusterInfo.get(REMOTE_CLUSTER_2);
            assertThat(remote2.isConnected(), equalTo(true));
            assertThat(remote2.getSkipUnavailable(), equalTo(skipUnavailable2));
            assertThat(remote2.getMatchingIndices(), equalTo(null));  // should not be set
            assertNotNull(remote2.getBuild().version());
            assertNull(remote2.getError());
        }
    }

    public void testClusterResolveWithMatchingAliases() throws IOException {
        Map<String, Object> testClusterInfo = setupThreeClusters(true);
        String localAlias = (String) testClusterInfo.get("local.alias");
        String remoteAlias1 = (String) testClusterInfo.get("remote1.alias");
        String remoteAlias2 = (String) testClusterInfo.get("remote2.alias");
        boolean skipUnavailable1 = (Boolean) testClusterInfo.get("remote1.skip_unavailable");
        boolean skipUnavailable2 = true;

        // all clusters and both have matching indices
        {
            String[] indexExpressions = new String[] {
                localAlias,
                REMOTE_CLUSTER_1 + ":" + remoteAlias1,
                REMOTE_CLUSTER_2 + ":" + remoteAlias2 };
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(indexExpressions);

            ActionFuture<ResolveClusterActionResponse> future = client(LOCAL_CLUSTER).admin()
                .indices()
                .execute(TransportResolveClusterAction.TYPE, request);
            ResolveClusterActionResponse response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(3, clusterInfo.size());
            Set<String> expectedClusterNames = Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));

            ResolveClusterInfo remote1 = clusterInfo.get(REMOTE_CLUSTER_1);
            assertThat(remote1.isConnected(), equalTo(true));
            assertThat(remote1.getSkipUnavailable(), equalTo(skipUnavailable1));
            assertThat(remote1.getMatchingIndices(), equalTo(true));
            assertNotNull(remote1.getBuild().version());
            assertNull(remote1.getError());

            ResolveClusterInfo remote2 = clusterInfo.get(REMOTE_CLUSTER_2);
            assertThat(remote2.isConnected(), equalTo(true));
            assertThat(remote2.getSkipUnavailable(), equalTo(skipUnavailable2));
            assertThat(remote2.getMatchingIndices(), equalTo(true));
            assertNotNull(remote2.getBuild().version());
            assertNull(remote2.getError());

            ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertThat(local.getSkipUnavailable(), equalTo(false));
            assertThat(local.getMatchingIndices(), equalTo(true));
            assertNotNull(local.getBuild().version());
            assertNull(local.getError());
        }

        // only remote clusters have matching indices
        {
            String[] indexExpressions = new String[] {
                "no-matching-index*",
                REMOTE_CLUSTER_1 + ":" + remoteAlias1,
                REMOTE_CLUSTER_2 + ":" + remoteAlias2 };
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(indexExpressions);

            ActionFuture<ResolveClusterActionResponse> future = client(LOCAL_CLUSTER).admin()
                .indices()
                .execute(TransportResolveClusterAction.TYPE, request);
            ResolveClusterActionResponse response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(3, clusterInfo.size());
            Set<String> expectedClusterNames = Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));

            ResolveClusterInfo remote1 = clusterInfo.get(REMOTE_CLUSTER_1);
            assertThat(remote1.isConnected(), equalTo(true));
            assertThat(remote1.getSkipUnavailable(), equalTo(skipUnavailable1));
            assertThat(remote1.getMatchingIndices(), equalTo(true));
            assertNotNull(remote1.getBuild().version());

            ResolveClusterInfo remote2 = clusterInfo.get(REMOTE_CLUSTER_2);
            assertThat(remote2.isConnected(), equalTo(true));
            assertThat(remote2.getSkipUnavailable(), equalTo(skipUnavailable2));
            assertThat(remote2.getMatchingIndices(), equalTo(true));
            assertNotNull(remote2.getBuild().version());

            ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertThat(local.getSkipUnavailable(), equalTo(false));
            assertThat(local.getMatchingIndices(), equalTo(false));
            assertNotNull(local.getBuild().version());
        }

        // only local cluster has matching alias; using cluster exclusion for remote2
        {
            String[] indexExpressions = new String[] { localAlias, "rem*:foo", "-" + REMOTE_CLUSTER_2 + ":*" };
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(indexExpressions);

            ActionFuture<ResolveClusterActionResponse> future = client(LOCAL_CLUSTER).admin()
                .indices()
                .execute(TransportResolveClusterAction.TYPE, request);
            ResolveClusterActionResponse response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(2, clusterInfo.size());
            assertThat(clusterInfo.keySet(), equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1)));

            ResolveClusterInfo remote = clusterInfo.get(REMOTE_CLUSTER_1);
            assertThat(remote.isConnected(), equalTo(true));
            assertThat(remote.getSkipUnavailable(), equalTo(skipUnavailable1));
            assertNull(remote.getMatchingIndices());
            assertNull(remote.getBuild());
            assertThat(remote.getError(), containsString("no such index [foo]"));

            ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertThat(local.getSkipUnavailable(), equalTo(false));
            assertThat(local.getMatchingIndices(), equalTo(true));
            assertNotNull(local.getBuild().version());
        }
    }

    public void testClusterResolveWithNoMatchingClustersReturnsEmptyResult() throws Exception {
        setupThreeClusters(false);
        {
            String[] indexExpressions = new String[] { "no_matching_cluster*:foo" };
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(indexExpressions);

            ActionFuture<ResolveClusterActionResponse> future = client(LOCAL_CLUSTER).admin()
                .indices()
                .execute(TransportResolveClusterAction.TYPE, request);
            ResolveClusterActionResponse response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(0, clusterInfo.size());
            assertThat(Strings.toString(response), equalTo("{}"));
        }
    }

    public void testClusterResolveDisconnectedAndErrorScenarios() throws Exception {
        Map<String, Object> testClusterInfo = setupThreeClusters(false);
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex1 = (String) testClusterInfo.get("remote1.index");
        String remoteIndex2 = (String) testClusterInfo.get("remote2.index");
        boolean skipUnavailable1 = (Boolean) testClusterInfo.get("remote1.skip_unavailable");
        boolean skipUnavailable2 = true;

        // query for a remote cluster that does not exist without wildcard -> should result in 404 error response
        {
            String[] indexExpressions = new String[] {
                localIndex,
                REMOTE_CLUSTER_1 + ":" + remoteIndex1,
                REMOTE_CLUSTER_2 + ":" + remoteIndex2,
                "no_such_cluster:*" };
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(indexExpressions);

            Exception e = expectThrows(
                ExecutionException.class,
                () -> client(LOCAL_CLUSTER).admin().indices().execute(TransportResolveClusterAction.TYPE, request).get()
            );
            NoSuchRemoteClusterException ce = (NoSuchRemoteClusterException) ExceptionsHelper.unwrap(e, NoSuchRemoteClusterException.class);
            assertThat(ce.getMessage(), containsString("no such remote cluster: [no_such_cluster]"));
        }

        // query for a cluster that does not exist with wildcard -> should NOT result in 404 error response
        {
            String[] indexExpressions = new String[] {
                localIndex,
                REMOTE_CLUSTER_1 + ":" + remoteIndex1,
                REMOTE_CLUSTER_2 + ":" + remoteIndex2,
                "no_such_cluster*:*" };
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(indexExpressions);

            ActionFuture<ResolveClusterActionResponse> future = client(LOCAL_CLUSTER).admin()
                .indices()
                .execute(TransportResolveClusterAction.TYPE, request);
            ResolveClusterActionResponse response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(3, clusterInfo.size());
            Set<String> expectedClusterNames = Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));
        }

        // shut down REMOTE_CLUSTER_1 to get `connected:false` responses from _resolve/cluster

        InternalTestCluster remoteCluster1 = cluster(REMOTE_CLUSTER_1);
        for (String nodeName : remoteCluster1.getNodeNames()) {
            remoteCluster1.stopNode(nodeName);
        }

        // cluster1 was stopped/disconnected, so it should return a connected:false response (but not throw an error)
        {
            String[] indexExpressions = new String[] {
                localIndex,
                REMOTE_CLUSTER_1 + ":" + remoteIndex1,
                REMOTE_CLUSTER_2 + ":" + remoteIndex2 };
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(indexExpressions);

            ActionFuture<ResolveClusterActionResponse> future = client(LOCAL_CLUSTER).admin()
                .indices()
                .execute(TransportResolveClusterAction.TYPE, request);
            ResolveClusterActionResponse response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(3, clusterInfo.size());
            Set<String> expectedClusterNames = Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));

            ResolveClusterInfo remote1 = clusterInfo.get(REMOTE_CLUSTER_1);
            assertThat(remote1.isConnected(), equalTo(false));  // key assert of this sub-test
            assertThat(remote1.getSkipUnavailable(), equalTo(skipUnavailable1));
            assertNull(remote1.getMatchingIndices());
            assertNull(remote1.getBuild());
            assertNull(remote1.getError());

            ResolveClusterInfo remote2 = clusterInfo.get(REMOTE_CLUSTER_2);
            assertThat(remote2.isConnected(), equalTo(true));
            assertThat(remote2.getSkipUnavailable(), equalTo(skipUnavailable2));
            assertThat(remote2.getMatchingIndices(), equalTo(true));
            assertNotNull(remote2.getBuild().version());
            assertNull(remote2.getError());

            ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertThat(local.getSkipUnavailable(), equalTo(false));
            assertThat(local.getMatchingIndices(), equalTo(true));
            assertNotNull(local.getBuild().version());
            assertNull(local.getError());
        }

        // cluster1 was stopped/disconnected, so it should return a connected:false response when querying with no index expression,
        // corresponding to GET _resolve/cluster endpoint
        {
            String[] noIndexSpecified = new String[0];
            boolean clusterInfoOnly = true;
            boolean runningOnQueryingCluster = true;
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(
                noIndexSpecified,
                IndicesOptions.DEFAULT,
                clusterInfoOnly,
                runningOnQueryingCluster
            );

            ActionFuture<ResolveClusterActionResponse> future = client(LOCAL_CLUSTER).admin()
                .indices()
                .execute(TransportResolveClusterAction.TYPE, request);
            ResolveClusterActionResponse response = future.actionGet(30, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(2, clusterInfo.size());
            // local cluster is not present when querying without an index expression
            Set<String> expectedClusterNames = Set.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));

            ResolveClusterInfo remote1 = clusterInfo.get(REMOTE_CLUSTER_1);
            assertThat(remote1.isConnected(), equalTo(false));
            assertThat(remote1.getSkipUnavailable(), equalTo(skipUnavailable1));
            assertNull(remote1.getMatchingIndices());
            assertNull(remote1.getBuild());
            assertNull(remote1.getError());

            ResolveClusterInfo remote2 = clusterInfo.get(REMOTE_CLUSTER_2);
            assertThat(remote2.isConnected(), equalTo(true));
            assertThat(remote2.getSkipUnavailable(), equalTo(skipUnavailable2));
            assertNull(remote2.getMatchingIndices());  // not present when no index expression specified
            assertNotNull(remote2.getBuild().version());
            assertNull(remote2.getError());
        }
    }

    private Map<String, Object> setupThreeClusters(boolean useAlias) {
        String localAlias = randomAlphaOfLengthBetween(5, 25);
        String remoteAlias1 = randomAlphaOfLengthBetween(5, 25);
        String remoteAlias2 = randomAlphaOfLengthBetween(5, 25);

        // set up local cluster and index

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
            IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
            aliasesAddRequest.addAliasAction(addAction);

            assertAcked(client(LOCAL_CLUSTER).admin().indices().aliases(aliasesAddRequest));
        }
        indexDocs(client(LOCAL_CLUSTER), localIndex);

        // set up remote1 cluster and index

        String remoteIndex1 = "prod";
        int numShardsRemote1 = randomIntBetween(3, 6);
        final InternalTestCluster remoteCluster1 = cluster(REMOTE_CLUSTER_1);
        remoteCluster1.ensureAtLeastNumDataNodes(randomIntBetween(1, 3));
        final Settings.Builder remoteSettings1 = Settings.builder();
        remoteSettings1.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShardsRemote1);

        assertAcked(
            client(REMOTE_CLUSTER_1).admin()
                .indices()
                .prepareCreate(remoteIndex1)
                .setSettings(Settings.builder().put(remoteSettings1.build()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
                .setMapping("@timestamp", "type=date", "f", "type=text")
        );
        if (useAlias) {
            // remote1 index alias
            IndicesAliasesRequest.AliasActions addAction = new IndicesAliasesRequest.AliasActions(
                IndicesAliasesRequest.AliasActions.Type.ADD
            ).index(remoteIndex1).aliases(remoteAlias1);
            IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
            aliasesAddRequest.addAliasAction(addAction);

            assertAcked(client(REMOTE_CLUSTER_1).admin().indices().aliases(aliasesAddRequest));
        }

        assertFalse(
            client(REMOTE_CLUSTER_1).admin()
                .cluster()
                .prepareHealth(TEST_REQUEST_TIMEOUT, remoteIndex1)
                .setWaitForYellowStatus()
                .setTimeout(TimeValue.timeValueSeconds(10))
                .get()
                .isTimedOut()
        );
        indexDocs(client(REMOTE_CLUSTER_1), remoteIndex1);

        // set up remote2 cluster and index

        String remoteIndex2 = "prod123";
        int numShardsRemote2 = randomIntBetween(2, 4);
        final InternalTestCluster remoteCluster2 = cluster(REMOTE_CLUSTER_2);
        remoteCluster2.ensureAtLeastNumDataNodes(randomIntBetween(1, 2));
        final Settings.Builder remoteSettings2 = Settings.builder();
        remoteSettings2.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShardsRemote2);

        assertAcked(
            client(REMOTE_CLUSTER_2).admin()
                .indices()
                .prepareCreate(remoteIndex2)
                .setSettings(Settings.builder().put(remoteSettings2.build()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
                .setMapping("@timestamp", "type=date", "f", "type=text")
        );
        if (useAlias) {
            // remote2 index alias
            IndicesAliasesRequest.AliasActions addAction = new IndicesAliasesRequest.AliasActions(
                IndicesAliasesRequest.AliasActions.Type.ADD
            ).index(remoteIndex2).aliases(remoteAlias2);
            IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
            aliasesAddRequest.addAliasAction(addAction);

            assertAcked(client(REMOTE_CLUSTER_2).admin().indices().aliases(aliasesAddRequest));
        }

        assertFalse(
            client(REMOTE_CLUSTER_2).admin()
                .cluster()
                .prepareHealth(TEST_REQUEST_TIMEOUT, remoteIndex2)
                .setWaitForYellowStatus()
                .setTimeout(TimeValue.timeValueSeconds(10))
                .get()
                .isTimedOut()
        );
        indexDocs(client(REMOTE_CLUSTER_2), remoteIndex2);

        String skipUnavailableKey = Strings.format("cluster.remote.%s.skip_unavailable", REMOTE_CLUSTER_1);
        Setting<?> skipUnavailableSetting = cluster(REMOTE_CLUSTER_1).clusterService().getClusterSettings().get(skipUnavailableKey);
        boolean skipUnavailable1 = (boolean) cluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).clusterService()
            .getClusterSettings()
            .get(skipUnavailableSetting);

        Map<String, Object> clusterInfo = new HashMap<>();
        clusterInfo.put("local.index", localIndex);

        clusterInfo.put("remote1.index", remoteIndex1);
        clusterInfo.put("remote1.skip_unavailable", skipUnavailable1);

        clusterInfo.put("remote2.index", remoteIndex2);
        clusterInfo.put("remote2.skip_unavailable", true);

        if (useAlias) {
            clusterInfo.put("local.alias", localAlias);
            clusterInfo.put("remote1.alias", remoteAlias1);
            clusterInfo.put("remote2.alias", remoteAlias2);
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
