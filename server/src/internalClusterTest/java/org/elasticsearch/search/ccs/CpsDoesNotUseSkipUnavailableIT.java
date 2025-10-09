/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.ccs;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.hamcrest.Matchers;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

// TODO: Move this test to the Serverless repo once the IT framework is ready there.
public class CpsDoesNotUseSkipUnavailableIT extends AbstractMultiClustersTestCase {
    private static final String LINKED_CLUSTER_1 = "cluster-a";

    public static class CpsPlugin extends Plugin implements ClusterPlugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(CpsEnableSetting);
        }
    }

    private static final Setting<String> CpsEnableSetting = Setting.simpleString(
        "serverless.cross_project.enabled",
        Setting.Property.NodeScope
    );

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(LINKED_CLUSTER_1);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return CollectionUtils.appendToCopy(super.nodePlugins(clusterAlias), CpsPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put("serverless.cross_project.enabled", "true").build();
    }

    public void testCpsShouldNotUseSkipUnavailable() throws Exception {
        // Add some dummy data to prove we are communicating fine with the remote.
        assertAcked(client(LINKED_CLUSTER_1).admin().indices().prepareCreate("test-index"));
        client(LINKED_CLUSTER_1).prepareIndex("test-index").setSource("sample-field", "sample-value").get();
        client(LINKED_CLUSTER_1).admin().indices().prepareRefresh("test-index").get();

        // Shut down the linked cluster we'd be targeting in the search.
        try {
            cluster(LINKED_CLUSTER_1).close();
        } catch (Exception e) {
            throw new AssertionError(e);
        }

        // 1. We first execute a search request with partial results allowed and shouldn't observe any top-level errors.
        {
            var searchRequest = getSearchRequest(true);
            searchRequest.setCcsMinimizeRoundtrips(randomBoolean());
            assertResponse(client().execute(TransportSearchAction.TYPE, searchRequest), result -> {
                var originCluster = result.getClusters().getCluster(LOCAL_CLUSTER);
                assertThat(originCluster.getStatus(), Matchers.is(SearchResponse.Cluster.Status.SUCCESSFUL));

                var linkedCluster = result.getClusters().getCluster(LINKED_CLUSTER_1);
                assertThat(linkedCluster.getStatus(), Matchers.is(SearchResponse.Cluster.Status.SKIPPED));

                var linkedClusterFailures = result.getClusters().getCluster(LINKED_CLUSTER_1).getFailures();
                assertThat(linkedClusterFailures.size(), Matchers.is(1));
                // Failure is something along the lines of shard failure and is caused by a connection error.
                assertThat(
                    linkedClusterFailures.getFirst().getCause(),
                    Matchers.anyOf(
                        Matchers.instanceOf(RemoteTransportException.class),
                        Matchers.instanceOf(ConnectTransportException.class)
                    )
                );
            });
        }

        // 2. We now execute a search request with partial results disallowed and should observe a top-level error.
        {
            var searchRequest = getSearchRequest(false);
            searchRequest.setCcsMinimizeRoundtrips(randomBoolean());
            var ae = expectThrows(AssertionError.class, () -> safeGet(client().execute(TransportSearchAction.TYPE, searchRequest)));
            assertThat(ae.getCause(), Matchers.instanceOf(ExecutionException.class));
            assertThat(
                ae.getCause().getCause(),
                Matchers.anyOf(Matchers.instanceOf(RemoteTransportException.class), Matchers.instanceOf(ConnectTransportException.class))
            );
        }

        /*
         * We usually get a top-level error when skip_unavailable is false. However, irrespective of that setting in this test, we now
         * observe a top-level error when partial results are disallowed. This proves that skip_unavailable's scope has now shifted to
         * allow_partial_search_results in CPS environment.
         */
    }

    private SearchRequest getSearchRequest(boolean allowPartialResults) {
        // Include both origin and linked cluster in the search op.
        var searchRequest = new SearchRequest("*", "*:*");
        searchRequest.allowPartialSearchResults(allowPartialResults);
        return searchRequest;
    }
}
