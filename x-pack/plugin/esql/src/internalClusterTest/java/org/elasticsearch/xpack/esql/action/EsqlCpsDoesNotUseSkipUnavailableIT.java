/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.ConnectTransportException;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

// TODO: Move this test to the Serverless repo once the IT framework is ready there.
public class EsqlCpsDoesNotUseSkipUnavailableIT extends AbstractCrossClusterTestCase {

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
        return List.of(REMOTE_CLUSTER_1);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return CollectionUtils.appendToCopy(super.nodePlugins(clusterAlias), CpsPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put("serverless.cross_project.enabled", "true").build();
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        // Setting skip_unavailable=false results in a fatal error when the linked cluster is not available.
        return Map.of(REMOTE_CLUSTER_1, false);
    }

    public void testCpsShouldNotUseSkipUnavailable() throws Exception {
        // Add some dummy data to prove we are communicating fine with the remote.
        assertAcked(client(REMOTE_CLUSTER_1).admin().indices().prepareCreate("test-index"));
        client(REMOTE_CLUSTER_1).prepareIndex("test-index").setSource("sample-field", "sample-value").get();
        client(REMOTE_CLUSTER_1).admin().indices().prepareRefresh("test-index").get();

        // Shut down the linked cluster we'd be targeting in the search.
        try {
            cluster(REMOTE_CLUSTER_1).close();
        } catch (Exception e) {
            throw new AssertionError(e);
        }

        /*
         * Under normal circumstances, we should get a fatal error for when skip_unavailable=false for a linked cluster
         * and that cluster is targeted in a search op. However, in CPS environment, setting allow_partial_search_results=true
         * should not result in a fatal error.
         */

        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query("FROM *,*:* | limit 10");
        request.allowPartialResults(true);
        try (EsqlQueryResponse response = runQuery(request)) {
            assertThat(response.isPartial(), is(true));
            EsqlExecutionInfo info = response.getExecutionInfo();
            assertThat(info.getCluster(REMOTE_CLUSTER_1).getStatus(), is(EsqlExecutionInfo.Cluster.Status.SKIPPED));
        }

        request = new EsqlQueryRequest().query("FROM *,*:* | limit 10");
        try (EsqlQueryResponse response = runQuery(request)) {
            fail("a fatal error should be thrown since allow_partial_results=false");
        } catch (Exception e) {
            assertThat(e, instanceOf(ConnectTransportException.class));
        }

    }

}
