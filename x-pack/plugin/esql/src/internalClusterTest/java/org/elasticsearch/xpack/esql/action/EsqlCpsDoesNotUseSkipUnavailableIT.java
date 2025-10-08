/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.transport.ConnectTransportException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

// TODO: Move this test to the Serverless repo once the IT framework is ready there.
public class EsqlCpsDoesNotUseSkipUnavailableIT extends AbstractMultiClustersTestCase {
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
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(EsqlAsyncActionIT.LocalStateEsqlAsync.class);
        plugins.add(CpsPlugin.class);
        return plugins;
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
            assertThat(info.getCluster(LINKED_CLUSTER_1).getStatus(), is(EsqlExecutionInfo.Cluster.Status.SKIPPED));
        }

        request = new EsqlQueryRequest().query("FROM *,*:* | limit 10");
        request.allowPartialResults(false);
        try (EsqlQueryResponse response = runQuery(request)) {
            fail("a fatal error should be thrown since allow_partial_results=false");
        } catch (Exception e) {
            assertThat(e, instanceOf(ConnectTransportException.class));
        }

        /*
         * We usually get a top-level error when skip_unavailable is false. However, irrespective of that setting in this test, we now
         * observe a top-level error when partial results are disallowed. This proves that skip_unavailable's scope has now shifted to
         * allow_partial_search_results in CPS environment.
         */
    }

    private EsqlQueryResponse runQuery(EsqlQueryRequest request) {
        return client(LOCAL_CLUSTER).execute(EsqlQueryAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS);
    }
}
