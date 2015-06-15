/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher;

import org.apache.http.HttpStatus;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.license.plugin.LicensePlugin;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolInfo;
import org.elasticsearch.watcher.execution.InternalWatchExecutor;
import org.junit.Test;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.SUITE;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 *
 */
@ClusterScope(scope = SUITE, numClientNodes = 0, transportClientRatio = 0, randomDynamicTemplates = false, maxNumDataNodes = 3)
public class WatcherPluginDisableTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugin.types", WatcherPlugin.class.getName() + "," + LicensePlugin.class.getName())
                .put(WatcherPlugin.ENABLED_SETTING, false)
                .put(Node.HTTP_ENABLED, true)
                .build();
    }

    @Test
    public void testRestEndpoints() throws Exception {
        HttpServerTransport httpServerTransport = internalCluster().getDataNodeInstance(HttpServerTransport.class);
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpRequestBuilder request = new HttpRequestBuilder(httpClient).httpTransport(httpServerTransport).method("GET").path("/_watcher");
            HttpResponse response = request.execute();
            assertThat(response.getStatusCode(), is(HttpStatus.SC_BAD_REQUEST));
        }
    }

    @Test
    public void testThreadPools() throws Exception {
        NodesInfoResponse nodesInfo = client().admin().cluster().prepareNodesInfo().setThreadPool(true).get();
        for (NodeInfo nodeInfo : nodesInfo) {
            ThreadPoolInfo threadPoolInfo = nodeInfo.getThreadPool();
            for (ThreadPool.Info info : threadPoolInfo) {
                assertThat(info.getName(), not(is(InternalWatchExecutor.THREAD_POOL_NAME)));
            }
        }
    }
}
