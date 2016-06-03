/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import org.apache.http.HttpStatus;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.ElasticsearchResponseException;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.marvel.Monitoring;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.shield.Security;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolInfo;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.watcher.execution.InternalWatchExecutor;

import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 *
 */
@ClusterScope(scope = SUITE, numClientNodes = 0, transportClientRatio = 0, randomDynamicTemplates = false, maxNumDataNodes = 3)
public class WatcherPluginDisableTests extends ESIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(XPackPlugin.featureEnabledSetting(Watcher.NAME), false)

                // disable shield because of query cache check and authentication/authorization
                .put(XPackPlugin.featureEnabledSetting(Security.NAME), false)
                .put(XPackPlugin.featureEnabledSetting(Monitoring.NAME), false)

                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.<Class<? extends Plugin>>singleton(XPackPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.<Class<? extends Plugin>>singleton(XPackPlugin.class);
    }

    @Override
    protected Settings transportClientSettings() {
        return Settings.builder()
                .put(super.transportClientSettings())
                .build();
    }

    public void testRestEndpoints() throws Exception {
        HttpServerTransport httpServerTransport = internalCluster().getDataNodeInstance(HttpServerTransport.class);
        try {
            getRestClient().performRequest("GET", "/_xpack/watcher", Collections.emptyMap(), null);
            fail("request should have failed");
        } catch(ElasticsearchResponseException e) {
            assertThat(e.getElasticsearchResponse().getStatusLine().getStatusCode(), is(HttpStatus.SC_BAD_REQUEST));
        }
    }

    public void testThreadPools() throws Exception {
        NodesInfoResponse nodesInfo = client().admin().cluster().prepareNodesInfo().setThreadPool(true).get();
        for (NodeInfo nodeInfo : nodesInfo.getNodes()) {
            ThreadPoolInfo threadPoolInfo = nodeInfo.getThreadPool();
            for (ThreadPool.Info info : threadPoolInfo) {
                assertThat(info.getName(), not(is(InternalWatchExecutor.THREAD_POOL_NAME)));
            }
        }
    }
}
