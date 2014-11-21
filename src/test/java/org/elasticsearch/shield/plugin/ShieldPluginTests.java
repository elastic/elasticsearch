/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.plugin;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.test.ShieldIntegrationTest;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.RestStatus.UNAUTHORIZED;
import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.*;

public class ShieldPluginTests extends ShieldIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(InternalNode.HTTP_ENABLED, true)
                .put("shield.http.ssl", false)
                .put("shield.audit.enabled", false)
                .build();
    }

    @Test
    public void testThatPluginIsLoaded() throws IOException {
        logger.info("--> Getting nodes info");
        NodesInfoResponse nodeInfos = internalCluster().transportClient().admin().cluster().prepareNodesInfo().get();
        logger.info("--> Checking nodes info that shield plugin is loaded");
        for (NodeInfo nodeInfo : nodeInfos.getNodes()) {
            assertThat(nodeInfo.getPlugins().getInfos(), hasSize(1));
            assertThat(nodeInfo.getPlugins().getInfos().get(0).getName(), is(ShieldPlugin.NAME));
        }

        HttpServerTransport httpServerTransport = internalCluster().getDataNodeInstance(HttpServerTransport.class);
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            logger.info("Executing unauthorized request to /_shield infos");
            HttpResponse response = new HttpRequestBuilder(httpClient).httpTransport(httpServerTransport).method("GET").path("/_shield").execute();
            assertThat(response.getStatusCode(), is(UNAUTHORIZED.getStatus()));

            logger.info("Executing authorized request to /_shield infos");
            response = new HttpRequestBuilder(httpClient).httpTransport(httpServerTransport).method("GET").path("/_shield").addHeader("Authorization", basicAuthHeaderValue(DEFAULT_USER_NAME, new SecuredString(DEFAULT_PASSWORD.toCharArray()))).execute();
            assertThat(response.getStatusCode(), is(OK.getStatus()));
            assertThat(response.getBody(), allOf(containsString("build_hash"), containsString("build_timestamp"), containsString("build_version")));
        }
    }
}
