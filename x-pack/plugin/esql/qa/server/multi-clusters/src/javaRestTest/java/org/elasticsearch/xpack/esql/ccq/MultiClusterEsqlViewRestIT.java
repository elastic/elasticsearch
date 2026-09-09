/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ccq;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matcher;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.ccq.Clusters.REMOTE_CLUSTER_NAME;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class MultiClusterEsqlViewRestIT extends ESRestTestCase {

    static ElasticsearchCluster remoteCluster = Clusters.remoteCluster();
    static ElasticsearchCluster localCluster = Clusters.localCluster(remoteCluster, false);

    private static RestClient remoteClient;

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    private RestClient remoteClusterClient() throws IOException {
        if (remoteClient == null) {
            var clusterHosts = parseClusterHosts(remoteCluster.getHttpAddresses());
            remoteClient = buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[0]));
        }
        return remoteClient;
    }

    @AfterClass
    public static void closeRemoteClients() throws IOException {
        try {
            IOUtils.close(remoteClient);
        } finally {
            remoteClient = null;
        }
    }

    public void testLocalView() throws IOException {
        assumeTrue(
            "views not supported",
            clusterHasCapability("POST", "/_query", List.of(), List.of("views_crud_as_index_actions")).orElse(false)
        );
        createIndex(client(), "data", Settings.builder().put("index.number_of_shards", 1).build());
        createView(client(), "view", "FROM data | WHERE true");

        // existing view
        assertOK(runEsqlQuery(client(), "FROM view"));

        // not existing view
        expectResponseException(400, containsString("Unknown index [not-found]"), () -> runEsqlQuery(client(), "FROM not-found"));
    }

    public void testRemoteView() throws IOException {
        assumeTrue(
            "views not supported",
            clusterHasCapability("POST", "/_query", List.of(), List.of("views_crud_as_index_actions")).orElse(false)
        );
        assumeTrue(
            "views not supported on remote cluster",
            clusterHasCapability(remoteClusterClient(), "POST", "/_query", List.of(), List.of("views_crud_as_index_actions")).orElse(false)
        );
        createIndex(remoteClusterClient(), "data", Settings.builder().put("index.number_of_shards", 1).build());
        createView(remoteClusterClient(), "view", "FROM data | WHERE true");

        // existing view
        expectResponseException(
            400,
            containsString("ES|QL queries with remote views are not supported"),
            () -> runEsqlQuery(client(), "FROM " + REMOTE_CLUSTER_NAME + ":view")
        );

        // not existing view
        expectResponseException(
            400,
            containsString("Unknown index [" + REMOTE_CLUSTER_NAME + ":not-found]"),
            () -> runEsqlQuery(client(), "FROM " + REMOTE_CLUSTER_NAME + ":not-found")
        );
    }

    private void createView(RestClient client, String name, String query) throws IOException {
        Request putView = new Request("PUT", "/_query/view/" + name);
        putView.setJsonEntity(Strings.format("""
            {"query": "%s"}""", query));
        assertOK(client.performRequest(putView));
    }

    private Response runEsqlQuery(RestClient client, String query) throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity(Strings.format("""
            {"query": "%s | LIMIT 1"}""", query));
        return client.performRequest(request);
    }

    private void expectResponseException(int statusCode, Matcher<String> messageMatcher, ThrowingRunnable runnable) {
        var exception = expectThrows(ResponseException.class, runnable);
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(statusCode));
        assertThat(exception.getMessage(), messageMatcher);
    }
}
