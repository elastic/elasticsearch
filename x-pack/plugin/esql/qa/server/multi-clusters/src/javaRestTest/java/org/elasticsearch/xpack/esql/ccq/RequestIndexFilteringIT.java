/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ccq;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.qa.rest.RequestIndexFilteringTestCase;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class RequestIndexFilteringIT extends RequestIndexFilteringTestCase {

    static ElasticsearchCluster remoteCluster = Clusters.remoteCluster();
    static ElasticsearchCluster localCluster = Clusters.localCluster(remoteCluster);

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);
    private static RestClient remoteClient;

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    @Before
    public void setRemoteClient() throws IOException {
        if (remoteClient == null) {
            var clusterHosts = parseClusterHosts(remoteCluster.getHttpAddresses());
            remoteClient = buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[0]));
        }
    }

    private boolean isCCSRequest;

    @BeforeClass
    public static void checkVersion() {
        assumeTrue("skip if version before 8.18", Clusters.localClusterVersion().onOrAfter(Version.V_8_18_0));
    }

    @AfterClass
    public static void closeRemoteClients() throws IOException {
        try {
            IOUtils.close(remoteClient);
        } finally {
            remoteClient = null;
        }
    }

    @Override
    protected void indexTimestampData(int docs, String indexName, String date, String differentiatorFieldName) throws IOException {
        indexTimestampDataForClient(client(), docs, indexName, date, differentiatorFieldName);
        indexTimestampDataForClient(remoteClient, docs, indexName, date, differentiatorFieldName);
    }

    @Override
    protected String from(String... indexName) {
        isCCSRequest = randomBoolean();
        if (isCCSRequest) {
            return "FROM *:" + String.join(",*:", indexName);
        } else {
            return "FROM " + String.join(",", indexName);
        }
    }

    @Override
    public Map<String, Object> runEsql(RestEsqlTestCase.RequestObjectBuilder requestObject) throws IOException {
        if (requestObject.allowPartialResults() != null) {
            assumeTrue(
                "require allow_partial_results on local cluster",
                clusterHasCapability("POST", "/_query", List.of(), List.of("support_partial_results")).orElse(false)
            );
        }
        requestObject.includeCCSMetadata(true);
        return super.runEsql(requestObject);
    }

    @After
    public void wipeRemoteTestData() throws IOException {
        try {
            var response = remoteClient.performRequest(new Request("DELETE", "/test*"));
            assertEquals(200, response.getStatusLine().getStatusCode());
        } catch (ResponseException re) {
            assertEquals(404, re.getResponse().getStatusLine().getStatusCode());
        }
    }

    private MapMatcher getClustersMetadataMatcher() {
        MapMatcher mapMatcher = matchesMap();
        mapMatcher = mapMatcher.entry("running", 0);
        mapMatcher = mapMatcher.entry("total", 1);
        mapMatcher = mapMatcher.entry("failed", 0);
        mapMatcher = mapMatcher.entry("partial", 0);
        mapMatcher = mapMatcher.entry("successful", 1);
        mapMatcher = mapMatcher.entry("skipped", 0);
        mapMatcher = mapMatcher.entry(
            "details",
            matchesMap().entry(
                Clusters.REMOTE_CLUSTER_NAME,
                matchesMap().entry("_shards", matchesMap().extraOk())
                    .entry("took", greaterThanOrEqualTo(0))
                    .entry("indices", instanceOf(String.class))
                    .entry("status", "successful")
            )
        );
        return mapMatcher;
    }

    @Override
    protected void assertQueryResult(Map<String, Object> result, Matcher<?> columnMatcher, Matcher<?> valuesMatcher) {
        var matcher = getResultMatcher(result).entry("columns", columnMatcher).entry("values", valuesMatcher);
        if (isCCSRequest) {
            matcher = matcher.entry("_clusters", getClustersMetadataMatcher());
        }
        assertMap(result, matcher);
    }

}
