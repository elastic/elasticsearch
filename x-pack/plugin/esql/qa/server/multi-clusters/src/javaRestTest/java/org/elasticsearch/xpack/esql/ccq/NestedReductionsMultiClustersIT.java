/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ccq;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.ccq.Clusters.REMOTE_CLUSTER_NAME;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * The nested functions over indices of a remote cluster. Each cluster reads the child documents of its own parents, so the
 * results are the same as over local indices with the same documents. A remote that predates the functions is rejected
 * when the query is analyzed, before any cluster reads anything.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class NestedReductionsMultiClustersIT extends ESRestTestCase {
    static ElasticsearchCluster remoteCluster = Clusters.remoteCluster();
    static ElasticsearchCluster localCluster = Clusters.localCluster(remoteCluster);

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);

    private static RestClient remoteClient;
    private static boolean indicesCreated;

    private static final String NESTED_MAPPING = """
        "properties": {
          "id": { "type": "keyword" },
          "items": {
            "type": "nested",
            "properties": {
              "price": { "type": "long" },
              "sku": { "type": "keyword" }
            }
          }
        }
        """;

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    private RestClient remoteClusterClient() throws IOException {
        if (remoteClient == null) {
            var clusterHosts = parseClusterHosts(remoteCluster.getHttpAddresses());
            remoteClient = buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[0]));
        }
        return remoteClient;
    }

    @AfterClass
    public static void closeRemoteClient() throws IOException {
        try {
            IOUtils.close(remoteClient);
        } finally {
            remoteClient = null;
            indicesCreated = false;
        }
    }

    @Before
    public void setUpIndices() throws IOException {
        assumeTrue("the nested functions are registered on snapshot builds only", Build.current().isSnapshot());
        assumeTrue("an older local cluster does not know the nested functions", Clusters.localClusterVersion().equals(Version.CURRENT));
        if (indicesCreated) {
            return;
        }
        createIndex(client(), "nr_ccs", Settings.builder().put("index.number_of_shards", 1).build(), NESTED_MAPPING, null);
        index(client(), "nr_ccs", """
            {"index": {"_id": "l1"}}
            {"id": "l1", "items": [{"price": 1, "sku": "a"}, {"price": 2, "sku": "b"}]}
            {"index": {"_id": "l2"}}
            {"id": "l2", "items": []}
            """);

        RestClient remote = remoteClusterClient();
        createIndex(remote, "nr_ccs", Settings.builder().put("index.number_of_shards", 2).build(), NESTED_MAPPING, null);
        index(remote, "nr_ccs", """
            {"index": {"_id": "r1", "routing": "1"}}
            {"id": "r1", "items": [{"price": 10, "sku": "c"}]}
            {"index": {"_id": "r2", "routing": "2"}}
            {"id": "r2", "items": [{"price": 3, "sku": "d"}, {"price": 5, "sku": "a"}]}
            """);
        createIndex(remote, "nr_ccs_unmapped", Settings.builder().put("index.number_of_shards", 1).build(), """
            "dynamic": false,
            "properties": {
              "id": { "type": "keyword" }
            }
            """, null);
        index(remote, "nr_ccs_unmapped", """
            {"index": {"_id": "r3"}}
            {"id": "r3", "items": [{"price": 999, "sku": "a"}]}
            """);
        createIndex(remote, "nr_ccs_object", Settings.builder().put("index.number_of_shards", 1).build(), """
            "properties": {
              "id": { "type": "keyword" },
              "items": {
                "properties": {
                  "price": { "type": "long" },
                  "sku": { "type": "keyword" }
                }
              }
            }
            """, null);
        index(remote, "nr_ccs_object", """
            {"index": {"_id": "o1"}}
            {"id": "o1", "items": [{"price": 7, "sku": "a"}]}
            """);
        indicesCreated = true;
    }

    public void testRemoteReductions() throws IOException {
        assumeRemoteIsCurrent();
        assertValues(
            query("""
                FROM %s:nr_ccs
                | EVAL n = NESTED(COUNT(items.price)), total = NESTED(SUM(items.price)), hi = NESTED(MAX(items.price)),
                       avg = NESTED(AVG(items.price)), a = NESTED_ANY(items.sku == "a")
                | KEEP id, n, total, hi, avg, a
                | SORT id
                | LIMIT 10""".formatted(REMOTE_CLUSTER_NAME)),
            List.of(List.of("r1", 1, 10, 10, 10.0, false), List.of("r2", 2, 8, 5, 4.0, true))
        );
    }

    public void testLocalAndRemoteReductions() throws IOException {
        assumeRemoteIsCurrent();
        assertValues(
            query("""
                FROM nr_ccs,%s:nr_ccs
                | EVAL n = NESTED(COUNT(items.price)), total = NESTED(SUM(items.price))
                | KEEP id, n, total
                | SORT id
                | LIMIT 10""".formatted(REMOTE_CLUSTER_NAME)),
            List.of(List.of("l1", 2, 3), Arrays.asList("l2", 0, null), List.of("r1", 1, 10), List.of("r2", 2, 8))
        );
    }

    public void testFilterThenExpand() throws IOException {
        assumeRemoteIsCurrent();
        assertValues(
            query("""
                FROM nr_ccs,%s:nr_ccs
                | WHERE NESTED_ANY(items.sku == "a")
                | NESTED_EXPAND items
                | KEEP id, items.price, items.sku
                | SORT id, items.price
                | LIMIT 10""".formatted(REMOTE_CLUSTER_NAME)),
            List.of(List.of("l1", 1, "a"), List.of("l1", 2, "b"), List.of("r2", 3, "d"), List.of("r2", 5, "a"))
        );
    }

    public void testFlatten() throws IOException {
        assumeRemoteIsCurrent();
        assertValues(
            query("""
                FROM nr_ccs,%s:nr_ccs
                | NESTED_FLATTEN items
                | KEEP id, items.price
                | SORT id
                | LIMIT 10""".formatted(REMOTE_CLUSTER_NAME)),
            List.of(List.of("l1", List.of(1, 2)), Arrays.asList("l2", null), List.of("r1", 10), List.of("r2", List.of(3, 5)))
        );
    }

    /**
     * A remote index that does not map the collection has no children to read, even when its source has them, as for a
     * local index.
     */
    public void testRemoteIndexWithoutTheCollection() throws IOException {
        assumeRemoteIsCurrent();
        assertValues(query("""
            FROM nr_ccs,%s:nr_ccs_unmapped
            | WHERE id == "r3"
            | EVAL hit = NESTED_ANY(items.price > 0), total = NESTED(SUM(items.price))
            | KEEP id, hit, total
            | LIMIT 10""".formatted(REMOTE_CLUSTER_NAME)), List.of(Arrays.asList("r3", false, null)));
    }

    public void testCollectionNestedLocallyAndObjectRemotely() throws IOException {
        assumeRemoteIsCurrent();
        ResponseException e = expectThrows(
            ResponseException.class,
            () -> query("FROM nr_ccs,%s:nr_ccs_object | EVAL total = NESTED(SUM(items.price)) | LIMIT 10".formatted(REMOTE_CLUSTER_NAME))
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(
            e.getMessage(),
            containsString("[items] is [nested] in [nr_ccs] but [object] in [" + REMOTE_CLUSTER_NAME + ":nr_ccs_object]")
        );
    }

    public void testOlderRemoteIsRejected() throws IOException {
        assumeTrue("the remote cluster is current", Clusters.remoteClusterVersion().before(Version.CURRENT));
        ResponseException e = expectThrows(
            ResponseException.class,
            () -> query("FROM nr_ccs,%s:nr_ccs | EVAL total = NESTED(SUM(items.price)) | LIMIT 10".formatted(REMOTE_CLUSTER_NAME))
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e.getMessage(), containsString("NESTED requires all nodes to support nested reductions"));
    }

    private static void assumeRemoteIsCurrent() {
        assumeTrue("an older remote cluster is rejected", Clusters.remoteClusterVersion().equals(Version.CURRENT));
    }

    private static void index(RestClient client, String index, String bulk) throws IOException {
        Request request = new Request("POST", "/" + index + "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity(bulk);
        Map<String, Object> response = entityAsMap(client.performRequest(request));
        assertThat(response.get("errors"), equalTo(false));
    }

    private static Map<String, Object> query(String query) throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().startObject().field("query", query).endObject()));
        return entityAsMap(client().performRequest(request));
    }

    private static void assertValues(Map<String, Object> response, List<List<?>> expected) {
        assertThat(response.toString(), response.get("values"), equalTo(expected));
    }
}
