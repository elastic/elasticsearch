/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.multiproject.MultiProjectRestTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class SearchMultiProjectIT extends MultiProjectRestTestCase {

    private static final String PASSWORD = "hunter2";

    @ClassRule
    public static ElasticsearchCluster cluster = createCluster();

    @Rule
    public final TestName testNameRule = new TestName();

    private static ElasticsearchCluster createCluster() {
        LocalClusterSpecBuilder<ElasticsearchCluster> clusterBuilder = ElasticsearchCluster.local()
            .nodes(1)
            .distribution(DistributionType.INTEG_TEST)
            .module("test-multi-project")
            .setting("test.multi_project.enabled", "true")
            .setting("xpack.security.enabled", "true")
            .user("admin", PASSWORD);
        return clusterBuilder.build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        final String token = basicAuthHeaderValue("admin", new SecureString(PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testSearchIndexThatExistsInMultipleProjects() throws Exception {
        final ProjectId projectId1 = ProjectId.fromId(randomIdentifier());
        createProject(projectId1.id());

        final ProjectId projectId2 = ProjectId.fromId(randomIdentifier());
        createProject(projectId2.id());

        final String indexPrefix = getTestName().toLowerCase(Locale.ROOT);
        final String indexName = indexPrefix + "-" + randomAlphanumericOfLength(6).toLowerCase(Locale.ROOT);

        createIndex(projectId1, indexName);
        String docId1 = putDocument(projectId1, indexName, "{\"project\": 1 }", true);

        createIndex(projectId2, indexName);
        String docId2a = putDocument(projectId2, indexName, "{\"project\": 2, \"doc\": \"a\" }", false);
        String docId2b = putDocument(projectId2, indexName, "{\"project\": 2, \"doc\": \"b\" }", true);

        List<String> results1 = search(projectId1, indexName);
        assertThat(results1, containsInAnyOrder(docId1));

        List<String> results2 = search(projectId2, indexName);
        assertThat(results2, containsInAnyOrder(docId2a, docId2b));

        final var query = """
            {
             "query": { "term": { "project": 1 } }
            }
            """;
        results1 = getHitIds(search(projectId1, indexPrefix + "-*", query));
        assertThat(results1, containsInAnyOrder(docId1));

        results2 = getHitIds(search(projectId2, indexPrefix + "-*", query));
        assertThat(results2, empty());

        final String aliasName = indexPrefix + "-" + randomIntBetween(100, 999);
        addAlias(projectId1, indexName, aliasName);

        results1 = search(projectId1, aliasName);
        assertThat(results1, containsInAnyOrder(docId1));

        assertIndexNotFound(projectId2, aliasName);

        addAlias(projectId2, indexName, aliasName);
        results2 = search(projectId2, indexName);
        assertThat(results2, containsInAnyOrder(docId2a, docId2b));

        results1 = search(projectId1, indexPrefix + "-*");
        assertThat(results1, containsInAnyOrder(docId1));
    }

    public void testIndexNotVisibleAcrossProjects() throws IOException {
        final ProjectId projectId1 = ProjectId.fromId(randomIdentifier());
        createProject(projectId1.id());

        final ProjectId projectId2 = ProjectId.fromId(randomIdentifier());
        createProject(projectId2.id());

        final String indexPrefix = getTestName().toLowerCase(Locale.ROOT);
        final String indexName = indexPrefix + "-" + randomAlphanumericOfLength(6).toLowerCase(Locale.ROOT);

        createIndex(projectId1, indexName);
        String docId1 = putDocument(projectId1, indexName, "{\"project\": 1 }", true);

        List<String> results1 = search(projectId1, indexName);
        assertThat(results1, containsInAnyOrder(docId1));

        assertIndexNotFound(projectId2, indexName);

        results1 = search(projectId1, indexPrefix + "-*");
        assertThat(results1, containsInAnyOrder(docId1));

        List<String> results2 = search(projectId2, indexPrefix + "-*");
        assertThat(results2, empty());

        results2 = search(projectId2, "");
        assertThat(results2, empty());
    }

    public void testRequestCacheIsNotSharedAcrossProjects() throws IOException {
        final ProjectId projectId1 = ProjectId.fromId(randomIdentifier());
        createProject(projectId1.id());

        final ProjectId projectId2 = ProjectId.fromId(randomIdentifier());
        createProject(projectId2.id());

        final String indexPrefix = getTestName().toLowerCase(Locale.ROOT);
        final String indexName = indexPrefix + "-" + randomAlphanumericOfLength(6).toLowerCase(Locale.ROOT);

        createIndex(projectId1, indexName);
        putDocument(projectId1, indexName, "{\"project\": 1 }", true);

        createIndex(projectId2, indexName);
        putDocument(projectId2, indexName, "{\"project\": 2, \"doc\": \"a\" }", false);
        putDocument(projectId2, indexName, "{\"project\": 2, \"doc\": \"b\" }", false);
        putDocument(projectId2, indexName, "{\"project\": 2, \"doc\": \"c\" }", true);

        final long initialCacheSize = getRequestCacheUsage();

        final var query = """
            {
              "size": 0,
              "aggs": {
                "proj": { "terms": { "field": "project" } }
              }
            }
            """;

        // Perform a search in project 1 that should be cached in shard request cache
        // That is, an aggregation with size:0
        ObjectPath response = search(projectId1, indexName, query);
        String context = "In search response: " + response;
        assertThat(response.evaluateArraySize("aggregations.proj.buckets"), equalTo(1));
        assertThat(response.evaluate("aggregations.proj.buckets.0.key"), equalTo(1));
        assertThat(response.evaluate("aggregations.proj.buckets.0.doc_count"), equalTo(1));

        final long agg1CacheSize = getRequestCacheUsage();
        assertThat("Expected aggregation result to be stored in shard request cache", agg1CacheSize, greaterThan(initialCacheSize));

        // Perform the identical search on project 2 and make sure it returns the right results for the project
        response = search(projectId2, indexName, query);
        context = "In search response: " + response;
        assertThat(context, response.evaluateArraySize("aggregations.proj.buckets"), equalTo(1));
        assertThat(context, response.evaluate("aggregations.proj.buckets.0.key"), equalTo(2));
        assertThat(context, response.evaluate("aggregations.proj.buckets.0.doc_count"), equalTo(3));

        final long agg2CacheSize = getRequestCacheUsage();
        assertThat("Expected aggregation result to be stored in shard request cache", agg2CacheSize, greaterThan(agg1CacheSize));
    }

    private void createIndex(ProjectId projectId, String indexName) throws IOException {
        Request request = new Request("PUT", "/" + indexName);
        setRequestProjectId(request, projectId.id());
        Response response = client().performRequest(request);
        assertOK(response);
    }

    private void addAlias(ProjectId projectId, String indexName, String alias) throws IOException {
        Request request = new Request("POST", "/_aliases");
        request.setJsonEntity(Strings.format("""
            {
              "actions": [
                {
                  "add": {
                    "index": "%s",
                    "alias": "%s"
                  }
                }
              ]
            }
            """, indexName, alias));
        setRequestProjectId(request, projectId.id());
        Response response = client().performRequest(request);
        assertOK(response);
    }

    private String putDocument(ProjectId projectId, String indexName, String body, boolean refresh) throws IOException {
        Request request = new Request("POST", "/" + indexName + "/_doc?refresh=" + refresh);
        request.setJsonEntity(body);
        setRequestProjectId(request, projectId.id());
        Response response = client().performRequest(request);
        assertOK(response);
        return String.valueOf(entityAsMap(response).get("_id"));
    }

    private List<String> search(ProjectId projectId, String indexExpression) throws IOException {
        return getHitIds(search(projectId, indexExpression, null));
    }

    private static ObjectPath search(ProjectId projectId, String indexExpression, String body) throws IOException {
        Request request = new Request("GET", "/" + indexExpression + "/_search");
        if (body != null) {
            request.setJsonEntity(body);
        }
        setRequestProjectId(request, projectId.id());
        Response response = client().performRequest(request);
        assertOK(response);
        return new ObjectPath(entityAsMap(response));
    }

    private void assertIndexNotFound(ProjectId projectId2, String indexName) {
        ResponseException ex = expectThrows(ResponseException.class, () -> search(projectId2, indexName));
        assertThat(ex.getMessage(), containsString("index_not_found"));
        assertThat(ex.getMessage(), containsString(indexName));
    }

    private static List<String> getHitIds(ObjectPath searchResponse) throws IOException {
        List<Map<String, ?>> ids = searchResponse.evaluate("hits.hits");
        return ids.stream().map(o -> String.valueOf(o.get("_id"))).toList();
    }

    private long getRequestCacheUsage() throws IOException {
        final ObjectPath nodeStats = getNodeStats("indices/request_cache");
        return evaluateLong(nodeStats, "indices.request_cache.memory_size_in_bytes");
    }

    private static ObjectPath getNodeStats(String stat) throws IOException {
        Request request = new Request("GET", "/_nodes/stats/" + stat);
        Response response = client().performRequest(request);
        assertOK(response);
        final Map<String, ?> responseMap = entityAsMap(response);

        @SuppressWarnings("unchecked")
        final Map<String, ?> nodes = (Map<String, ?>) responseMap.get("nodes");
        assertThat(nodes, aMapWithSize(1));

        ObjectPath nodeStats = new ObjectPath(nodes.values().iterator().next());
        return nodeStats;
    }

    private static long evaluateLong(ObjectPath nodeStats, String path) throws IOException {
        Object size = nodeStats.evaluate(path);
        assertThat("did not find " + path + " in " + nodeStats, size, notNullValue());
        assertThat("incorrect type for " + path + " in " + nodeStats, size, instanceOf(Number.class));
        return ((Number) size).longValue();
    }

}
