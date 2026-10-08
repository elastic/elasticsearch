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
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;

/**
 * Cross-cluster {@code semantic_text} search against an index that lives only on the remote cluster, with the two
 * clusters on different versions. The base {@code javaRestTest} task is disabled by the bwc-test plugin, so this runs
 * under the {@code #newToOld} and {@code #oldToNew} pairings, which is what it is for. Same-version coverage lives in
 * {@code CrossClusterSemanticTextIT} and in the csv-spec suite.
 * <p>
 * The csv-spec suite cannot cover the mixed-version case. It backs inference with the {@code inference-service-test}
 * plugin, which is built for the current version only and cannot be installed on a BWC node
 * (<a href="https://github.com/elastic/elasticsearch/issues/115166">#115166</a>), so {@code semantic_text} is disabled
 * there whenever the two clusters differ.
 * <p>
 * This suite avoids the plugin entirely: it points a built-in {@code openai} inference endpoint at a
 * {@link MockWebServer}, so only shipped code runs on the nodes and the remote can host the endpoint whatever its
 * version. The same approach is used by {@code x-pack/plugin/inference/qa/mixed-cluster}.
 * <p>
 * The index lives only on the remote, so the remote is the cluster that embeds both the documents and the query,
 * using its own {@code search_inference_id}. That is the path this is here to exercise.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class RemoteSemanticTextIT extends ESRestTestCase {

    /**
     * Querying a {@code semantic_text} field across clusters needs {@code get_inference_fields_action_as_indices_action}
     * on every cluster, which landed in 9.4.0 and in a 9.3 patch. Older remotes fail by design with an error naming the
     * required version; that boundary is not asserted here because it depends on which 9.3 patch carries the backport.
     */
    private static final Version SEMANTIC_CCS_MIN_VERSION = Version.V_9_4_0;

    private static final String INDEX = "semantic-remote";
    private static final String INFERENCE_ID = "mock-text-embedding";

    private static MockWebServer embeddingServer;

    static ElasticsearchCluster remoteCluster = Clusters.remoteCluster();
    static ElasticsearchCluster localCluster = Clusters.localCluster(remoteCluster);

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);

    @BeforeClass
    public static void startEmbeddingServer() throws IOException {
        embeddingServer = new MockWebServer();
        embeddingServer.start();
    }

    @AfterClass
    public static void stopEmbeddingServer() {
        if (embeddingServer != null) {
            embeddingServer.close();
            embeddingServer = null;
        }
    }

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    @Before
    public void setUpRemoteSemanticIndex() throws IOException {
        assumeTrue(
            "semantic_text over CCS requires " + SEMANTIC_CCS_MIN_VERSION + " or later on every cluster",
            Clusters.remoteClusterVersion().onOrAfter(SEMANTIC_CCS_MIN_VERSION)
                && Clusters.localClusterVersion().onOrAfter(SEMANTIC_CCS_MIN_VERSION)
        );

        try (RestClient remoteClient = remoteClusterClient()) {
            if (indexExists(remoteClient, INDEX)) {
                return;
            }
            createInferenceEndpoint(remoteClient);
            createSemanticIndex(remoteClient);
            indexDoc(remoteClient, "1", "the quick brown fox");
            indexDoc(remoteClient, "2", "a lazy dog sleeps");
            refresh(remoteClient, INDEX);
        }
    }

    public void testMatchOnSemanticTextOnRemoteOfDifferentVersion() throws IOException {
        // The mock returns the same embedding for every input, so every document matches; the assertion is about
        // the query reaching the remote and coming back, not about relevance.
        enqueueEmbeddingResponse();
        List<String> ids = queryIds("FROM " + REMOTE + " | WHERE body : \"quick\" | KEEP id | SORT id | LIMIT 10");
        assertThat(ids, containsInAnyOrder("1", "2"));
    }

    public void testMatchOnSemanticTextCombinedWithPushedDownFilter() throws IOException {
        enqueueEmbeddingResponse();
        List<String> ids = queryIds("FROM " + REMOTE + " | WHERE body : \"quick\" AND host == \"host-1\" | KEEP id | LIMIT 10");
        assertThat(ids, containsInAnyOrder("1"));
    }

    public void testScoreIsPopulatedForRemoteSemanticMatch() throws IOException {
        enqueueEmbeddingResponse();
        Map<String, Object> response = runEsql(
            "FROM " + REMOTE + " METADATA _score | WHERE body : \"quick\" | KEEP id, _score | SORT id | LIMIT 10"
        );
        List<?> values = (List<?>) response.get("values");
        assertThat(values, hasSize(2));
        for (Object row : values) {
            Double score = (Double) ((List<?>) row).get(1);
            assertNotNull("no score returned", score);
            assertTrue("expected a positive score but got " + score, score > 0.0);
        }
    }

    private static final String REMOTE = Clusters.REMOTE_CLUSTER_NAME + ":" + INDEX;

    @SuppressWarnings("unchecked")
    private List<String> queryIds(String query) throws IOException {
        List<?> values = (List<?>) runEsql(query).get("values");
        return values.stream().map(row -> (String) ((List<Object>) row).getFirst()).toList();
    }

    private Map<String, Object> runEsql(String query) throws IOException {
        Request request = new Request("POST", "/_query");
        try (XContentBuilder body = JsonXContent.contentBuilder()) {
            body.startObject().field("query", query).endObject();
            request.setJsonEntity(Strings.toString(body));
        }
        Response response = client().performRequest(request);
        return entityAsMap(response);
    }

    /**
     * {@link MockWebServer} serves one queued response per request and has no dispatcher, so every inference call has
     * to be queued up front.
     */
    private static void enqueueEmbeddingResponse() {
        embeddingServer.enqueue(new MockResponse().setResponseCode(200).setBody("""
            {
              "object": "list",
              "data": [
                { "object": "embedding", "index": 0, "embedding": [0.0123, -0.0123] }
              ],
              "model": "text-embedding-ada-002",
              "usage": { "prompt_tokens": 8, "total_tokens": 8 }
            }
            """));
    }

    private static void createInferenceEndpoint(RestClient remoteClient) throws IOException {
        // Creating the endpoint validates it against the service, which costs one queued response.
        enqueueEmbeddingResponse();
        Request request = new Request("PUT", "/_inference/text_embedding/" + INFERENCE_ID);
        request.setJsonEntity(Strings.format("""
            {
              "service": "openai",
              "service_settings": {
                "api_key": "test-key",
                "url": "%s",
                "model_id": "text-embedding-ada-002",
                "similarity": "cosine"
              }
            }
            """, Strings.format("http://%s:%s", embeddingServer.getHostName(), embeddingServer.getPort())));
        remoteClient.performRequest(request);
    }

    private static void createSemanticIndex(RestClient remoteClient) throws IOException {
        Request request = new Request("PUT", "/" + INDEX);
        request.setJsonEntity(Strings.format("""
            {
              "mappings": {
                "properties": {
                  "id": { "type": "keyword" },
                  "host": { "type": "keyword" },
                  "body": { "type": "semantic_text", "inference_id": "%s" }
                }
              }
            }
            """, INFERENCE_ID));
        remoteClient.performRequest(request);
    }

    private static void indexDoc(RestClient remoteClient, String id, String body) throws IOException {
        // One document per request so that each triggers a single-input inference call.
        enqueueEmbeddingResponse();
        Request request = new Request("PUT", "/" + INDEX + "/_doc/" + id);
        request.setJsonEntity(Strings.format("""
            {"id": "%s", "host": "host-%s", "body": "%s"}
            """, id, id, body));
        remoteClient.performRequest(request);
    }

    private RestClient remoteClusterClient() throws IOException {
        HttpHost[] hosts = parseClusterHosts(remoteCluster.getHttpAddresses()).toArray(HttpHost[]::new);
        return buildClient(restAdminSettings(), hosts);
    }

}
