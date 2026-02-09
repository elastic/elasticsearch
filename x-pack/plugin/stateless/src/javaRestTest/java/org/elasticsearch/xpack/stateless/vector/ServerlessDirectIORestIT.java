/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.vector;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * We don't support direct IO in Serverless, but we run a test that exercises the code paths to ensure that they operate without issue.
 * It should always be possible to create an index using BBQ and index some docs, regardless of direct IO.
 *
 * The test trivially creates an index using BBQ and ingests a number of docs containing vectors. This is sufficient to verify that
 * the directory implementations do not try to access the raw vectors `.vec` file (using direct IO) when it may be within the blob
 * cache (rather than an individual file in the file-system).
 */
public class ServerlessDirectIORestIT extends ESRestTestCase {

    @ClassRule
    public static ServerlessElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .user("admin-user", "x-pack-test-password")
        .setting("xpack.ml.enabled", "false")
        .build();

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin-user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testBasicIndexAndSearchVectors() throws Exception {
        var indexName = "foo-index-" + randomIdentifier();
        createIndexWithVectorMapping(indexName);
        int numDocs = between(50, 100);
        indexVectors(indexName, numDocs);
        ensureGreen(indexName);
        flush(indexName, false); // upload/commit the indexed docs

        // restarting the index node results in retrieving the commited docs from the cache
        cluster.restartNodeInPlace(0, true);
        assertThat(getNodesInfo(client()).keySet(), hasSize(2));
        ensureGreen(indexName);

        // first search - match_all
        var searchRequest = new Request("POST", "/" + indexName + "/_search");
        searchRequest.setJsonEntity("""
            {
              "query": {
                "match_all": {}
              }
            }
            """);
        assertHitCount(searchRequest, numDocs);

        // second search - knn
        searchRequest = new Request("POST", "/" + indexName + "/_search");
        var randomVector = Arrays.toString(IntStream.range(0, 64).mapToDouble(d -> randomFloat()).toArray());
        searchRequest.setJsonEntity("""
            {
              "knn": {
                   "field": "fooVector",
                   "query_vector": %randomVector%,
                   "k": 10,
                   "num_candidates": 20
                 }
            }
            """.replace("%randomVector%", randomVector));
        assertHitCount(searchRequest, 10);
    }

    void createIndexWithVectorMapping(String indexName) throws IOException {
        String type = randomFrom("bbq_flat", "bbq_hnsw");
        String mapping = """
            {
              "properties": {
                "fooVector": {
                  "type": "dense_vector",
                  "dims": 64,
                  "element_type": "float",
                  "index": true,
                  "similarity": "l2_norm",
                  "index_options": {
                    "type": "%type%"
                  }
                }
              }
            }
            """.replace("%type%", type);
        var settings = indexSettings(1, 1);
        // setting ensures that segments created by merge do not use compound files
        settings.put("index.compound_format", false);
        var response = createIndex(indexName, settings.build(), mapping);
        assertAcked(response);
        ensureGreen(indexName);
        assertBBQIndexType(indexName, type);
    }

    void indexVectors(String indexName, int numDocs) throws IOException {
        StringBuilder body = new StringBuilder();
        int mid = numDocs / 2; // we flush after half the docs in order to create more than one segment
        for (int i = 0; i < numDocs; i++) {
            var randomVector = Arrays.toString(IntStream.range(0, 64).mapToDouble(d -> randomFloat()).toArray());
            String s = """
                  { "index": { "_index": "%index%", "_id": %id% } }
                  { "fooVector": %randomVector% }
                """.replace("%index%", indexName).replace("%id%", Integer.toString(i)).replace("%randomVector%", randomVector);
            body.append(s);
            if (i == mid || i == (numDocs - 1)) {
                bulkIndex(body.toString());
                body = new StringBuilder();
                flush(indexName, true);
            }
        }

        // force merge to a single segment in order to avoid a compound file
        var forceMergeRequest = new Request("POST", "/" + indexName + "/_forcemerge");
        forceMergeRequest.addParameter("max_num_segments", "1");
        var forceMergeResponse = client().performRequest(forceMergeRequest);
        assertOK(forceMergeResponse);
    }

    void bulkIndex(String bulk) throws IOException {
        Request bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.setJsonEntity(bulk);
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.addParameter("pretty", null);
        String bulkResponse = EntityUtils.toString(client().performRequest(bulkRequest).getEntity());
        assertThat(bulkResponse, not(containsString("\"errors\": false")));
    }

    static void assertBBQIndexType(String indexName, String type) throws IOException {
        var mappingsRequest = new Request("GET", "/" + indexName + "/_mappings");
        var response = client().performRequest(mappingsRequest);
        assertOK(response);
        var responseBody = responseAsMap(response);
        var s = (String) XContentMapValues.extractValue(indexName + ".mappings.properties.fooVector.index_options.type", responseBody);
        assertThat(s, equalTo(type));
    }

    void assertHitCount(Request searchRequest, int hitCount) throws IOException {
        Response searchResponse = client().performRequest(searchRequest);
        assertOK(searchResponse);
        var searchResponseBody = responseAsMap(searchResponse);
        int totalHits = (int) XContentMapValues.extractValue("hits.total.value", searchResponseBody);
        assertThat(totalHits, equalTo(hitCount));

        var shardsHeader = (Map<?, ?>) searchResponseBody.get("_shards");
        assertThat(shardsHeader.get("failed"), equalTo(0));
        assertThat(shardsHeader.get("successful"), equalTo(1));
        assertThat(shardsHeader.get("skipped"), equalTo(0));
    }
}
