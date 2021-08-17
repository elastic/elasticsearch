/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.rest.action.document.RestBulkAction;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.routing.UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY;
import static org.elasticsearch.upgrades.AbstractRollingTestCase.ClusterType.MIXED;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

public class SnapshotBasedRecoveryIT extends AbstractRollingTestCase {
    public void testSnapshotBasedRecovery() throws Exception {
        final String indexName = "snapshot_based_recovery";
        final String repositoryName = "snapshot_based_recovery_repo";
        final int numDocs = 200;
        switch (CLUSTER_TYPE) {
            case OLD:
                Settings.Builder settings = Settings.builder()
                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                    .put(INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "100ms")
                    .put(SETTING_ALLOCATION_MAX_RETRY.getKey(), "0"); // fail faster
                createIndex(indexName, settings.build());
                ensureGreen(indexName);
                indexDocs(indexName, numDocs);
                flush(indexName, true);

                registerRepository(
                    repositoryName,
                    "fs",
                    true,
                    Settings.builder()
                        .put("location", "./snapshot_based_recovery")
                        .put(BlobStoreRepository.USE_FOR_PEER_RECOVERY_SETTING.getKey(), true)
                        .build()
                );

                createSnapshot(repositoryName, "snap", true);

                updateIndexSettings(indexName, Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1));
                ensureGreen(indexName);
                break;
            case MIXED:
            case UPGRADED:
                // the following `if` for first round mixed was added as a selective test mute. Sometimes the primary shard ends
                // on the upgraded node. This causes issues when removing and adding replicas, since then we cannot allocate to
                // any of the old nodes. That is an issue only for the first mixed round, hence this check.
                // Ideally we would find the reason the primary ends on the upgraded node and fix that (or figure out that it
                // is all good).
                // @AwaitsFix(bugUrl = https://github.com/elastic/elasticsearch/issues/76595)
                if (CLUSTER_TYPE != MIXED || FIRST_MIXED_ROUND == false) {
                    // Drop replicas
                    updateIndexSettings(indexName, Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0));
                }
                ensureGreen(indexName);

                updateIndexSettings(indexName, Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1));
                ensureGreen(indexName);
                assertMatchAllReturnsAllDocuments(indexName, numDocs);
                assertMatchQueryReturnsAllDocuments(indexName, numDocs);
                break;
            default:
                throw new IllegalStateException("unknown type " + CLUSTER_TYPE);
        }
    }

    private void assertMatchAllReturnsAllDocuments(String indexName, int numDocs) throws IOException {
        Map<String, Object> searchResults = search(indexName, QueryBuilders.matchAllQuery());
        List<Map<String, Object>> hits = extractValue(searchResults, "hits.hits");
        assertThat(hits.size(), equalTo(numDocs));
        for (Map<String, Object> hit : hits) {
            String docId = extractValue(hit, "_id");
            assertThat(Integer.parseInt(docId), allOf(greaterThanOrEqualTo(0), lessThan(numDocs)));
            assertThat(extractValue(hit, "_source.field"), equalTo(Integer.parseInt(docId)));
            assertThat(extractValue(hit, "_source.text"), equalTo("Some text " + docId));
        }
    }

    private void assertMatchQueryReturnsAllDocuments(String indexName, int numDocs) throws IOException {
        Map<String, Object> searchResults = search(indexName, QueryBuilders.matchQuery("text", "some"));
        List<Map<String, Object>> hits = extractValue(searchResults, "hits.hits");
        assertThat(hits.size(), equalTo(numDocs));
        for (Map<String, Object> hit : hits) {
            String docId = extractValue(hit, "_id");
            assertThat(Integer.parseInt(docId), allOf(greaterThanOrEqualTo(0), lessThan(numDocs)));
            assertThat(extractValue(hit, "_source.field"), equalTo(Integer.parseInt(docId)));
            assertThat(extractValue(hit, "_source.text"), equalTo("Some text " + docId));
        }
    }

    private static Map<String, Object> search(String index, QueryBuilder query) throws IOException {
        final Request request = new Request(HttpPost.METHOD_NAME, '/' + index + "/_search");
        request.setJsonEntity(new SearchSourceBuilder().size(1000).query(query).toString());

        final Response response = client().performRequest(request);
        assertOK(response);

        final Map<String, Object> responseAsMap = responseAsMap(response);
        assertThat(
            extractValue(responseAsMap, "_shards.failed"),
            equalTo(0)
        );
        return responseAsMap;
    }

    private void indexDocs(String indexName, int numDocs) throws IOException {
        final StringBuilder bulkBody = new StringBuilder();
        for (int i = 0; i < numDocs; i++) {
            bulkBody.append("{\"index\":{\"_id\":\"").append(i).append("\", \"_type\": \"_doc\"}}\n");
            bulkBody.append("{\"field\":").append(i).append(",\"text\":\"Some text ").append(i).append("\"}\n");
        }

        final Request documents = new Request(HttpPost.METHOD_NAME, '/' + indexName + "/_bulk");
        documents.addParameter("refresh", "true");
        documents.setOptions(expectWarnings(RestBulkAction.TYPES_DEPRECATION_MESSAGE));
        documents.setJsonEntity(bulkBody.toString());
        assertOK(client().performRequest(documents));
    }

    @SuppressWarnings("unchecked")
    private static <T> T extractValue(Map<String, Object> map, String path) {
        return (T) XContentMapValues.extractValue(path, map);
    }
}
