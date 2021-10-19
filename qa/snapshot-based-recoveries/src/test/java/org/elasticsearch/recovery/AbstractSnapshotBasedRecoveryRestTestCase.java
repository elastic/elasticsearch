/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.recovery;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

public abstract class AbstractSnapshotBasedRecoveryRestTestCase extends ESRestTestCase {
    private static final String REPOSITORY_NAME = "repository";
    private static final String SNAPSHOT_NAME = "snapshot-for-recovery";

    protected abstract String repositoryType();

    protected abstract Settings repositorySettings();

    public void testRecoveryUsingSnapshots() throws Exception {
        final String repositoryType = repositoryType();
        Settings repositorySettings = Settings.builder().put(repositorySettings())
            .put(BlobStoreRepository.USE_FOR_PEER_RECOVERY_SETTING.getKey(), true)
            .build();

        registerRepository(REPOSITORY_NAME, repositoryType, true, repositorySettings);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        ensureGreen(indexName);

        final int numDocs = randomIntBetween(500, 1000);
        indexDocs(indexName, numDocs);
        waitUntilGlobalCheckpointIsStable(indexName);
        forceMerge(indexName, randomBoolean(), randomBoolean());

        deleteSnapshot(REPOSITORY_NAME, SNAPSHOT_NAME, true);
        createSnapshot(REPOSITORY_NAME, SNAPSHOT_NAME, true);

        // Add a new replica
        updateIndexSettings(indexName, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));
        ensureGreen(indexName);
        assertSnapshotIsUsed(indexName);

        assertMatchAllReturnsAllDocuments(indexName, numDocs);
        assertMatchQueryReturnsAllDocuments(indexName, numDocs);

        deleteSnapshot(REPOSITORY_NAME, SNAPSHOT_NAME, false);
    }

    private void waitUntilGlobalCheckpointIsStable(String index) throws Exception {
        assertBusy(() -> {
            Request request = new Request(HttpGet.METHOD_NAME, '/' + index + "/_stats?level=shards");
            Response response = client().performRequest(request);
            assertOK(response);
            Map<String, Object> responseAsMap = responseAsMap(response);
            Map<String, Object> indices = extractValue(responseAsMap, "indices");
            Map<String, Object> indexShardsStats = extractValue(extractValue(indices, index), "shards");
            List<Map<String, Object>> shardStats = extractValue(indexShardsStats, "0");
            for (Map<String, Object> shardStat : shardStats) {
                final boolean isPrimary = extractValue(shardStat, "routing.primary");
                if (isPrimary == false) {
                    continue;
                }
                Map<Object, Integer> seqNos = extractValue(shardStat, "seq_no");
                assertThat(seqNos.toString(), seqNos.get("max_seq_no"), is(equalTo(seqNos.get("global_checkpoint"))));
            }
        }, 60, TimeUnit.SECONDS);
    }

    private void assertMatchAllReturnsAllDocuments(String indexName, int numDocs) throws IOException {
        Map<String, Object> searchResults = search(indexName, QueryBuilders.matchAllQuery());
        assertThat(extractValue(searchResults, "hits.total.value"), equalTo(numDocs));
        List<Map<String, Object>> hits = extractValue(searchResults, "hits.hits");
        for (Map<String, Object> hit : hits) {
            String docId = extractValue(hit, "_id");
            assertThat(Integer.parseInt(docId), allOf(greaterThanOrEqualTo(0), lessThan(numDocs)));
            assertThat(extractValue(hit, "_source.field"), equalTo(Integer.parseInt(docId)));
            assertThat(extractValue(hit, "_source.text"), equalTo("Some text " + docId));
        }
    }

    private void assertSnapshotIsUsed(String index) throws Exception {
        Request request = new Request(HttpGet.METHOD_NAME, '/' + index + "/_recovery?detailed=true");
        Response response = client().performRequest(request);
        assertOK(response);
        Map<String, Object> responseAsMap = responseAsMap(response);
        List<Map<String, Object>> shardRecoveries = extractValue(responseAsMap, index + ".shards");
        long totalRecoveredFromSnapshot = 0;
        for (Map<String, Object> shardRecoveryState : shardRecoveries) {
            String recoveryType = extractValue(shardRecoveryState, "type");
            if (recoveryType.equals("PEER") == false) {
                continue;
            }
            String stage = extractValue(shardRecoveryState, "stage");
            assertThat(stage, is(equalTo("DONE")));

            List<Map<String, Object>> fileDetails = extractValue(shardRecoveryState, "index.files.details");
            for (Map<String, Object> fileDetail : fileDetails) {
                int recoveredFromSnapshot = extractValue(fileDetail, "recovered_from_snapshot_in_bytes");
                assertThat(recoveredFromSnapshot, is(greaterThan(0)));
                totalRecoveredFromSnapshot += recoveredFromSnapshot;
            }
        }
        long snapshotSize = getSnapshotSizeForIndex(index);
        assertThat(totalRecoveredFromSnapshot, is(greaterThan(0L)));
        assertThat(totalRecoveredFromSnapshot, is(equalTo(snapshotSize)));
    }

    private int getSnapshotSizeForIndex(String indexName) throws Exception {
        Request request = new Request(HttpGet.METHOD_NAME, "/_snapshot/" + REPOSITORY_NAME + "/" + SNAPSHOT_NAME);
        request.addParameter("index_details", "true");
        Response response = client().performRequest(request);
        assertOK(response);
        Map<String, Object> snapshotsResponse = responseAsMap(response);
        List<Map<String, Object>> snapshots = extractValue(snapshotsResponse, "snapshots");
        assertThat(snapshots.size(), is(equalTo(1)));
        Map<String, Object> snapshot = snapshots.get(0);
        return extractValue(snapshot, "index_details." + indexName + ".size_in_bytes");
    }

    private void assertMatchQueryReturnsAllDocuments(String indexName, int numDocs) throws IOException {
        Map<String, Object> searchResults = search(indexName, QueryBuilders.matchQuery("text", "some"));
        assertThat(extractValue(searchResults, "hits.total.value"), equalTo(numDocs));
    }

    private static void forceMerge(String index, boolean onlyExpungeDeletes, boolean flush) throws IOException {
        final Request request = new Request(HttpPost.METHOD_NAME, '/' + index + "/_forcemerge");
        request.addParameter("only_expunge_deletes", Boolean.toString(onlyExpungeDeletes));
        request.addParameter("flush", Boolean.toString(flush));
        assertOK(client().performRequest(request));
    }

    private void indexDocs(String indexName, int numDocs) throws IOException {
        final StringBuilder bulkBody = new StringBuilder();
        for (int i = 0; i < numDocs; i++) {
            bulkBody.append("{\"index\":{\"_id\":\"").append(i).append("\"}}\n");
            bulkBody.append("{\"field\":").append(i).append(",\"text\":\"Some text ").append(i).append("\"}\n");
        }

        final Request documents = new Request(HttpPost.METHOD_NAME, '/' + indexName + "/_bulk");
        documents.addParameter("refresh", Boolean.TRUE.toString());
        documents.setJsonEntity(bulkBody.toString());
        assertOK(client().performRequest(documents));
    }

    private static Map<String, Object> search(String index, QueryBuilder query) throws IOException {
        final Request request = new Request(HttpPost.METHOD_NAME, '/' + index + "/_search");
        request.setJsonEntity(new SearchSourceBuilder().trackTotalHits(true).query(query).toString());

        final Response response = client().performRequest(request);
        assertOK(response);

        final Map<String, Object> responseAsMap = responseAsMap(response);
        assertThat(
            extractValue(responseAsMap, "_shards.failed"),
            equalTo(0)
        );
        return responseAsMap;
    }

    @SuppressWarnings("unchecked")
    private static <T> T extractValue(Map<String, Object> map, String path) {
        return (T) XContentMapValues.extractValue(path, map);
    }
}
