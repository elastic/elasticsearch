/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 1)
public class BatchBulkIT extends ESIntegTestCase {

    private static final String MAPPING = """
        {
          "_doc": {
            "dynamic": "strict",
            "_source": {
              "mode": "synthetic"
            },
            "properties": {
              "name": { "type": "keyword" },
              "value": { "type": "long" },
              "message": { "type": "text", "store": true }
            }
          }
        }""";

    private void createBatchIndex(String index, int shards, int replicas) {
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", shards)
                        .put("index.number_of_replicas", replicas)
                        .put(IndexSettings.COLUMN_BATCH_INDEX.getKey(), true)
                )
                .setMapping(MAPPING)
        );
        ensureGreen(index);
    }

    private String findCoordinatingNode() {
        for (String nodeName : internalCluster().getNodeNames()) {
            if (internalCluster().clusterService(nodeName).localNode().canContainData() == false
                && internalCluster().clusterService(nodeName).localNode().isMasterNode() == false) {
                return nodeName;
            }
        }
        return internalCluster().getNodeNames()[internalCluster().getNodeNames().length - 1];
    }

    public void testBulkIndexingViaBatchMode() {
        String index = "test-batch";
        createBatchIndex(index, 2, 1);
        String coordinatingNode = findCoordinatingNode();

        int numDocs = randomIntBetween(20, 100);
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            IndexRequest indexRequest = new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE)
                .source(Map.of("name", "doc-" + i, "value", i, "message", "hello world " + i));
            bulkRequest.add(indexRequest);
        }

        BulkResponse bulkResponse = client(coordinatingNode).bulk(bulkRequest).actionGet();
        assertNoFailures(bulkResponse);
        assertThat(bulkResponse.getItems().length, equalTo(numDocs));

        refresh(index);

        assertResponse(prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).setSize(0).setTrackTotalHits(true), searchResponse -> {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) numDocs));
        });
    }

    public void testSyntheticSourceReconstruction() {
        String index = "test-batch-synthetic";
        createBatchIndex(index, 2, 1);
        String coordinatingNode = findCoordinatingNode();

        int numDocs = 20;
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(
                new IndexRequest(index).id("id-" + i)
                    .opType(DocWriteRequest.OpType.CREATE)
                    .source(Map.of("name", "synth-" + i, "value", i, "message", "synthetic source test " + i))
            );
        }

        BulkResponse bulkResponse = client(coordinatingNode).bulk(bulkRequest).actionGet();
        assertNoFailures(bulkResponse);

        refresh(index);

        // Fetch all docs sorted by value and verify synthetic source is reconstructed correctly
        assertResponse(
            prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                .setSize(numDocs)
                .addSort("value", SortOrder.ASC)
                .setTrackTotalHits(true),
            searchResponse -> {
                assertNoFailures(searchResponse);
                assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) numDocs));
                SearchHit[] hits = searchResponse.getHits().getHits();
                for (int i = 0; i < numDocs; i++) {
                    Map<String, Object> source = hits[i].getSourceAsMap();
                    assertThat("name mismatch at doc " + i, source.get("name"), equalTo("synth-" + i));
                    assertThat("value mismatch at doc " + i, source.get("value"), equalTo(i));
                    assertThat("message mismatch at doc " + i, source.get("message"), equalTo("synthetic source test " + i));
                }
            }
        );

        // Also verify via GET by id
        var getResponse = client().get(new org.elasticsearch.action.get.GetRequest(index).id("id-5")).actionGet();
        assertTrue(getResponse.isExists());
        assertThat(getResponse.getSourceAsMap().get("name"), equalTo("synth-5"));
        assertThat(getResponse.getSourceAsMap().get("value"), equalTo(5));
        assertThat(getResponse.getSourceAsMap().get("message"), equalTo("synthetic source test 5"));
    }

    public void testMultipleBulkRequests() {
        String index = "test-multi-batch";
        createBatchIndex(index, 2, 1);
        String coordinatingNode = findCoordinatingNode();

        int totalDocs = 0;
        int numBulks = randomIntBetween(3, 6);
        for (int b = 0; b < numBulks; b++) {
            int numDocs = randomIntBetween(10, 50);
            BulkRequest bulkRequest = new BulkRequest();
            for (int i = 0; i < numDocs; i++) {
                IndexRequest indexRequest = new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE)
                    .source(Map.of("name", "bulk" + b + "-doc-" + i, "value", totalDocs + i, "message", "batch " + b));
                bulkRequest.add(indexRequest);
            }

            BulkResponse bulkResponse = client(coordinatingNode).bulk(bulkRequest).actionGet();
            assertNoFailures(bulkResponse);
            assertThat(bulkResponse.getItems().length, equalTo(numDocs));
            totalDocs += numDocs;
        }

        refresh(index);

        int expectedTotal = totalDocs;
        assertResponse(prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).setSize(0).setTrackTotalHits(true), searchResponse -> {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) expectedTotal));
        });
    }

    public void testBulkWithExplicitIds() {
        String index = "test-batch-ids";
        createBatchIndex(index, 2, 1);
        String coordinatingNode = findCoordinatingNode();

        int numDocs = randomIntBetween(20, 80);
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            IndexRequest indexRequest = new IndexRequest(index).id("doc-" + i)
                .opType(DocWriteRequest.OpType.CREATE)
                .source(Map.of("name", "explicit-" + i, "value", i, "message", "explicit id test"));
            bulkRequest.add(indexRequest);
        }

        BulkResponse bulkResponse = client(coordinatingNode).bulk(bulkRequest).actionGet();
        assertNoFailures(bulkResponse);

        refresh(index);

        assertResponse(prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).setSize(0).setTrackTotalHits(true), searchResponse -> {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) numDocs));
        });

        // Verify specific document is retrievable by id
        var getResponse = client().get(new org.elasticsearch.action.get.GetRequest(index).id("doc-0")).actionGet();
        assertTrue(getResponse.isExists());
        assertThat(getResponse.getSourceAsMap().get("name"), equalTo("explicit-0"));
    }

    @AwaitsFix(bugUrl = "Not propogating exception properly")
    public void testBatchModeWithParseFailures() {
        String index = "test-batch-failures";
        createBatchIndex(index, 2, 1);
        String coordinatingNode = findCoordinatingNode();

        // With strict mappings, sending a document with an unknown field should fail
        BulkRequest bulkRequest = new BulkRequest();

        // Valid documents
        for (int i = 0; i < 5; i++) {
            bulkRequest.add(
                new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE)
                    .source(Map.of("name", "valid-" + i, "value", i, "message", "valid doc"))
            );
        }

        // Invalid document (unknown field with strict mapping)
        bulkRequest.add(
            new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE)
                .source("{\"name\": \"bad\", \"value\": 99, \"unknown_field\": \"oops\"}", XContentType.JSON)
        );

        // More valid documents
        for (int i = 5; i < 10; i++) {
            bulkRequest.add(
                new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE)
                    .source(Map.of("name", "valid-" + i, "value", i, "message", "valid doc"))
            );
        }

        BulkResponse bulkResponse = client(coordinatingNode).bulk(bulkRequest).actionGet();
        assertTrue(bulkResponse.hasFailures());

        // Count successes and failures
        int successes = 0;
        int failures = 0;
        for (BulkItemResponse item : bulkResponse.getItems()) {
            if (item.isFailed()) {
                failures++;
            } else {
                successes++;
            }
        }
        assertThat(successes, equalTo(10));
        assertThat(failures, equalTo(1));

        refresh(index);

        assertResponse(prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).setSize(0).setTrackTotalHits(true), searchResponse -> {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(10L));
        });
    }

    public void testBatchModeNotEnabledWithoutSetting() {
        // Create an index without the column_batch_index setting
        String index = "test-no-batch";
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 1))
                .setMapping(MAPPING)
        );
        ensureGreen(index);
        String coordinatingNode = findCoordinatingNode();

        // Indexing should still work via the serial path
        int numDocs = randomIntBetween(10, 50);
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(
                new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE)
                    .source(Map.of("name", "serial-" + i, "value", i, "message", "serial path"))
            );
        }

        BulkResponse bulkResponse = client(coordinatingNode).bulk(bulkRequest).actionGet();
        assertNoFailures(bulkResponse);

        refresh(index);

        assertResponse(prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).setSize(0).setTrackTotalHits(true), searchResponse -> {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) numDocs));
        });
    }

    public void testLargeBatchAcrossShards() {
        String index = "test-large-batch";
        createBatchIndex(index, 3, 1);
        String coordinatingNode = findCoordinatingNode();

        int numDocs = randomIntBetween(200, 500);
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(
                new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE)
                    .source(Map.of("name", "large-" + i, "value", i, "message", randomAlphaOfLength(50)))
            );
        }

        BulkResponse bulkResponse = client(coordinatingNode).bulk(bulkRequest).actionGet();
        assertNoFailures(bulkResponse);
        assertThat(bulkResponse.getItems().length, equalTo(numDocs));

        refresh(index);

        assertResponse(prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).setSize(0).setTrackTotalHits(true), searchResponse -> {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) numDocs));
        });

        // Verify data integrity with a filtered search and source reconstruction
        assertResponse(
            prepareSearch(index).setQuery(QueryBuilders.termQuery("name", "large-0")).setTrackTotalHits(true),
            searchResponse -> {
                assertNoFailures(searchResponse);
                assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(1L));
                Map<String, Object> sourceAsMap = searchResponse.getHits().getHits()[0].getSourceAsMap();
                assertThat(sourceAsMap.get("value"), equalTo(0));
                assertThat(sourceAsMap.get("name"), equalTo("large-0"));
            }
        );
    }
}
