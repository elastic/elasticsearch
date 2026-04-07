/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.Build;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2, numClientNodes = 1)
public class BatchBulkIT extends ESIntegTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        assumeTrue("batch indexing requires snapshot builds", Build.current().isSnapshot());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ShardBatchIndexer.BATCH_INDEXING.getKey(), true)
            .build();
    }

    private void createBatchIndex(String index, int shards, int replicas) throws IOException {
        XContentBuilder mapping = getMapping();
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", shards)
                        .put("index.number_of_replicas", replicas)
                        .put("index.mapping.source.mode", "synthetic")

                )
                .setMapping(mapping)
        );
        ensureGreen(index);
    }

    private static XContentBuilder getMapping() throws IOException {
        XContentBuilder mapping = JsonXContent.contentBuilder();
        mapping.startObject();
        {
            // Wrap everything in _doc
            mapping.startObject("_doc");
            {
                mapping.startObject("_source");
                mapping.field("mode", "synthetic");
                mapping.endObject();

                // Set Dynamic Mapping to Strict
                mapping.field("dynamic", "strict");

                // Define Properties
                mapping.startObject("properties");
                {
                    mapping.startObject("name").field("type", "keyword").endObject();
                    mapping.startObject("value").field("type", "long").endObject();
                    mapping.startObject("message").field("type", "text").field("store", true).endObject();
                }
                mapping.endObject();
            }
            mapping.endObject(); // end _doc
        }
        mapping.endObject();
        return mapping;
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

    public void testBulkIndexingViaBatchMode() throws IOException {
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

    public void testSyntheticSourceReconstruction() throws IOException {
        String index = "test-batch-synthetic";
        createBatchIndex(index, 1, 0);
        String coordinatingNode = findCoordinatingNode();

        logger.info(
            "ACTUAL MAPPING: {}",
            client().admin().indices().prepareGetMappings(TimeValue.MAX_VALUE, index).get().getMappings().get(index).source().toString()
        );

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
                    logger.info("Doc {}: hasSource={}, source={}", i, hits[i].hasSource(), source);
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

    public void testMultipleBulkRequests() throws IOException {
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

    public void testBulkWithExplicitIds() throws IOException {
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

    public void testBatchModeWithParseFailures() throws IOException {
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

    public void testFallbackToItemByItemWithMixedOps() throws IOException {
        // When the bulk request contains deletes or updates, the batch path cannot be used
        // and it should fall back to the item-by-item path
        String index = "test-mixed-ops";
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 1))
                .setMapping(getMapping())
        );
        ensureGreen(index);
        String coordinatingNode = findCoordinatingNode();

        // First, index some documents
        int numDocs = 10;
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(
                new IndexRequest(index).id("doc-" + i)
                    .opType(DocWriteRequest.OpType.INDEX)
                    .source(Map.of("name", "serial-" + i, "value", i, "message", "serial path"))
            );
        }

        BulkResponse bulkResponse = client(coordinatingNode).bulk(bulkRequest).actionGet();
        assertNoFailures(bulkResponse);

        // Now send a mixed bulk with indexes and deletes — this forces fallback to item-by-item
        BulkRequest mixedRequest = new BulkRequest();
        for (int i = 0; i < 5; i++) {
            mixedRequest.add(
                new IndexRequest(index).id("new-doc-" + i)
                    .opType(DocWriteRequest.OpType.INDEX)
                    .source(Map.of("name", "new-" + i, "value", 100 + i, "message", "new doc"))
            );
        }
        mixedRequest.add(new DeleteRequest(index, "doc-0"));
        mixedRequest.add(new DeleteRequest(index, "doc-1"));

        BulkResponse mixedResponse = client(coordinatingNode).bulk(mixedRequest).actionGet();
        assertNoFailures(mixedResponse);

        refresh(index);

        // 10 original - 2 deleted + 5 new = 13
        assertResponse(prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).setSize(0).setTrackTotalHits(true), searchResponse -> {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(13L));
        });
    }

    public void testLargeBatchAcrossShards() throws IOException {
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

    public void testRealtimeGetFromTranslog() throws IOException {
        String index = "test-batch-realtime-get";
        createBatchIndex(index, 1, 0);
        String coordinatingNode = findCoordinatingNode();

        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < 5; i++) {
            bulkRequest.add(
                new IndexRequest(index).id("rtget-" + i)
                    .opType(DocWriteRequest.OpType.CREATE)
                    .source(Map.of("name", "realtime-" + i, "value", i, "message", "realtime get test"))
            );
        }

        BulkResponse bulkResponse = client(coordinatingNode).bulk(bulkRequest).actionGet();
        assertNoFailures(bulkResponse);

        // GET immediately after bulk index, before refresh — should read from translog
        var getResponse = client().get(new org.elasticsearch.action.get.GetRequest(index).id("rtget-2").realtime(true)).actionGet();
        assertTrue("Document should exist via realtime GET", getResponse.isExists());
        assertThat(getResponse.getSourceAsMap().get("name"), equalTo("realtime-2"));
        assertThat(getResponse.getSourceAsMap().get("value"), equalTo(2));
    }

    @SuppressWarnings("unchecked")
    public void testDynamicMappingsViaBatchMode() throws IOException {
        String index = "test-batch-dynamic";
        // Create index with dynamic:true (default)
        // Only map @timestamp — everything else needs dynamic mapping.
        XContentBuilder mapping = JsonXContent.contentBuilder();
        mapping.startObject();
        {
            mapping.startObject("_doc");
            {
                mapping.startObject("properties");
                {
                    mapping.startObject("@timestamp").field("type", "date").endObject();
                }
                mapping.endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();

        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.mapping.source.mode", "synthetic")
                )
                .setMapping(mapping)
        );
        ensureGreen(index);

        String coordinatingNode = findCoordinatingNode();
        int docsPerBulk = 5;

        // First bulk: triggers dynamic mapping creation (serial path)
        BulkRequest firstBulk = new BulkRequest();
        for (int i = 0; i < docsPerBulk; i++) {
            firstBulk.add(new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE).source(buildDynamicDoc(i)));
        }
        BulkResponse firstResponse = client(coordinatingNode).bulk(firstBulk).actionGet();
        assertNoFailures(firstResponse);
        assertThat(firstResponse.getItems().length, equalTo(docsPerBulk));

        // Verify key mappings exist after first bulk
        var mappingsResponse = client().admin().indices().prepareGetMappings(TimeValue.MAX_VALUE, index).get();
        Map<String, Object> mappingMap = mappingsResponse.getMappings().get(index).sourceAsMap();
        Map<String, Object> properties = (Map<String, Object>) mappingMap.get("properties");
        assertNotNull("kubernetes should be mapped", properties.get("kubernetes"));
        assertNotNull("event should be mapped", properties.get("event"));
        assertNotNull("@timestamp should be mapped", properties.get("@timestamp"));

        // Second bulk: mappings already established, so this uses batch mode
        BulkRequest secondBulk = new BulkRequest();
        for (int i = docsPerBulk; i < docsPerBulk * 2; i++) {
            secondBulk.add(new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE).source(buildDynamicDoc(i)));
        }
        BulkResponse secondResponse = client(coordinatingNode).bulk(secondBulk).actionGet();
        assertNoFailures(secondResponse);
        assertThat(secondResponse.getItems().length, equalTo(docsPerBulk));

        refresh(index);

        int totalDocs = docsPerBulk * 2;

        // Verify correct count
        assertResponse(prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).setSize(0).setTrackTotalHits(true), searchResponse -> {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) totalDocs));
        });

        // Verify source reconstruction across both bulks
        assertResponse(
            prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                .setSize(totalDocs)
                .addSort("kubernetes.namespace.keyword", SortOrder.ASC)
                .setTrackTotalHits(true),
            searchResponse -> {
                assertNoFailures(searchResponse);
                SearchHit[] hits = searchResponse.getHits().getHits();
                for (int i = 0; i < totalDocs; i++) {
                    Map<String, Object> source = hits[i].getSourceAsMap();
                    Map<String, Object> kubernetes = (Map<String, Object>) source.get("kubernetes");
                    assertThat("namespace mismatch at doc " + i, kubernetes.get("namespace"), equalTo("namespace" + i));
                    Map<String, Object> event = (Map<String, Object>) source.get("event");
                    assertThat("event.dataset mismatch at doc " + i, event.get("dataset"), equalTo("kubernetes.volume"));
                }
            }
        );
    }

    private XContentBuilder buildDynamicDoc(int i) throws IOException {
        XContentBuilder doc = JsonXContent.contentBuilder();
        doc.startObject();
        {
            doc.field("@timestamp", "2021-04-28T19:45:28.222Z");
            doc.startObject("kubernetes");
            {
                doc.field("namespace", "namespace" + i);
                doc.startObject("node");
                doc.field("name", "gke-apps-node-name-" + i);
                doc.endObject();
                doc.startObject("pod");
                doc.field("name", "pod-name-pod-name-" + i);
                doc.endObject();
                doc.startObject("volume");
                {
                    doc.field("name", "volume-" + i);
                    doc.startObject("fs");
                    {
                        doc.startObject("capacity");
                        doc.field("bytes", 7883960320L);
                        doc.endObject();
                        doc.startObject("used");
                        doc.field("bytes", 12288 + i);
                        doc.endObject();
                        doc.startObject("inodes");
                        doc.field("used", 9 + i);
                        doc.field("free", 1924786);
                        doc.field("count", 1924795);
                        doc.endObject();
                        doc.startObject("available");
                        doc.field("bytes", 7883948032L);
                        doc.endObject();
                    }
                    doc.endObject();
                }
                doc.endObject();
            }
            doc.endObject();
            doc.startObject("metricset");
            doc.field("name", "volume");
            doc.field("period", 10000);
            doc.endObject();
            doc.startObject("fields");
            doc.field("cluster", "elastic-apps");
            doc.endObject();
            doc.startObject("host");
            doc.field("name", "gke-apps-host-name" + i);
            doc.endObject();
            doc.startObject("agent");
            {
                doc.field("id", "agent-id-" + i);
                doc.field("version", "7.6.2");
                doc.field("type", "metricbeat");
                doc.field("ephemeral_id", "ephemeral-id-" + i);
                if (i > 0) {
                    doc.field("hostname", "gke-apps-host-name-" + i);
                }
            }
            doc.endObject();
            doc.startObject("ecs");
            doc.field("version", "1.4.0");
            doc.endObject();
            doc.startObject("service");
            doc.field("address", "service-address-" + i);
            doc.field("type", "kubernetes");
            doc.endObject();
            doc.startObject("event");
            doc.field("dataset", "kubernetes.volume");
            doc.field("module", "kubernetes");
            doc.field("duration", 132588484 + i);
            doc.endObject();
        }
        doc.endObject();
        return doc;
    }

    @SuppressWarnings("unchecked")
    public void testDynamicMappingsWithArrayField() throws IOException {
        String index = "test-batch-dynamic-array";
        // Create index with dynamic:true
        // Only map @timestamp — array fields need dynamic mapping.
        XContentBuilder mapping = JsonXContent.contentBuilder();
        mapping.startObject();
        {
            mapping.startObject("_doc");
            {
                mapping.startObject("properties");
                {
                    mapping.startObject("@timestamp").field("type", "date").endObject();
                }
                mapping.endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();

        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping(mapping)
        );
        ensureGreen(index);

        String coordinatingNode = findCoordinatingNode();
        int numDocs = 5;
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            XContentBuilder doc = JsonXContent.contentBuilder();
            doc.startObject();
            doc.field("@timestamp", "2021-04-28T19:45:28.222Z");
            doc.array("tags", "tag-a-" + i, "tag-b-" + i);
            doc.array("metrics", 10 + i, 20 + i, 30 + i);
            doc.endObject();
            bulkRequest.add(new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE).source(doc));
        }

        BulkResponse bulkResponse = client(coordinatingNode).bulk(bulkRequest).actionGet();
        assertNoFailures(bulkResponse);
        assertThat(bulkResponse.getItems().length, equalTo(numDocs));

        refresh(index);

        // Verify correct count
        assertResponse(prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).setSize(0).setTrackTotalHits(true), searchResponse -> {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) numDocs));
        });

        // Verify mappings created for tags and metrics
        var mappingsResponse = client().admin().indices().prepareGetMappings(TimeValue.MAX_VALUE, index).get();
        Map<String, Object> mappingMap = mappingsResponse.getMappings().get(index).sourceAsMap();
        Map<String, Object> properties = (Map<String, Object>) mappingMap.get("properties");
        assertNotNull("tags should be mapped", properties.get("tags"));
        assertNotNull("metrics should be mapped", properties.get("metrics"));

        // Verify source reconstruction
        assertResponse(
            prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).setSize(numDocs).setTrackTotalHits(true),
            searchResponse -> {
                assertNoFailures(searchResponse);
                SearchHit[] hits = searchResponse.getHits().getHits();
                for (SearchHit hit : hits) {
                    Map<String, Object> source = hit.getSourceAsMap();
                    assertNotNull("tags should be present in source", source.get("tags"));
                    assertNotNull("metrics should be present in source", source.get("metrics"));
                    List<String> tags = (List<String>) source.get("tags");
                    assertThat("each doc should have 2 tags", tags.size(), equalTo(2));
                }
            }
        );
    }

    public void testBatchModeWithDynamicFalseAndUnmappedFields() throws IOException {
        String index = "test-batch-dynamic-false";

        // Create index with dynamic=false so unmapped fields are ignored (not rejected, not dynamically mapped).
        XContentBuilder mapping = JsonXContent.contentBuilder();
        mapping.startObject();
        {
            mapping.startObject("_doc");
            {
                mapping.startObject("_source");
                mapping.field("mode", "synthetic");
                mapping.endObject();
                mapping.field("dynamic", "false");
                mapping.startObject("properties");
                {
                    mapping.startObject("name").field("type", "keyword").endObject();
                    mapping.startObject("value").field("type", "long").endObject();
                }
                mapping.endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();

        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 2)
                        .put("index.number_of_replicas", 1)
                        .put("index.mapping.source.mode", "synthetic")
                )
                .setMapping(mapping)
        );
        ensureGreen(index);

        String coordinatingNode = findCoordinatingNode();
        int numDocs = randomIntBetween(20, 100);
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            // Include "extra_field" which is not mapped — with dynamic=false it should be ignored
            bulkRequest.add(
                new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE)
                    .source(Map.of("name", "doc-" + i, "value", i, "extra_field", "ignored-" + i))
            );
        }

        BulkResponse bulkResponse = client(coordinatingNode).bulk(bulkRequest).actionGet();
        assertNoFailures(bulkResponse);
        assertThat(bulkResponse.getItems().length, equalTo(numDocs));

        refresh(index);

        // Verify all docs are indexed
        assertResponse(prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).setSize(0).setTrackTotalHits(true), searchResponse -> {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) numDocs));
        });

        // Verify mapped fields are correct via source reconstruction
        assertResponse(
            prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                .setSize(numDocs)
                .addSort("value", SortOrder.ASC)
                .setTrackTotalHits(true),
            searchResponse -> {
                assertNoFailures(searchResponse);
                SearchHit[] hits = searchResponse.getHits().getHits();
                for (int i = 0; i < numDocs; i++) {
                    Map<String, Object> source = hits[i].getSourceAsMap();
                    assertThat("name mismatch at doc " + i, source.get("name"), equalTo("doc-" + i));
                    assertThat("value mismatch at doc " + i, source.get("value"), equalTo(i));
                }
            }
        );
    }

    public void testTimeSeriesIndexViaBatchMode() throws IOException {
        String index = "test-batch-tsdb";

        // Create a time series index
        XContentBuilder mapping = JsonXContent.contentBuilder();
        mapping.startObject();
        {
            mapping.startObject("_doc");
            {
                mapping.startObject("_source");
                mapping.field("mode", "synthetic");
                mapping.endObject();
                mapping.field("dynamic", "strict");
                mapping.startObject("properties");
                {
                    mapping.startObject("@timestamp").field("type", "date").endObject();
                    mapping.startObject("metricset").field("type", "keyword").field("time_series_dimension", true).endObject();
                    mapping.startObject("pod_name").field("type", "keyword").field("time_series_dimension", true).endObject();
                    mapping.startObject("tx").field("type", "long").field("time_series_metric", "gauge").endObject();
                    mapping.startObject("rx").field("type", "long").field("time_series_metric", "gauge").endObject();
                }
                mapping.endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();

        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                        .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of("metricset"))
                        .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2000-01-01T00:00:00.000Z")
                        .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2100-01-01T00:00:00.000Z")
                )
                .setMapping(mapping)
        );
        ensureGreen(index);

        String coordinatingNode = findCoordinatingNode();
        int numDocs = randomIntBetween(10, 50);
        Instant baseTime = Instant.parse("2025-01-15T10:00:00.000Z");

        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(
                new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE)
                    .source(
                        Map.of(
                            "@timestamp",
                            baseTime.plusSeconds(i).toEpochMilli(),
                            "metricset",
                            "pod",
                            "pod_name",
                            "pod-" + (i % 5),
                            "tx",
                            1000L + i,
                            "rx",
                            2000L + i
                        )
                    )
            );
        }

        BulkResponse bulkResponse = client(coordinatingNode).bulk(bulkRequest).actionGet();
        assertNoFailures(bulkResponse);
        assertThat(bulkResponse.getItems().length, equalTo(numDocs));

        refresh(index);

        // Verify all docs are indexed
        assertResponse(prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).setSize(0).setTrackTotalHits(true), searchResponse -> {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) numDocs));
        });

        // Verify source reconstruction and field values
        assertResponse(
            prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                .setSize(numDocs)
                .addSort("tx", SortOrder.ASC)
                .setTrackTotalHits(true),
            searchResponse -> {
                assertNoFailures(searchResponse);
                assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) numDocs));
                SearchHit[] hits = searchResponse.getHits().getHits();
                for (int i = 0; i < numDocs; i++) {
                    Map<String, Object> source = hits[i].getSourceAsMap();
                    assertThat("metricset mismatch at doc " + i, source.get("metricset"), equalTo("pod"));
                    assertThat("pod_name mismatch at doc " + i, source.get("pod_name"), equalTo("pod-" + (i % 5)));
                    assertThat("tx mismatch at doc " + i, source.get("tx"), equalTo(1000 + i));
                    assertThat("rx mismatch at doc " + i, source.get("rx"), equalTo(2000 + i));
                }
            }
        );
    }

    @SuppressWarnings("unchecked")
    public void testBatchModeWithIpArrayField() throws IOException {
        String index = "test-batch-ip-array";

        // Create index with strict mapping
        XContentBuilder mapping = JsonXContent.contentBuilder();
        mapping.startObject();
        {
            mapping.startObject("_doc");
            {
                mapping.startObject("_source");
                mapping.field("mode", "synthetic");
                mapping.endObject();
                mapping.field("dynamic", "strict");
                mapping.startObject("properties");
                {
                    mapping.startObject("name").field("type", "keyword").endObject();
                    mapping.startObject("host_ip").field("type", "ip").endObject();
                }
                mapping.endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();

        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.mapping.source.mode", "synthetic")
                )
                .setMapping(mapping)
        );
        ensureGreen(index);

        String coordinatingNode = findCoordinatingNode();

        // First bulk to establish batch path (mappings already strict, no dynamic needed)
        int numDocs = 10;
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            XContentBuilder doc = JsonXContent.contentBuilder();
            doc.startObject();
            doc.field("name", "host-" + i);
            doc.array("host_ip", "10.0.0." + i, "10.0.1." + i);
            doc.endObject();
            bulkRequest.add(new IndexRequest(index).id("doc-" + i).opType(DocWriteRequest.OpType.CREATE).source(doc));
        }

        BulkResponse bulkResponse = client(coordinatingNode).bulk(bulkRequest).actionGet();
        assertNoFailures(bulkResponse);
        assertThat(bulkResponse.getItems().length, equalTo(numDocs));

        refresh(index);

        // Verify all docs indexed
        assertResponse(prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).setSize(0).setTrackTotalHits(true), searchResponse -> {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) numDocs));
        });

        // Verify source reconstruction returns the IP array correctly
        assertResponse(
            prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                .setSize(numDocs)
                .addSort("name", SortOrder.ASC)
                .setTrackTotalHits(true),
            searchResponse -> {
                assertNoFailures(searchResponse);
                SearchHit[] hits = searchResponse.getHits().getHits();
                for (int i = 0; i < numDocs; i++) {
                    Map<String, Object> source = hits[i].getSourceAsMap();
                    assertThat("name mismatch at doc " + i, source.get("name"), equalTo("host-" + i));
                    List<String> ips = (List<String>) source.get("host_ip");
                    assertNotNull("host_ip should be present at doc " + i, ips);
                    assertThat("should have 2 IPs at doc " + i, ips.size(), equalTo(2));
                    assertTrue("should contain 10.0.0." + i, ips.contains("10.0.0." + i));
                    assertTrue("should contain 10.0.1." + i, ips.contains("10.0.1." + i));
                }
            }
        );
    }

    /**
     * Tests the batch path with named dynamic templates and template parameters,
     * replicating the OTEL metrics indexing scenario where:
     * - A TSDB index has a passthrough "metrics" object with dynamic:true
     * - Dynamic templates define metric types (gauge_long, gauge_double, counter_long, etc.)
     *   without match patterns — they're referenced by name via IndexRequest.setDynamicTemplates()
     * - Template parameters (e.g. {{unit}}) are substituted into the mapping
     * - The batch path must detect dynamic columns, build a mapping update using the templates,
     *   submit it, and then re-parse with the updated mappings
     */
    @SuppressWarnings("unchecked")
    public void testBatchModeWithNamedDynamicTemplatesOtelStyle() throws IOException {
        String index = "test-batch-otel-style";

        // Create a TSDB index with passthrough metrics object and named dynamic templates
        XContentBuilder mapping = JsonXContent.contentBuilder();
        mapping.startObject();
        {
            mapping.startObject("_doc");
            {
                mapping.startObject("_source");
                mapping.field("mode", "synthetic");
                mapping.endObject();

                mapping.startObject("properties");
                {
                    mapping.startObject("@timestamp").field("type", "date").endObject();
                    mapping.startObject("unit").field("type", "keyword").field("time_series_dimension", true).endObject();
                    mapping.startObject("resource_id").field("type", "keyword").field("time_series_dimension", true).endObject();
                    mapping.startObject("metrics");
                    {
                        mapping.field("type", "passthrough");
                        mapping.field("dynamic", true);
                        mapping.field("priority", 10);
                    }
                    mapping.endObject();
                }
                mapping.endObject();

                // Named dynamic templates — no match patterns, referenced by name
                mapping.startArray("dynamic_templates");
                {
                    // gauge_long template
                    mapping.startObject();
                    mapping.startObject("gauge_long");
                    mapping.startObject("mapping");
                    mapping.field("type", "long");
                    mapping.field("time_series_metric", "gauge");
                    mapping.startObject("meta");
                    mapping.field("unit", "{{unit}}");
                    mapping.endObject();
                    mapping.endObject();
                    mapping.endObject();
                    mapping.endObject();

                    // gauge_double template
                    mapping.startObject();
                    mapping.startObject("gauge_double");
                    mapping.startObject("mapping");
                    mapping.field("type", "double");
                    mapping.field("time_series_metric", "gauge");
                    mapping.startObject("meta");
                    mapping.field("unit", "{{unit}}");
                    mapping.endObject();
                    mapping.endObject();
                    mapping.endObject();
                    mapping.endObject();

                    // counter_long template
                    mapping.startObject();
                    mapping.startObject("counter_long");
                    mapping.startObject("mapping");
                    mapping.field("type", "long");
                    mapping.field("time_series_metric", "counter");
                    mapping.startObject("meta");
                    mapping.field("unit", "{{unit}}");
                    mapping.endObject();
                    mapping.endObject();
                    mapping.endObject();
                    mapping.endObject();
                }
                mapping.endArray();
            }
            mapping.endObject();
        }
        mapping.endObject();

        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                        .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of("resource_id"))
                        .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2000-01-01T00:00:00.000Z")
                        .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2100-01-01T00:00:00.000Z")
                )
                .setMapping(mapping)
        );
        ensureGreen(index);

        String coordinatingNode = findCoordinatingNode();
        Instant baseTime = Instant.parse("2025-01-15T10:00:00.000Z");

        // Build a bulk request where each doc has different metric fields requiring dynamic mapping via templates
        int numDocs = 5;
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            // Each doc has: @timestamp, resource_id, unit, and metric fields under "metrics.*"
            XContentBuilder doc = JsonXContent.contentBuilder();
            doc.startObject();
            doc.field("@timestamp", baseTime.plusSeconds(i).toEpochMilli());
            doc.field("resource_id", "host-1");
            doc.field("unit", "By");
            doc.startObject("metrics");
            doc.field("cpu_usage", 42.5 + i);     // gauge_double
            doc.field("memory_used", 1024L + i);   // gauge_long
            doc.field("requests_total", 100L + i);  // counter_long
            doc.endObject();
            doc.endObject();

            // Set named dynamic templates — these map field paths to template names
            IndexRequest indexRequest = new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE).source(doc);
            indexRequest.setDynamicTemplates(
                Map.of("metrics.cpu_usage", "gauge_double", "metrics.memory_used", "gauge_long", "metrics.requests_total", "counter_long")
            );
            indexRequest.setDynamicTemplateParams(
                Map.of(
                    "metrics.cpu_usage",
                    Map.of("unit", "percent"),
                    "metrics.memory_used",
                    Map.of("unit", "By"),
                    "metrics.requests_total",
                    Map.of("unit", "1")
                )
            );
            bulkRequest.add(indexRequest);
        }

        BulkResponse bulkResponse = client(coordinatingNode).bulk(bulkRequest).actionGet();
        assertNoFailures(bulkResponse);
        assertThat(bulkResponse.getItems().length, equalTo(numDocs));

        refresh(index);

        // Verify dynamic mappings were created correctly via the templates
        var mappingsResponse = client().admin().indices().prepareGetMappings(TimeValue.MAX_VALUE, index).get();
        Map<String, Object> mappingMap = mappingsResponse.getMappings().get(index).sourceAsMap();
        Map<String, Object> properties = (Map<String, Object>) mappingMap.get("properties");
        Map<String, Object> metricsProps = (Map<String, Object>) ((Map<String, Object>) properties.get("metrics")).get("properties");

        // cpu_usage should be mapped as double with gauge metric
        Map<String, Object> cpuUsage = (Map<String, Object>) metricsProps.get("cpu_usage");
        assertThat("cpu_usage type", cpuUsage.get("type"), equalTo("double"));
        assertThat("cpu_usage metric", cpuUsage.get("time_series_metric"), equalTo("gauge"));
        Map<String, Object> cpuMeta = (Map<String, Object>) cpuUsage.get("meta");
        assertThat("cpu_usage unit", cpuMeta.get("unit"), equalTo("percent"));

        // memory_used should be mapped as long with gauge metric
        Map<String, Object> memoryUsed = (Map<String, Object>) metricsProps.get("memory_used");
        assertThat("memory_used type", memoryUsed.get("type"), equalTo("long"));
        assertThat("memory_used metric", memoryUsed.get("time_series_metric"), equalTo("gauge"));
        Map<String, Object> memMeta = (Map<String, Object>) memoryUsed.get("meta");
        assertThat("memory_used unit", memMeta.get("unit"), equalTo("By"));

        // requests_total should be mapped as long with counter metric
        Map<String, Object> requestsTotal = (Map<String, Object>) metricsProps.get("requests_total");
        assertThat("requests_total type", requestsTotal.get("type"), equalTo("long"));
        assertThat("requests_total metric", requestsTotal.get("time_series_metric"), equalTo("counter"));
        Map<String, Object> reqMeta = (Map<String, Object>) requestsTotal.get("meta");
        assertThat("requests_total unit", reqMeta.get("unit"), equalTo("1"));

        // Verify all docs are indexed and retrievable
        assertResponse(prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).setSize(0).setTrackTotalHits(true), searchResponse -> {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) numDocs));
        });

        // Verify field values via source reconstruction
        assertResponse(
            prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                .setSize(numDocs)
                .addSort("metrics.cpu_usage", SortOrder.ASC)
                .setTrackTotalHits(true),
            searchResponse -> {
                assertNoFailures(searchResponse);
                SearchHit[] hits = searchResponse.getHits().getHits();
                for (int i = 0; i < numDocs; i++) {
                    Map<String, Object> source = hits[i].getSourceAsMap();
                    Map<String, Object> metrics = (Map<String, Object>) source.get("metrics");
                    assertThat("cpu_usage mismatch at doc " + i, metrics.get("cpu_usage"), equalTo(42.5 + i));
                    assertThat("memory_used mismatch at doc " + i, ((Number) metrics.get("memory_used")).longValue(), equalTo(1024L + i));
                    assertThat(
                        "requests_total mismatch at doc " + i,
                        ((Number) metrics.get("requests_total")).longValue(),
                        equalTo(100L + i)
                    );
                }
            }
        );
    }

    /**
     * Reproduces the scenario where different documents in a batch have different metric fields,
     * each with their own dynamic template assignments. The batch path was only reading
     * dynamic templates from the first IndexRequest, so fields that only appeared in later
     * documents would get default mappings (without time_series_metric) instead of the
     * correct template-based mapping (with time_series_metric: counter/gauge).
     *
     * This caused: "Cannot update parameter [time_series_metric] from [null] to [counter]"
     */
    @SuppressWarnings("unchecked")
    public void testBatchModeWithHeterogeneousMetricFields() throws IOException {
        String index = "test-batch-heterogeneous-metrics";

        // Create a TSDB index with passthrough metrics object and named dynamic templates
        XContentBuilder mapping = JsonXContent.contentBuilder();
        mapping.startObject();
        {
            mapping.startObject("_doc");
            {
                mapping.startObject("_source");
                mapping.field("mode", "synthetic");
                mapping.endObject();

                mapping.startObject("properties");
                {
                    mapping.startObject("@timestamp").field("type", "date").endObject();
                    mapping.startObject("resource_id").field("type", "keyword").field("time_series_dimension", true).endObject();
                    mapping.startObject("metrics");
                    {
                        mapping.field("type", "passthrough");
                        mapping.field("dynamic", true);
                        mapping.field("priority", 10);
                    }
                    mapping.endObject();
                }
                mapping.endObject();

                // Named dynamic templates
                mapping.startArray("dynamic_templates");
                {
                    mapping.startObject();
                    mapping.startObject("gauge_long");
                    mapping.startObject("mapping");
                    mapping.field("type", "long");
                    mapping.field("time_series_metric", "gauge");
                    mapping.endObject();
                    mapping.endObject();
                    mapping.endObject();

                    mapping.startObject();
                    mapping.startObject("counter_long");
                    mapping.startObject("mapping");
                    mapping.field("type", "long");
                    mapping.field("time_series_metric", "counter");
                    mapping.endObject();
                    mapping.endObject();
                    mapping.endObject();

                    mapping.startObject();
                    mapping.startObject("gauge_double");
                    mapping.startObject("mapping");
                    mapping.field("type", "double");
                    mapping.field("time_series_metric", "gauge");
                    mapping.endObject();
                    mapping.endObject();
                    mapping.endObject();
                }
                mapping.endArray();
            }
            mapping.endObject();
        }
        mapping.endObject();

        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                        .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of("resource_id"))
                        .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2000-01-01T00:00:00.000Z")
                        .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2100-01-01T00:00:00.000Z")
                )
                .setMapping(mapping)
        );
        ensureGreen(index);

        String coordinatingNode = findCoordinatingNode();
        Instant baseTime = Instant.parse("2025-01-15T10:00:00.000Z");

        // Key: different documents have DIFFERENT metric fields with DIFFERENT templates.
        // Doc 0: has cpu_usage (gauge_double) and memory_used (gauge_long), but NOT network_errors
        // Doc 1: has cpu_usage (gauge_double) and network_errors (counter_long), but NOT memory_used
        // The batch schema will have columns for all three fields.
        // The bug: only doc 0's templates were read, so network_errors got a default mapping.
        BulkRequest bulkRequest = new BulkRequest();

        // Doc 0: cpu_usage + memory_used
        {
            XContentBuilder doc = JsonXContent.contentBuilder();
            doc.startObject();
            doc.field("@timestamp", baseTime.toEpochMilli());
            doc.field("resource_id", "host-1");
            doc.startObject("metrics");
            doc.field("cpu_usage", 42.5);
            doc.field("memory_used", 1024L);
            doc.endObject();
            doc.endObject();

            IndexRequest ir = new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE).source(doc);
            ir.setDynamicTemplates(Map.of("metrics.cpu_usage", "gauge_double", "metrics.memory_used", "gauge_long"));
            bulkRequest.add(ir);
        }

        // Doc 1: cpu_usage + network_errors (counter!)
        {
            XContentBuilder doc = JsonXContent.contentBuilder();
            doc.startObject();
            doc.field("@timestamp", baseTime.plusSeconds(1).toEpochMilli());
            doc.field("resource_id", "host-1");
            doc.startObject("metrics");
            doc.field("cpu_usage", 55.0);
            doc.field("network_errors", 7L);
            doc.endObject();
            doc.endObject();

            IndexRequest ir = new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE).source(doc);
            ir.setDynamicTemplates(Map.of("metrics.cpu_usage", "gauge_double", "metrics.network_errors", "counter_long"));
            bulkRequest.add(ir);
        }

        // Doc 2: all three fields
        {
            XContentBuilder doc = JsonXContent.contentBuilder();
            doc.startObject();
            doc.field("@timestamp", baseTime.plusSeconds(2).toEpochMilli());
            doc.field("resource_id", "host-1");
            doc.startObject("metrics");
            doc.field("cpu_usage", 60.0);
            doc.field("memory_used", 2048L);
            doc.field("network_errors", 3L);
            doc.endObject();
            doc.endObject();

            IndexRequest ir = new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE).source(doc);
            ir.setDynamicTemplates(
                Map.of("metrics.cpu_usage", "gauge_double", "metrics.memory_used", "gauge_long", "metrics.network_errors", "counter_long")
            );
            bulkRequest.add(ir);
        }

        BulkResponse bulkResponse = client(coordinatingNode).bulk(bulkRequest).actionGet();
        assertNoFailures(bulkResponse);
        assertThat(bulkResponse.getItems().length, equalTo(3));

        refresh(index);

        // Verify all docs indexed
        assertResponse(prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).setSize(0).setTrackTotalHits(true), searchResponse -> {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(3L));
        });

        // Verify dynamic mappings were created correctly via templates
        var mappingsResponse = client().admin().indices().prepareGetMappings(TimeValue.MAX_VALUE, index).get();
        Map<String, Object> mappingMap = mappingsResponse.getMappings().get(index).sourceAsMap();
        Map<String, Object> properties = (Map<String, Object>) mappingMap.get("properties");
        Map<String, Object> metricsProps = (Map<String, Object>) ((Map<String, Object>) properties.get("metrics")).get("properties");

        // network_errors should be counter, not a plain long
        Map<String, Object> networkErrors = (Map<String, Object>) metricsProps.get("network_errors");
        assertThat("network_errors type", networkErrors.get("type"), equalTo("long"));
        assertThat("network_errors must be counter metric", networkErrors.get("time_series_metric"), equalTo("counter"));

        // memory_used should be gauge
        Map<String, Object> memoryUsed = (Map<String, Object>) metricsProps.get("memory_used");
        assertThat("memory_used type", memoryUsed.get("type"), equalTo("long"));
        assertThat("memory_used must be gauge metric", memoryUsed.get("time_series_metric"), equalTo("gauge"));

        // cpu_usage should be gauge double
        Map<String, Object> cpuUsage = (Map<String, Object>) metricsProps.get("cpu_usage");
        assertThat("cpu_usage type", cpuUsage.get("type"), equalTo("double"));
        assertThat("cpu_usage must be gauge metric", cpuUsage.get("time_series_metric"), equalTo("gauge"));
    }

    public void testBatchModeWithDynamicRuntimeFields() throws IOException {
        String index = "test-batch-runtime";

        // Create index with dynamic=runtime so unmapped fields become runtime fields.
        // The first bulk triggers dynamic mapping creation.
        // The second bulk should NOT require a dynamic mapping update.
        XContentBuilder mapping = JsonXContent.contentBuilder();
        mapping.startObject();
        {
            mapping.startObject("_doc");
            {
                mapping.startObject("_source");
                mapping.field("mode", "synthetic");
                mapping.endObject();
                mapping.field("dynamic", "runtime");
                mapping.startObject("properties");
                {
                    mapping.startObject("name").field("type", "keyword").endObject();
                    mapping.startObject("value").field("type", "long").endObject();
                }
                mapping.endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();

        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 2)
                        .put("index.number_of_replicas", 1)
                        .put("index.mapping.source.mode", "synthetic")
                )
                .setMapping(mapping)
        );
        ensureGreen(index);

        String coordinatingNode = findCoordinatingNode();

        // First bulk: triggers dynamic runtime mapping creation for "message" field (serial path)
        int firstBulkDocs = 5;
        BulkRequest firstBulk = new BulkRequest();
        for (int i = 0; i < firstBulkDocs; i++) {
            firstBulk.add(
                new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE)
                    .source(Map.of("name", "first-" + i, "value", i, "message", "runtime field test " + i))
            );
        }
        BulkResponse firstResponse = client(coordinatingNode).bulk(firstBulk).actionGet();
        assertNoFailures(firstResponse);

        // Second bulk: "message" is now a runtime field in the mapping — batch path should work
        int secondBulkDocs = randomIntBetween(20, 100);
        BulkRequest secondBulk = new BulkRequest();
        for (int i = 0; i < secondBulkDocs; i++) {
            secondBulk.add(
                new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE)
                    .source(Map.of("name", "second-" + i, "value", firstBulkDocs + i, "message", "batch runtime test " + i))
            );
        }
        BulkResponse secondResponse = client(coordinatingNode).bulk(secondBulk).actionGet();
        assertNoFailures(secondResponse);
        assertThat(secondResponse.getItems().length, equalTo(secondBulkDocs));

        refresh(index);

        int totalDocs = firstBulkDocs + secondBulkDocs;

        // Verify all docs are indexed
        assertResponse(prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).setSize(0).setTrackTotalHits(true), searchResponse -> {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) totalDocs));
        });

        // Verify mapped fields are correct via source reconstruction
        assertResponse(
            prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                .setSize(totalDocs)
                .addSort("value", SortOrder.ASC)
                .setTrackTotalHits(true),
            searchResponse -> {
                assertNoFailures(searchResponse);
                assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) totalDocs));
                SearchHit[] hits = searchResponse.getHits().getHits();
                for (int i = 0; i < firstBulkDocs; i++) {
                    Map<String, Object> source = hits[i].getSourceAsMap();
                    assertThat("name mismatch at doc " + i, source.get("name"), equalTo("first-" + i));
                    assertThat("value mismatch at doc " + i, source.get("value"), equalTo(i));
                }
                for (int i = 0; i < secondBulkDocs; i++) {
                    Map<String, Object> source = hits[firstBulkDocs + i].getSourceAsMap();
                    assertThat("name mismatch at doc " + i, source.get("name"), equalTo("second-" + i));
                    assertThat("value mismatch at doc " + i, source.get("value"), equalTo(firstBulkDocs + i));
                }
            }
        );
    }
}
