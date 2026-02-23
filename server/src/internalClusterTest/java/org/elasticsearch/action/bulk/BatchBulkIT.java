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

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 1)
public class BatchBulkIT extends ESIntegTestCase {

    private void createBatchIndex(String index, int shards, int replicas) throws IOException {
        XContentBuilder mapping = getMapping();
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", shards)
                        .put("index.number_of_replicas", replicas)
                        .put("index.mapping.source.mode", "synthetic")
                        .put(IndexSettings.COLUMN_BATCH_INDEX.getKey(), true)
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
                // 1. Move synthetic source configuration here (it's safer than the setting)
                mapping.startObject("_source");
                mapping.field("mode", "synthetic");
                mapping.endObject();

                // 2. Set Dynamic Mapping to Strict
                mapping.field("dynamic", "strict");

                // 3. Define Properties
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

    public void testBatchModeNotEnabledWithoutSetting() throws IOException {
        // Create an index without the column_batch_index setting
        String index = "test-no-batch";
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 1))
                .setMapping(getMapping())
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

    @AwaitsFix(bugUrl = "Source reconstruction from columnar batch not yet implemented")
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
        // Create index with dynamic:true (default), synthetic source, and column_batch_index enabled.
        // Only map @timestamp — everything else needs dynamic mapping.
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
                        .put(IndexSettings.COLUMN_BATCH_INDEX.getKey(), true)
                )
                .setMapping(mapping)
        );
        ensureGreen(index);

        String coordinatingNode = findCoordinatingNode();
        int numDocs = 5;
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
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
                    // Omit hostname from doc 0 to exercise "first present value" inference via serial fallback
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

        // Verify key mappings exist
        var mappingsResponse = client().admin().indices().prepareGetMappings(TimeValue.MAX_VALUE, index).get();
        Map<String, Object> mappingMap = mappingsResponse.getMappings().get(index).sourceAsMap();
        Map<String, Object> properties = (Map<String, Object>) mappingMap.get("properties");
        assertNotNull("kubernetes should be mapped", properties.get("kubernetes"));
        assertNotNull("event should be mapped", properties.get("event"));
        assertNotNull("@timestamp should be mapped", properties.get("@timestamp"));

        // Verify source reconstruction
        assertResponse(
            prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                .setSize(numDocs)
                .addSort("kubernetes.namespace.keyword", SortOrder.ASC)
                .setTrackTotalHits(true),
            searchResponse -> {
                assertNoFailures(searchResponse);
                SearchHit[] hits = searchResponse.getHits().getHits();
                for (int i = 0; i < numDocs; i++) {
                    Map<String, Object> source = hits[i].getSourceAsMap();
                    Map<String, Object> kubernetes = (Map<String, Object>) source.get("kubernetes");
                    assertThat("namespace mismatch at doc " + i, kubernetes.get("namespace"), equalTo("namespace" + i));
                    Map<String, Object> event = (Map<String, Object>) source.get("event");
                    assertThat("event.dataset mismatch at doc " + i, event.get("dataset"), equalTo("kubernetes.volume"));
                }
            }
        );
    }

    @SuppressWarnings("unchecked")
    public void testDynamicMappingsWithArrayField() throws IOException {
        String index = "test-batch-dynamic-array";
        // Create index with dynamic:true, stored source, and column_batch_index enabled.
        // Only map @timestamp — array fields need dynamic mapping.
        // Note: we use stored source (not synthetic) because synthetic source has limitations
        // with dynamically mapped text/long arrays.
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
                        .put(IndexSettings.COLUMN_BATCH_INDEX.getKey(), true)
                )
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

    public void testTimeSeriesIndexViaBatchMode() throws IOException {
        String index = "test-batch-tsdb";

        // Create a time series index with batch mode enabled
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
                        .put(IndexSettings.COLUMN_BATCH_INDEX.getKey(), true)
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
}
