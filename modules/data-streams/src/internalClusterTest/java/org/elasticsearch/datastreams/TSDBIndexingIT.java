/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.diskusage.AnalyzeIndexDiskUsageRequest;
import org.elasticsearch.action.admin.indices.diskusage.TransportAnalyzeIndexDiskUsageAction;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.IndexDocFailureStoreStatus;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.xcontent.XContentType;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class TSDBIndexingIT extends ESSingleNodeTestCase {

    public static final String MAPPING_TEMPLATE = """
        {
          "_doc":{
            "properties": {
              "@timestamp" : {
                "type": "date"
              },
              "metricset": {
                "type": "keyword",
                "time_series_dimension": true
              }
            }
          }
        }""";

    private static final String DOC = """
        {
            "@timestamp": "$time",
            "metricset": "pod",
            "k8s": {
                "pod": {
                    "name": "dog",
                    "uid":"df3145b3-0563-4d3b-a0f7-897eb2876ea9",
                    "ip": "10.10.55.3",
                    "network": {
                        "tx": 1434595272,
                        "rx": 530605511
                    }
                }
            }
        }
        """;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(DataStreamsPlugin.class, InternalSettingsPlugin.class, ReindexPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        Settings.Builder newSettings = Settings.builder();
        newSettings.put(super.nodeSettings());
        // This essentially disables the automatic updates to end_time settings of a data stream's latest backing index.
        newSettings.put(DataStreamsPlugin.TIME_SERIES_POLL_INTERVAL.getKey(), "10m");
        return newSettings.build();
    }

    public void testTimeRanges() throws Exception {
        var templateSettings = Settings.builder().put("index.mode", "time_series");
        if (randomBoolean()) {
            templateSettings.put("index.routing_path", "metricset");
        }
        var mapping = new CompressedXContent(randomBoolean() ? MAPPING_TEMPLATE : MAPPING_TEMPLATE.replace("date", "date_nanos"));

        if (randomBoolean()) {
            var request = new TransportPutComposableIndexTemplateAction.Request("id");
            request.indexTemplate(
                ComposableIndexTemplate.builder()
                    .indexPatterns(List.of("k8s*"))
                    .template(new Template(templateSettings.build(), mapping, null))
                    .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                    .build()
            );
            client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();
        } else {
            var putComponentTemplateRequest = new PutComponentTemplateAction.Request("1");
            putComponentTemplateRequest.componentTemplate(new ComponentTemplate(new Template(null, mapping, null), null, null));
            client().execute(PutComponentTemplateAction.INSTANCE, putComponentTemplateRequest).actionGet();

            var putTemplateRequest = new TransportPutComposableIndexTemplateAction.Request("id");
            putTemplateRequest.indexTemplate(
                ComposableIndexTemplate.builder()
                    .indexPatterns(List.of("k8s*"))
                    .template(new Template(templateSettings.build(), null, null))
                    .componentTemplates(List.of("1"))
                    .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                    .build()
            );
            client().execute(TransportPutComposableIndexTemplateAction.TYPE, putTemplateRequest).actionGet();
        }

        // index doc
        Instant time = Instant.now();
        String backingIndexName;
        {
            var indexRequest = new IndexRequest("k8s").opType(DocWriteRequest.OpType.CREATE);
            indexRequest.source(DOC.replace("$time", formatInstant(time)), XContentType.JSON);
            var indexResponse = client().index(indexRequest).actionGet();
            backingIndexName = indexResponse.getIndex();
        }

        // fetch end time
        var getIndexResponse = indicesAdmin().getIndex(new GetIndexRequest().indices(backingIndexName)).actionGet();
        Instant endTime = IndexSettings.TIME_SERIES_END_TIME.get(getIndexResponse.getSettings().get(backingIndexName));

        // index another doc and verify index
        {
            var indexRequest = new IndexRequest("k8s").opType(DocWriteRequest.OpType.CREATE);
            indexRequest.source(DOC.replace("$time", formatInstant(endTime.minusSeconds(1))), XContentType.JSON);
            var indexResponse = client().index(indexRequest).actionGet();
            assertThat(indexResponse.getIndex(), equalTo(backingIndexName));
        }

        // index doc beyond range and check failure
        {
            var indexRequest = new IndexRequest("k8s").opType(DocWriteRequest.OpType.CREATE);
            time = randomBoolean() ? endTime : endTime.plusSeconds(randomIntBetween(1, 99));
            indexRequest.source(DOC.replace("$time", formatInstant(time)), XContentType.JSON);
            expectThrows(IndexDocFailureStoreStatus.ExceptionWithFailureStoreStatus.class, () -> client().index(indexRequest).actionGet());
        }

        // Fetch UpdateTimeSeriesRangeService and increment time range of latest backing index:
        UpdateTimeSeriesRangeService updateTimeSeriesRangeService = getInstanceFromNode(UpdateTimeSeriesRangeService.class);
        CountDownLatch latch = new CountDownLatch(1);
        updateTimeSeriesRangeService.perform(latch::countDown);
        latch.await();

        // index again and check for success
        {
            var indexRequest = new IndexRequest("k8s").opType(DocWriteRequest.OpType.CREATE);
            indexRequest.source(DOC.replace("$time", formatInstant(time)), XContentType.JSON);
            var indexResponse = client().index(indexRequest).actionGet();
            assertThat(indexResponse.getIndex(), equalTo(backingIndexName));
        }

        // rollover
        var rolloverRequest = new RolloverRequest("k8s", null);
        var rolloverResponse = indicesAdmin().rolloverIndex(rolloverRequest).actionGet();
        var newBackingIndexName = rolloverResponse.getNewIndex();

        // index and check target index is new
        getIndexResponse = indicesAdmin().getIndex(new GetIndexRequest().indices(newBackingIndexName)).actionGet();
        Instant newStartTime = IndexSettings.TIME_SERIES_START_TIME.get(getIndexResponse.getSettings().get(newBackingIndexName));
        Instant newEndTime = IndexSettings.TIME_SERIES_END_TIME.get(getIndexResponse.getSettings().get(newBackingIndexName));

        // Check whether documents land in the newest backing index, covering the [newStartTime, newEndtime) timestamp range:
        time = newStartTime;
        {
            var indexRequest = new IndexRequest("k8s").opType(DocWriteRequest.OpType.CREATE);
            indexRequest.source(DOC.replace("$time", formatInstant(time)), XContentType.JSON);
            var indexResponse = client().index(indexRequest).actionGet();
            assertThat(indexResponse.getIndex(), equalTo(newBackingIndexName));
        }
        time = Instant.ofEpochMilli(randomLongBetween(newStartTime.toEpochMilli(), newEndTime.toEpochMilli() - 1));
        {
            var indexRequest = new IndexRequest("k8s").opType(DocWriteRequest.OpType.CREATE);
            indexRequest.source(DOC.replace("$time", formatInstant(time)), XContentType.JSON);
            var indexResponse = client().index(indexRequest).actionGet();
            assertThat(indexResponse.getIndex(), equalTo(newBackingIndexName));
        }
        time = Instant.ofEpochMilli(newEndTime.toEpochMilli() - 1);
        {
            var indexRequest = new IndexRequest("k8s").opType(DocWriteRequest.OpType.CREATE);
            indexRequest.source(DOC.replace("$time", formatInstant(time)), XContentType.JSON);
            var indexResponse = client().index(indexRequest).actionGet();
            assertThat(indexResponse.getIndex(), equalTo(newBackingIndexName));
        }

        // Double check indexing against previous backing index:
        time = newStartTime.minusMillis(1);
        {
            var indexRequest = new IndexRequest("k8s").opType(DocWriteRequest.OpType.CREATE);
            indexRequest.source(DOC.replace("$time", formatInstant(time)), XContentType.JSON);
            var indexResponse = client().index(indexRequest).actionGet();
            assertThat(indexResponse.getIndex(), equalTo(backingIndexName));
        }
    }

    public void testInvalidTsdbTemplatesNoTimeSeriesDimensionAttribute() throws Exception {
        var mappingTemplate = """
            {
              "_doc":{
                "properties": {
                  "metricset": {
                    "type": "keyword"
                  }
                }
              }
            }""";
        {
            var request = new TransportPutComposableIndexTemplateAction.Request("id");
            request.indexTemplate(
                ComposableIndexTemplate.builder()
                    .indexPatterns(List.of("k8s*"))
                    .template(
                        new Template(
                            Settings.builder().put("index.mode", "time_series").put("index.routing_path", "metricset").build(),
                            new CompressedXContent(mappingTemplate),
                            null
                        )
                    )
                    .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                    .build()
            );
            var e = expectThrows(
                IllegalArgumentException.class,
                () -> client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet()
            );
            assertThat(
                e.getCause().getCause().getMessage(),
                equalTo(
                    "All fields that match routing_path must be configured with [time_series_dimension: true] "
                        + "or flattened fields with a list of dimensions in [time_series_dimensions] and "
                        + "without the [script] parameter. [metricset] was not a dimension."
                )
            );
        }
        {
            var request = new TransportPutComposableIndexTemplateAction.Request("id");
            request.indexTemplate(
                ComposableIndexTemplate.builder()
                    .indexPatterns(List.of("k8s*"))
                    .template(
                        new Template(
                            Settings.builder().put("index.mode", "time_series").build(),
                            new CompressedXContent(mappingTemplate),
                            null
                        )
                    )
                    .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                    .build()
            );
            var e = expectThrows(
                InvalidIndexTemplateException.class,
                () -> client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet()
            );
            assertThat(e.getMessage(), containsString("[index.mode=time_series] requires a non-empty [index.routing_path]"));
        }
    }

    public void testTsdbTemplatesNoKeywordFieldType() throws Exception {
        var mappingTemplate = """
            {
              "_doc":{
                "properties": {
                  "metricset": {
                    "type": "long",
                    "time_series_dimension": true
                  }
                }
              }
            }""";
        var request = new TransportPutComposableIndexTemplateAction.Request("id");
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of("k8s*"))
                .template(
                    new Template(
                        Settings.builder().put("index.mode", "time_series").put("index.routing_path", "metricset").build(),
                        new CompressedXContent(mappingTemplate),
                        null
                    )
                )
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                .build()
        );
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();
    }

    public void testInvalidTsdbTemplatesMissingSettings() throws Exception {
        var mappingTemplate = """
            {
              "_doc":{
                "properties": {
                  "metricset": {
                    "type": "keyword",
                    "time_series_dimension": true
                  }
                }
              }
            }""";
        var request = new TransportPutComposableIndexTemplateAction.Request("id");
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of("k8s*"))
                .template(
                    new Template(
                        Settings.builder().put("index.routing_path", "metricset").build(),
                        new CompressedXContent(mappingTemplate),
                        null
                    )
                )
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                .build()
        );
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet()
        );
        assertThat(e.getCause().getMessage(), equalTo("[index.routing_path] requires [index.mode=time_series]"));
    }

    public void testSkippingShards() throws Exception {
        Instant time = Instant.now();
        var mapping = new CompressedXContent(randomBoolean() ? MAPPING_TEMPLATE : MAPPING_TEMPLATE.replace("date", "date_nanos"));
        {
            var templateSettings = Settings.builder().put("index.mode", "time_series").put("index.routing_path", "metricset").build();
            var request = new TransportPutComposableIndexTemplateAction.Request("id1");
            request.indexTemplate(
                ComposableIndexTemplate.builder()
                    .indexPatterns(List.of("pattern-1"))
                    .template(new Template(templateSettings, mapping, null))
                    .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                    .build()
            );
            client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();
            var indexRequest = new IndexRequest("pattern-1").opType(DocWriteRequest.OpType.CREATE).setRefreshPolicy("true");
            indexRequest.source(DOC.replace("$time", formatInstant(time)), XContentType.JSON);
            client().index(indexRequest).actionGet();
        }
        {
            var request = new TransportPutComposableIndexTemplateAction.Request("id2");
            request.indexTemplate(
                ComposableIndexTemplate.builder()
                    .indexPatterns(List.of("pattern-2"))
                    .template(new Template(null, mapping, null))
                    .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                    .build()
            );
            client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();
            var indexRequest = new IndexRequest("pattern-2").opType(DocWriteRequest.OpType.CREATE).setRefreshPolicy("true");
            indexRequest.source(DOC.replace("$time", formatInstant(time)), XContentType.JSON);
            client().index(indexRequest).actionGet();
        }
        {
            var matchingRange = new SearchSourceBuilder().query(
                new RangeQueryBuilder("@timestamp").from(time.minusSeconds(1).toEpochMilli()).to(time.plusSeconds(1).toEpochMilli())
            );
            var searchRequest = new SearchRequest("pattern-*");
            searchRequest.setPreFilterShardSize(1);
            searchRequest.source(matchingRange);
            assertResponse(client().search(searchRequest), searchResponse -> {
                ElasticsearchAssertions.assertHitCount(searchResponse, 2);
                assertThat(searchResponse.getTotalShards(), equalTo(2));
                assertThat(searchResponse.getSkippedShards(), equalTo(0));
                assertThat(searchResponse.getSuccessfulShards(), equalTo(2));
            });
        }
        {
            var nonMatchingRange = new SearchSourceBuilder().query(
                new RangeQueryBuilder("@timestamp").from(time.minus(2, ChronoUnit.DAYS).toEpochMilli())
                    .to(time.minus(1, ChronoUnit.DAYS).toEpochMilli())
            );
            var searchRequest = new SearchRequest("pattern-*");
            searchRequest.setPreFilterShardSize(1);
            searchRequest.source(nonMatchingRange);
            assertResponse(client().search(searchRequest), searchResponse -> {
                ElasticsearchAssertions.assertNoSearchHits(searchResponse);
                assertThat(searchResponse.getTotalShards(), equalTo(2));
                assertThat(searchResponse.getSkippedShards(), equalTo(2));
                assertThat(searchResponse.getSuccessfulShards(), equalTo(2));
            });
        }
    }

    public void testTrimId() throws Exception {
        String dataStreamName = "k8s";
        var putTemplateRequest = new TransportPutComposableIndexTemplateAction.Request("id");
        putTemplateRequest.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(dataStreamName + "*"))
                .template(
                    new Template(
                        Settings.builder()
                            .put("index.mode", "time_series")
                            .put("index.number_of_replicas", 0)
                            // Reduce sync interval to speedup this integraton test,
                            // otherwise by default it will take 30 seconds before minimum retained seqno is updated:
                            .put("index.soft_deletes.retention_lease.sync_interval", "100ms")
                            .build(),
                        new CompressedXContent(MAPPING_TEMPLATE),
                        null
                    )
                )
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                .build()
        );
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, putTemplateRequest).actionGet();

        // index some data
        int numBulkRequests = 32;
        int numDocsPerBulk = 256;
        String indexName = null;
        {
            Instant time = Instant.now();
            for (int i = 0; i < numBulkRequests; i++) {
                BulkRequest bulkRequest = new BulkRequest(dataStreamName);
                for (int j = 0; j < numDocsPerBulk; j++) {
                    var indexRequest = new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE);
                    indexRequest.source(DOC.replace("$time", formatInstant(time)), XContentType.JSON);
                    bulkRequest.add(indexRequest);
                    time = time.plusMillis(1);
                }
                var bulkResponse = client().bulk(bulkRequest).actionGet();
                assertThat(bulkResponse.hasFailures(), is(false));
                indexName = bulkResponse.getItems()[0].getIndex();
            }
            client().admin().indices().refresh(new RefreshRequest(dataStreamName)).actionGet();

            // In rare cases we can end up with a single segment shard, which means we can't trim away the _id later.
            // So update an existing doc to create a new segment without adding a new document after force merging:
            var indexRequest = new IndexRequest(indexName).setIfPrimaryTerm(1L)
                .setIfSeqNo((numBulkRequests * numDocsPerBulk) - 1)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            indexRequest.source(DOC.replace("$time", formatInstant(time.minusMillis(1))), XContentType.JSON);
            var res = client().index(indexRequest).actionGet();
            assertThat(res.status(), equalTo(RestStatus.OK));
            assertThat(res.getVersion(), equalTo(2L));
        }

        // Check whether there are multiple segments:
        var getSegmentsResponse = client().admin().indices().segments(new IndicesSegmentsRequest(dataStreamName)).actionGet();
        assertThat(
            getSegmentsResponse.getIndices().get(indexName).getShards().get(0).shards()[0].getSegments(),
            hasSize(greaterThanOrEqualTo(2))
        );

        // Pre check whether _id stored field uses diskspace:
        var diskUsageResponse = client().execute(
            TransportAnalyzeIndexDiskUsageAction.TYPE,
            new AnalyzeIndexDiskUsageRequest(new String[] { dataStreamName }, AnalyzeIndexDiskUsageRequest.DEFAULT_INDICES_OPTIONS, true)
        ).actionGet();
        var map = XContentHelper.convertToMap(XContentType.JSON.xContent(), Strings.toString(diskUsageResponse), false);
        assertMap(
            map,
            matchesMap().extraOk()
                .entry(
                    indexName,
                    matchesMap().extraOk()
                        .entry(
                            "fields",
                            matchesMap().extraOk()
                                .entry("_id", matchesMap().extraOk().entry("stored_fields_in_bytes", greaterThanOrEqualTo(1)))
                        )
                )
        );

        // Check that the minimum retaining seqno has advanced, otherwise _id (and recovery source) doesn't get trimmed away.
        var finalIndexName = indexName;
        assertBusy(() -> {
            var r = client().admin().indices().stats(new IndicesStatsRequest().indices(dataStreamName).all()).actionGet();
            var retentionLeasesStats = r.getIndices().get(finalIndexName).getIndexShards().get(0).getShards()[0].getRetentionLeaseStats();
            assertThat(retentionLeasesStats.retentionLeases().leases(), hasSize(1));
            assertThat(
                retentionLeasesStats.retentionLeases().leases().iterator().next().retainingSequenceNumber(),
                equalTo((long) numBulkRequests * numDocsPerBulk + 1)
            );
        });

        // Force merge should trim the _id stored field away for all segments:
        var forceMergeResponse = client().admin().indices().forceMerge(new ForceMergeRequest(dataStreamName).maxNumSegments(1)).actionGet();
        assertThat(forceMergeResponse.getTotalShards(), equalTo(1));
        assertThat(forceMergeResponse.getSuccessfulShards(), equalTo(1));
        assertThat(forceMergeResponse.getFailedShards(), equalTo(0));

        // Check whether we really end up with 1 segment:
        getSegmentsResponse = client().admin().indices().segments(new IndicesSegmentsRequest(dataStreamName)).actionGet();
        assertThat(getSegmentsResponse.getIndices().get(indexName).getShards().get(0).shards()[0].getSegments(), hasSize(1));

        // Check the _id stored field uses no disk space:
        diskUsageResponse = client().execute(
            TransportAnalyzeIndexDiskUsageAction.TYPE,
            new AnalyzeIndexDiskUsageRequest(new String[] { dataStreamName }, AnalyzeIndexDiskUsageRequest.DEFAULT_INDICES_OPTIONS, true)
        ).actionGet();
        map = XContentHelper.convertToMap(XContentType.JSON.xContent(), Strings.toString(diskUsageResponse), false);
        assertMap(
            map,
            matchesMap().extraOk()
                .entry(
                    indexName,
                    matchesMap().extraOk()
                        .entry(
                            "fields",
                            matchesMap().extraOk().entry("_id", matchesMap().extraOk().entry("stored_fields_in_bytes", equalTo(0)))
                        )
                )
        );

        // Check the search api can synthesize _id
        final String idxName = indexName;
        var searchRequest = new SearchRequest(dataStreamName);
        searchRequest.source().trackTotalHits(true);
        assertResponse(client().search(searchRequest), searchResponse -> {
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) numBulkRequests * numDocsPerBulk));
            String id = searchResponse.getHits().getHits()[0].getId();
            assertThat(id, notNullValue());

            // Check that the _id is gettable:
            var getResponse = client().get(new GetRequest(idxName).id(id)).actionGet();
            assertThat(getResponse.isExists(), is(true));
            assertThat(getResponse.getId(), equalTo(id));
        });
    }

    public void testReindexing() throws Exception {
        String dataStreamName = "my-ds";
        String reindexedDataStreamName = "my-reindexed-ds";
        var putTemplateRequest = new TransportPutComposableIndexTemplateAction.Request("id");
        putTemplateRequest.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(dataStreamName, reindexedDataStreamName))
                .template(
                    new Template(
                        Settings.builder().put("index.mode", "time_series").build(),
                        new CompressedXContent(MAPPING_TEMPLATE),
                        null
                    )
                )
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                .build()
        );
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, putTemplateRequest));

        // index doc
        long docCount = randomLongBetween(10, 50);
        Instant startTime = Instant.now();
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < docCount; i++) {
            IndexRequest indexRequest = new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE);
            indexRequest.source(DOC.replace("$time", formatInstant(startTime.plusSeconds(i))), XContentType.JSON);
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        assertThat(bulkResponse.hasFailures(), is(false));

        BulkByScrollResponse reindexResponse = safeGet(
            client().execute(
                ReindexAction.INSTANCE,
                new ReindexRequest().setSourceIndices(dataStreamName).setDestIndex(reindexedDataStreamName).setDestOpType("create")
            )
        );
        assertThat(reindexResponse.getCreated(), equalTo(docCount));

        GetIndexResponse getIndexResponse = safeGet(
            indicesAdmin().getIndex(new GetIndexRequest().indices(dataStreamName, reindexedDataStreamName))
        );
        assertThat(getIndexResponse.getIndices().length, equalTo(2));
        var index1 = getIndexResponse.getIndices()[0];
        var index2 = getIndexResponse.getIndices()[1];
        assertThat(getIndexResponse.getSetting(index1, IndexSettings.MODE.getKey()), equalTo(IndexMode.TIME_SERIES.getName()));
        assertThat(getIndexResponse.getSetting(index2, IndexSettings.MODE.getKey()), equalTo(IndexMode.TIME_SERIES.getName()));
        assertThat(
            getIndexResponse.getSetting(index2, IndexMetadata.INDEX_ROUTING_PATH.getKey()),
            equalTo(getIndexResponse.getSetting(index1, IndexMetadata.INDEX_ROUTING_PATH.getKey()))
        );
    }

    static String formatInstant(Instant instant) {
        return DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(instant);
    }

}
