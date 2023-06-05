/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.xcontent.XContentType;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

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
        return List.of(DataStreamsPlugin.class);
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
            var request = new PutComposableIndexTemplateAction.Request("id");
            request.indexTemplate(
                new ComposableIndexTemplate(
                    List.of("k8s*"),
                    new Template(templateSettings.build(), mapping, null),
                    null,
                    null,
                    null,
                    null,
                    new ComposableIndexTemplate.DataStreamTemplate(false, false),
                    null
                )
            );
            client().execute(PutComposableIndexTemplateAction.INSTANCE, request).actionGet();
        } else {
            var putComponentTemplateRequest = new PutComponentTemplateAction.Request("1");
            putComponentTemplateRequest.componentTemplate(new ComponentTemplate(new Template(null, mapping, null), null, null));
            client().execute(PutComponentTemplateAction.INSTANCE, putComponentTemplateRequest).actionGet();

            var putTemplateRequest = new PutComposableIndexTemplateAction.Request("id");
            putTemplateRequest.indexTemplate(
                new ComposableIndexTemplate(
                    List.of("k8s*"),
                    new Template(templateSettings.build(), null, null),
                    List.of("1"),
                    null,
                    null,
                    null,
                    new ComposableIndexTemplate.DataStreamTemplate(false, false),
                    null
                )
            );
            client().execute(PutComposableIndexTemplateAction.INSTANCE, putTemplateRequest).actionGet();
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
        var getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices(backingIndexName)).actionGet();
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
            expectThrows(IllegalArgumentException.class, () -> client().index(indexRequest).actionGet());
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
        var rolloverResponse = client().admin().indices().rolloverIndex(rolloverRequest).actionGet();
        var newBackingIndexName = rolloverResponse.getNewIndex();

        // index and check target index is new
        getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices(newBackingIndexName)).actionGet();
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
            var request = new PutComposableIndexTemplateAction.Request("id");
            request.indexTemplate(
                new ComposableIndexTemplate(
                    List.of("k8s*"),
                    new Template(
                        Settings.builder().put("index.mode", "time_series").put("index.routing_path", "metricset").build(),
                        new CompressedXContent(mappingTemplate),
                        null
                    ),
                    null,
                    null,
                    null,
                    null,
                    new ComposableIndexTemplate.DataStreamTemplate(false, false),
                    null
                )
            );
            var e = expectThrows(
                IllegalArgumentException.class,
                () -> client().execute(PutComposableIndexTemplateAction.INSTANCE, request).actionGet()
            );
            assertThat(
                e.getCause().getCause().getMessage(),
                equalTo(
                    "All fields that match routing_path must be keywords with [time_series_dimension: true] "
                        + "or flattened fields with a list of dimensions in [time_series_dimensions] and "
                        + "without the [script] parameter. [metricset] was not a dimension."
                )
            );
        }
        {
            var request = new PutComposableIndexTemplateAction.Request("id");
            request.indexTemplate(
                new ComposableIndexTemplate(
                    List.of("k8s*"),
                    new Template(
                        Settings.builder().put("index.mode", "time_series").build(),
                        new CompressedXContent(mappingTemplate),
                        null
                    ),
                    null,
                    null,
                    null,
                    null,
                    new ComposableIndexTemplate.DataStreamTemplate(false, false),
                    null
                )
            );
            var e = expectThrows(
                InvalidIndexTemplateException.class,
                () -> client().execute(PutComposableIndexTemplateAction.INSTANCE, request).actionGet()
            );
            assertThat(e.getMessage(), containsString("[index.mode=time_series] requires a non-empty [index.routing_path]"));
        }
    }

    public void testInvalidTsdbTemplatesNoKeywordFieldType() throws Exception {
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
        var request = new PutComposableIndexTemplateAction.Request("id");
        request.indexTemplate(
            new ComposableIndexTemplate(
                List.of("k8s*"),
                new Template(
                    Settings.builder().put("index.mode", "time_series").put("index.routing_path", "metricset").build(),
                    new CompressedXContent(mappingTemplate),
                    null
                ),
                null,
                null,
                null,
                null,
                new ComposableIndexTemplate.DataStreamTemplate(false, false),
                null
            )
        );
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> client().execute(PutComposableIndexTemplateAction.INSTANCE, request).actionGet()
        );
        assertThat(
            e.getCause().getCause().getMessage(),
            equalTo(
                "All fields that match routing_path must be keywords with [time_series_dimension: true] "
                    + "or flattened fields with a list of dimensions in [time_series_dimensions] and "
                    + "without the [script] parameter. [metricset] was [long]."
            )
        );
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
        var request = new PutComposableIndexTemplateAction.Request("id");
        request.indexTemplate(
            new ComposableIndexTemplate(
                List.of("k8s*"),
                new Template(
                    Settings.builder().put("index.routing_path", "metricset").build(),
                    new CompressedXContent(mappingTemplate),
                    null
                ),
                null,
                null,
                null,
                null,
                new ComposableIndexTemplate.DataStreamTemplate(false, false),
                null
            )
        );
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> client().execute(PutComposableIndexTemplateAction.INSTANCE, request).actionGet()
        );
        assertThat(e.getCause().getMessage(), equalTo("[index.routing_path] requires [index.mode=time_series]"));
    }

    public void testSkippingShards() throws Exception {
        Instant time = Instant.now();
        var mapping = new CompressedXContent(randomBoolean() ? MAPPING_TEMPLATE : MAPPING_TEMPLATE.replace("date", "date_nanos"));
        {
            var templateSettings = Settings.builder().put("index.mode", "time_series").put("index.routing_path", "metricset").build();
            var request = new PutComposableIndexTemplateAction.Request("id1");
            request.indexTemplate(
                new ComposableIndexTemplate(
                    List.of("pattern-1"),
                    new Template(templateSettings, mapping, null),
                    null,
                    null,
                    null,
                    null,
                    new ComposableIndexTemplate.DataStreamTemplate(false, false),
                    null
                )
            );
            client().execute(PutComposableIndexTemplateAction.INSTANCE, request).actionGet();
            var indexRequest = new IndexRequest("pattern-1").opType(DocWriteRequest.OpType.CREATE).setRefreshPolicy("true");
            indexRequest.source(DOC.replace("$time", formatInstant(time)), XContentType.JSON);
            client().index(indexRequest).actionGet();
        }
        {
            var request = new PutComposableIndexTemplateAction.Request("id2");
            request.indexTemplate(
                new ComposableIndexTemplate(
                    List.of("pattern-2"),
                    new Template(null, mapping, null),
                    null,
                    null,
                    null,
                    null,
                    new ComposableIndexTemplate.DataStreamTemplate(false, false),
                    null
                )
            );
            client().execute(PutComposableIndexTemplateAction.INSTANCE, request).actionGet();
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
            var searchResponse = client().search(searchRequest).actionGet();
            ElasticsearchAssertions.assertHitCount(searchResponse, 2);
            assertThat(searchResponse.getTotalShards(), equalTo(2));
            assertThat(searchResponse.getSkippedShards(), equalTo(0));
            assertThat(searchResponse.getSuccessfulShards(), equalTo(2));
        }
        {
            var nonMatchingRange = new SearchSourceBuilder().query(
                new RangeQueryBuilder("@timestamp").from(time.minus(2, ChronoUnit.DAYS).toEpochMilli())
                    .to(time.minus(1, ChronoUnit.DAYS).toEpochMilli())
            );
            var searchRequest = new SearchRequest("pattern-*");
            searchRequest.setPreFilterShardSize(1);
            searchRequest.source(nonMatchingRange);
            var searchResponse = client().search(searchRequest).actionGet();
            ElasticsearchAssertions.assertNoSearchHits(searchResponse);
            assertThat(searchResponse.getTotalShards(), equalTo(2));
            assertThat(searchResponse.getSkippedShards(), equalTo(1));
            assertThat(searchResponse.getSuccessfulShards(), equalTo(2));
        }
    }

    static String formatInstant(Instant instant) {
        return DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(instant);
    }

}
