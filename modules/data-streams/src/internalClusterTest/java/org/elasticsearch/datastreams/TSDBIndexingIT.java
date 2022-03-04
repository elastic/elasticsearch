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
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;

public class TSDBIndexingIT extends ESSingleNodeTestCase {

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
        Settings templateSettings = Settings.builder().put("index.routing_path", "metricset").build();
        var request = new PutComposableIndexTemplateAction.Request("id");
        request.indexTemplate(
            new ComposableIndexTemplate(
                List.of("k8s*"),
                new Template(templateSettings, new CompressedXContent(mappingTemplate), null),
                null,
                null,
                null,
                null,
                new ComposableIndexTemplate.DataStreamTemplate(false, false, IndexMode.TIME_SERIES),
                null
            )
        );
        client().execute(PutComposableIndexTemplateAction.INSTANCE, request).actionGet();

        // index doc
        Instant time = Instant.now();
        String backingIndexName;
        {
            var indexRequest = new IndexRequest("k8s").opType(DocWriteRequest.OpType.CREATE);
            indexRequest.source(DOC.replace("$time", formatInstant(time)), XContentType.JSON);
            var indexResponse = client().index(indexRequest).actionGet();
            backingIndexName = indexResponse.getIndex();
        }

        // fetch start and end time
        var getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices(backingIndexName)).actionGet();
        Instant startTime = IndexSettings.TIME_SERIES_START_TIME.get(getIndexResponse.getSettings().get(backingIndexName));
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

        // Check whether the document lands in the newest backing index:
        time = Instant.ofEpochMilli(randomLongBetween(newStartTime.toEpochMilli(), newEndTime.toEpochMilli() - 1));
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

    static String formatInstant(Instant instant) {
        return DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(instant);
    }

}
