/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.datastreams.ModifyDataStreamsAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.downsample.DownsampleAction;
import org.elasticsearch.xpack.core.downsample.DownsampleConfig;
import org.elasticsearch.xpack.rollup.Rollup;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamTests extends ESIntegTestCase {

    private static final long A_BIT_MORE_THAN_3_HOURS_MILLIS = 10_000_000L;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(Rollup.class, DataStreamsPlugin.class);
    }

    public void testDataStreamDownsample() throws ExecutionException, InterruptedException, IOException {
        // GIVEN
        final String dataStreamName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        putComposableIndexTemplate("1", List.of(dataStreamName));
        final CreateDataStreamAction.Request request = new CreateDataStreamAction.Request(dataStreamName);
        client().execute(CreateDataStreamAction.INSTANCE, request).actionGet();
        indexDocs(dataStreamName, 10, 0L);
        final RolloverResponse rolloverResponse = client().admin().indices().rolloverIndex(new RolloverRequest(dataStreamName, null)).get();
        // NOTE: here delay = 10_000_000 because the next data stream write index is created with a start time of (about) two hours in the
        // future.
        // 10_000_000 milliseconds is a bit more than 3 hours, which is more than enough to make sure we are not writing documents to the
        // old data stream index.
        indexDocs(dataStreamName, 10, A_BIT_MORE_THAN_3_HOURS_MILLIS);
        client().admin()
            .indices()
            .updateSettings(
                new UpdateSettingsRequest().indices(rolloverResponse.getOldIndex())
                    .settings(Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true).build())
            )
            .actionGet();

        // WHEN (simulate downsampling as done by an ILM action)
        final String downsampleTargetIndex = DataStream.BACKING_INDEX_PREFIX + dataStreamName + "-downsample-1h";
        final DownsampleAction.Request downsampleRequest = new DownsampleAction.Request(
            rolloverResponse.getOldIndex(),
            downsampleTargetIndex,
            new DownsampleConfig(DateHistogramInterval.HOUR)
        );
        final AcknowledgedResponse downsampleResponse = client().admin()
            .indices()
            .execute(DownsampleAction.INSTANCE, downsampleRequest)
            .actionGet();

        client().admin()
            .indices()
            .updateSettings(
                new UpdateSettingsRequest().indices(downsampleTargetIndex)
                    .settings(Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, false).build())
            )
            .actionGet();

        final ModifyDataStreamsAction.Request modifyDataStreamRequest = new ModifyDataStreamsAction.Request(
            List.of(
                DataStreamAction.removeBackingIndex(dataStreamName, rolloverResponse.getOldIndex()),
                DataStreamAction.addBackingIndex(dataStreamName, downsampleTargetIndex)
            )
        );
        client().execute(ModifyDataStreamsAction.INSTANCE, modifyDataStreamRequest).actionGet();

        // THEN
        assertThat(downsampleResponse.isAcknowledged(), equalTo(true));
        final GetDataStreamAction.Response getDataStreamActionResponse = client().admin()
            .indices()
            .execute(GetDataStreamAction.INSTANCE, new GetDataStreamAction.Request(new String[] { dataStreamName }))
            .actionGet();
        assertThat(getDataStreamActionResponse.getDataStreams().get(0).getDataStream().getIndices().size(), equalTo(2));
        final List<String> backingIndices = getDataStreamActionResponse.getDataStreams()
            .get(0)
            .getDataStream()
            .getIndices()
            .stream()
            .map(Index::getName)
            .toList();
        assertThat(backingIndices, Matchers.containsInAnyOrder(downsampleTargetIndex, rolloverResponse.getNewIndex()));

        final SearchRequest searchRequest = new SearchRequest().indices(dataStreamName)
            .source(
                new SearchSourceBuilder().size(0)
                    .query(new MatchAllQueryBuilder())
                    .aggregation(
                        new DateHistogramAggregationBuilder("dateHistogram").field("@timestamp").fixedInterval(DateHistogramInterval.MINUTE)
                    )
            );
        final SearchResponse searchResponse = client().search(searchRequest).actionGet();
        final InternalDateHistogram dateHistogram = searchResponse.getAggregations().get("dateHistogram");
        // NOTE: due to unpredictable values for the @timestamp field we don't know how many buckets we have in the
        // date histogram. We know, anyway, that we will have 10 documents in the first bucket, 10 documents in the last bucket
        // and a variable number of intermediate buckets with exactly 0 documents. This is a result of the way downsampling deals
        // with a fixed interval granularity that is larger than the date histogram fixed interval (1 minute (date histogram fixed_interval)
        // < 1 hour (downsample fixed_interval)).
        final int totalBuckets = dateHistogram.getBuckets().size();
        assertThat(dateHistogram.getBuckets().get(0).getDocCount(), equalTo(10L));
        dateHistogram.getBuckets()
            .stream()
            .skip(1)
            .limit(totalBuckets - 2)
            .map(InternalDateHistogram.Bucket::getDocCount)
            .toList()
            .forEach(docCount -> { assertThat(docCount, equalTo(0L)); });
        assertThat(dateHistogram.getBuckets().get(totalBuckets - 1).getDocCount(), equalTo(10L));
    }

    static void putComposableIndexTemplate(final String id, final List<String> patterns) throws IOException {
        final PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request(id);
        final Template template = new Template(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of("routing_field"))
                .build(),
            new CompressedXContent("""
                {
                    "properties": {
                        "@timestamp" : {
                            "type": "date"
                        },
                        "routing_field": {
                            "type": "keyword",
                            "time_series_dimension": true
                        },
                        "counter": {
                            "type": "long",
                            "time_series_metric": "counter"
                        }
                    }
                }
                """),
            null
        );
        request.indexTemplate(
            new ComposableIndexTemplate(patterns, template, null, null, null, null, new ComposableIndexTemplate.DataStreamTemplate(), null)
        );
        client().execute(PutComposableIndexTemplateAction.INSTANCE, request).actionGet();
    }

    static void indexDocs(final String dataStream, int numDocs, long delay) {
        final BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            final String timestamp = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis() + i + delay);
            bulkRequest.add(
                new IndexRequest(dataStream).opType(DocWriteRequest.OpType.CREATE)
                    .source(
                        String.format(
                            Locale.ROOT,
                            "{\"%s\":\"%s\",\"%s\":\"%s\",\"%s\":\"%s\"}",
                            DEFAULT_TIMESTAMP_FIELD,
                            timestamp,
                            "routing_field",
                            0,
                            "counter",
                            i + 1
                        ),
                        XContentType.JSON
                    )
            );
        }
        final BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        final BulkItemResponse[] items = bulkResponse.getItems();
        assertThat(items.length, equalTo(numDocs));
        assertThat(bulkResponse.hasFailures(), equalTo(false));
        final RefreshResponse refreshResponse = client().admin().indices().refresh(new RefreshRequest(dataStream)).actionGet();
        assertThat(refreshResponse.getStatus().getStatus(), equalTo(RestStatus.OK.getStatus()));
    }
}
