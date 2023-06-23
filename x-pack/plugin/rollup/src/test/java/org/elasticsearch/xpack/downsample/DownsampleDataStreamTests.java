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
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.downsample.DownsampleAction;
import org.elasticsearch.xpack.core.downsample.DownsampleConfig;
import org.elasticsearch.xpack.rollup.Rollup;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.hamcrest.Matchers.equalTo;

public class DownsampleDataStreamTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(Rollup.class, DataStreamsPlugin.class);
    }

    public void testDataStreamDownsample() throws ExecutionException, InterruptedException, IOException {
        // GIVEN
        final String dataStreamName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        putComposableIndexTemplate("1", List.of(dataStreamName));
        client().execute(CreateDataStreamAction.INSTANCE, new CreateDataStreamAction.Request(dataStreamName)).actionGet();
        indexDocs(dataStreamName, 10, Instant.now().toEpochMilli());
        final RolloverResponse rolloverResponse = indicesAdmin().rolloverIndex(new RolloverRequest(dataStreamName, null)).get();
        // NOTE: here we calculate a delay to index documents because the next data stream write index is created with a start time of
        // (about) two hours in the future. As a result, we need to have documents whose @timestamp is in the future to avoid documents
        // being indexed in the old data stream backing index.
        final String newIndexStartTime = indicesAdmin().prepareGetSettings(rolloverResponse.getNewIndex())
            .get()
            .getSetting(rolloverResponse.getNewIndex(), IndexSettings.TIME_SERIES_START_TIME.getKey());
        indexDocs(dataStreamName, 10, Instant.parse(newIndexStartTime).toEpochMilli());
        indicesAdmin().updateSettings(
            new UpdateSettingsRequest().indices(rolloverResponse.getOldIndex())
                .settings(Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true).build())
        ).actionGet();

        // WHEN (simulate downsampling as done by an ILM action)
        final String downsampleTargetIndex = DataStream.BACKING_INDEX_PREFIX + dataStreamName + "-downsample-1h";
        final DownsampleAction.Request downsampleRequest = new DownsampleAction.Request(
            rolloverResponse.getOldIndex(),
            downsampleTargetIndex,
            new DownsampleConfig(DateHistogramInterval.HOUR)
        );
        final AcknowledgedResponse downsampleResponse = indicesAdmin().execute(DownsampleAction.INSTANCE, downsampleRequest).actionGet();

        /*
         * Force an index update to avoid failing with "Index updates are expected as index settings version has changed",
         * due to a possible bug while checking settings versions and actual settings/metadata changes.
         * See {@link IndexSettings#updateIndexMetadata}.
         */
        indicesAdmin().updateSettings(
            new UpdateSettingsRequest().indices(downsampleTargetIndex)
                .settings(Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, false).build())
        ).actionGet();

        final ModifyDataStreamsAction.Request modifyDataStreamRequest = new ModifyDataStreamsAction.Request(
            List.of(
                DataStreamAction.removeBackingIndex(dataStreamName, rolloverResponse.getOldIndex()),
                DataStreamAction.addBackingIndex(dataStreamName, downsampleTargetIndex)
            )
        );
        client().execute(ModifyDataStreamsAction.INSTANCE, modifyDataStreamRequest).actionGet();

        // THEN
        assertThat(downsampleResponse.isAcknowledged(), equalTo(true));
        final GetDataStreamAction.Response getDataStreamActionResponse = indicesAdmin().execute(
            GetDataStreamAction.INSTANCE,
            new GetDataStreamAction.Request(new String[] { dataStreamName })
        ).actionGet();
        assertThat(getDataStreamActionResponse.getDataStreams().get(0).getDataStream().getIndices().size(), equalTo(2));
        final List<String> backingIndices = getDataStreamActionResponse.getDataStreams()
            .get(0)
            .getDataStream()
            .getIndices()
            .stream()
            .map(Index::getName)
            .toList();
        assertThat(backingIndices, Matchers.contains(downsampleTargetIndex, rolloverResponse.getNewIndex()));

        final SearchRequest searchRequest = new SearchRequest().indices(dataStreamName)
            .source(
                new SearchSourceBuilder().size(20)
                    .query(new MatchAllQueryBuilder())
                    .sort(SortBuilders.fieldSort("@timestamp").order(SortOrder.DESC))
                    .aggregation(
                        new DateHistogramAggregationBuilder("dateHistogram").field("@timestamp").fixedInterval(DateHistogramInterval.MINUTE)
                    )
            );
        final SearchResponse searchResponse = client().search(searchRequest).actionGet();
        Arrays.stream(searchResponse.getHits().getHits())
            .limit(10)
            .forEach(hit -> assertThat(hit.getIndex(), equalTo(rolloverResponse.getNewIndex())));
        assertThat(searchResponse.getHits().getHits()[10].getIndex(), equalTo(downsampleTargetIndex));
        final InternalDateHistogram dateHistogram = searchResponse.getAggregations().get("dateHistogram");
        // NOTE: due to unpredictable values for the @timestamp field we don't know how many buckets we have in the
        // date histogram. We know, anyway, that we will have 10 documents in the first two buckets, 10 documents in the last two buckets.
        // The actual number of documents on each of the first two and last two buckets depends on the timestamp value generated when
        // indexing
        // documents, which might cross the minute boundary of the fixed_interval date histogram aggregation.
        // Then we check there is a variable number of intermediate buckets with exactly 0 documents. This is a result of the way
        // downsampling
        // deals with a fixed interval granularity that is larger than the date histogram fixed interval (1 minute (date histogram
        // fixed_interval)
        // < 1 hour (downsample fixed_interval)).
        final int totalBuckets = dateHistogram.getBuckets().size();
        assertThat(dateHistogram.getBuckets().get(0).getDocCount() + dateHistogram.getBuckets().get(1).getDocCount(), equalTo(10L));
        dateHistogram.getBuckets()
            .stream()
            .skip(2)
            .limit(totalBuckets - 3)
            .map(InternalDateHistogram.Bucket::getDocCount)
            .toList()
            .forEach(docCount -> assertThat(docCount, equalTo(0L)));
        assertThat(
            dateHistogram.getBuckets().get(totalBuckets - 2).getDocCount() + dateHistogram.getBuckets().get(totalBuckets - 1).getDocCount(),
            equalTo(10L)
        );
    }

    private void putComposableIndexTemplate(final String id, final List<String> patterns) throws IOException {
        final PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request(id);
        final Template template = new Template(
            indexSettings(1, 0).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
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

    private void indexDocs(final String dataStream, int numDocs, long startTime) {
        final BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            final String timestamp = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(startTime + i);
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
        final RefreshResponse refreshResponse = indicesAdmin().refresh(new RefreshRequest(dataStream)).actionGet();
        assertThat(refreshResponse.getStatus().getStatus(), equalTo(RestStatus.OK.getStatus()));
    }
}
