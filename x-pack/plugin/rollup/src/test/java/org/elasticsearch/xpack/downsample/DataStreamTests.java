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
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.downsample.DownsampleAction;
import org.elasticsearch.xpack.core.downsample.DownsampleConfig;
import org.elasticsearch.xpack.rollup.Rollup;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamTests extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(Rollup.class, DataStreamsPlugin.class);
    }

    public void testDataStreamDownsample() throws ExecutionException, InterruptedException, IOException {
        // Create the data stream
        final String dataStreamName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        final DataStream dataStream = new DataStream(
            dataStreamName,
            List.of(
                new Index(DataStream.getDefaultBackingIndexName(dataStreamName, 1, Instant.now().toEpochMilli()), UUIDs.randomBase64UUID())
            ),
            1,
            null,
            false,
            false,
            false,
            false,
            IndexMode.TIME_SERIES
        );
        final Tuple<String, Long> newCoordinates = dataStream.nextWriteIndexAndGeneration(Metadata.EMPTY_METADATA);

        // Index a few documents
        indexDocs(dataStream.getName(), 10);

        // Rollover
        final DataStream newDataStream = dataStream.rollover(
            new Index(newCoordinates.v1(), UUIDs.randomBase64UUID()),
            newCoordinates.v2(),
            true
        );
        assertEquals(2, newDataStream.getIndices().size());

        // Index a few documents
        indexDocs(dataStream.getName(), 10);

        final String oldIndex = newDataStream.getIndices().get(0).getName();
        // final String newIndex = newDataStream.getIndices().get(1).getName();

        // Downsample old rollover index
        final String targetIndex = DataStream.BACKING_INDEX_PREFIX + dataStream.getName() + "-downsample-1h";
        final DownsampleAction.Request downsampleRequest = new DownsampleAction.Request(
            oldIndex,
            targetIndex,
            new DownsampleConfig(DateHistogramInterval.HOUR)
        );
        final AcknowledgedResponse downsampleResponse = client().admin()
            .indices()
            .execute(DownsampleAction.INSTANCE, downsampleRequest)
            .actionGet();
        assertTrue(downsampleResponse.isAcknowledged());

        // Replace old data stream index with downsample target index
        // final ModifyDataStreamsAction.Request modifyDataStreamRequest = new ModifyDataStreamsAction.Request(
        // List.of(
        // DataStreamAction.removeBackingIndex(newDataStream.getName(), oldIndex)
        // //DataStreamAction.addBackingIndex(dataStream.getName(), targetIndex)
        // )
        // );
        // final AcknowledgedResponse modifyDataStreamResponse = client().execute(ModifyDataStreamsAction.INSTANCE, modifyDataStreamRequest)
        // .actionGet();
        // assertTrue(modifyDataStreamResponse.isAcknowledged());

        final SearchRequest searchRequest = new SearchRequest().indices(dataStreamName)
            .source(new SearchSourceBuilder().size(100).query(new MatchAllQueryBuilder()));
        final SearchResponse searchResponse = client().search(searchRequest).actionGet();
        assertEquals(20, searchResponse.getHits().getHits().length);
    }

    static void indexDocs(final String dataStream, int numDocs) {
        final BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            final String value = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
            bulkRequest.add(
                new IndexRequest(dataStream).opType(DocWriteRequest.OpType.CREATE)
                    .source(String.format(Locale.ROOT, "{\"%s\":\"%s\"}", DEFAULT_TIMESTAMP_FIELD, value), XContentType.JSON)
            );
        }
        final BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        final BulkItemResponse[] items = bulkResponse.getItems();
        assertThat(items.length, equalTo(numDocs));
        assertFalse(bulkResponse.hasFailures());
        final RefreshResponse refreshResponse = client().admin().indices().refresh(new RefreshRequest(dataStream)).actionGet();
        assertEquals(RestStatus.OK.getStatus(), refreshResponse.getStatus().getStatus());
    }
}
