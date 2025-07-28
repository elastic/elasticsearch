/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.downsample.DownsampleAction;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.downsample.DownsampleDataStreamTests.TIMEOUT;

public class DownsampleIT extends DownsamplingIntegTestCase {

    public void testDownsamplingPassthroughDimensions() throws Exception {
        String dataStreamName = "metrics-foo";
        // Set up template
        putTSDBIndexTemplate("my-template", List.of("metrics-foo"), null, """
            {
              "properties": {
                "attributes": {
                  "type": "passthrough",
                  "priority": 10,
                  "time_series_dimension": true
                },
                "metrics.cpu_usage": {
                  "type": "double",
                  "time_series_metric": "counter"
                }
              }
            }
            """, null, null);

        // Create data stream by indexing documents
        final Instant now = Instant.now();
        Supplier<XContentBuilder> sourceSupplier = () -> {
            String ts = randomDateForRange(now.minusSeconds(60 * 60).toEpochMilli(), now.plusSeconds(60 * 29).toEpochMilli());
            try {
                return XContentFactory.jsonBuilder()
                    .startObject()
                    .field("@timestamp", ts)
                    .field("attributes.host.name", randomFrom("host1", "host2", "host3"))
                    .field("metrics.cpu_usage", randomDouble())
                    .endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        bulkIndex(dataStreamName, sourceSupplier, 100);
        // Rollover to ensure the index we will downsample is not the write index
        assertAcked(client().admin().indices().rolloverIndex(new RolloverRequest(dataStreamName, null)));
        List<String> backingIndices = waitForDataStreamBackingIndices(dataStreamName, 2);
        String sourceIndex = backingIndices.get(0);
        String interval = "5m";
        String targetIndex = "downsample-" + interval + "-" + sourceIndex;
        // Set the source index to read-only state
        assertAcked(
            indicesAdmin().prepareUpdateSettings(sourceIndex)
                .setSettings(Settings.builder().put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true).build())
        );

        DownsampleConfig downsampleConfig = new DownsampleConfig(new DateHistogramInterval(interval));
        assertAcked(
            client().execute(
                DownsampleAction.INSTANCE,
                new DownsampleAction.Request(TEST_REQUEST_TIMEOUT, sourceIndex, targetIndex, TIMEOUT, downsampleConfig)
            )
        );

        // Wait for downsampling to complete
        SubscribableListener<Void> listener = ClusterServiceUtils.addMasterTemporaryStateListener(clusterState -> {
            final var indexMetadata = clusterState.metadata().index(targetIndex);
            if (indexMetadata == null) {
                return false;
            }
            var downsampleStatus = IndexMetadata.INDEX_DOWNSAMPLE_STATUS.get(indexMetadata.getSettings());
            return downsampleStatus == IndexMetadata.DownsampleTaskStatus.SUCCESS;
        });
        safeAwait(listener);

        assertDownsampleIndexFieldsAndDimensions(sourceIndex, targetIndex, downsampleConfig);
    }
}
