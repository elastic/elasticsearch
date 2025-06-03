/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.datastreams.ModifyDataStreamsAction;
import org.elasticsearch.action.downsample.DownsampleAction;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.metadata.DataStreamAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.downsample.DownsampleDataStreamTests.TIMEOUT;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

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
            final var indexMetadata = clusterState.metadata().getProject().index(targetIndex);
            if (indexMetadata == null) {
                return false;
            }
            var downsampleStatus = IndexMetadata.INDEX_DOWNSAMPLE_STATUS.get(indexMetadata.getSettings());
            return downsampleStatus == IndexMetadata.DownsampleTaskStatus.SUCCESS;
        });
        safeAwait(listener);

        assertDownsampleIndexFieldsAndDimensions(sourceIndex, targetIndex, downsampleConfig);
    }

    public void testEsqlTSAfterDownsampling() throws Exception {
        String dataStreamName = "metrics-foo";
        Settings settings = Settings.builder().put("mode", "time_series").putList("routing_path", List.of("host", "cluster")).build();
        putTSDBIndexTemplate("my-template", List.of("metrics-foo"), settings, """
            {
              "properties": {
                "host": {
                  "type": "keyword",
                  "time_series_dimension": true
                },
                "cluster" : {
                  "type": "keyword",
                  "time_series_dimension": true
                },
                "cpu": {
                  "type": "double",
                  "time_series_metric": "gauge"
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
                    .field("host", randomFrom("host1", "host2", "host3"))
                    .field("cluster", randomFrom("cluster1", "cluster2", "cluster3"))
                    .field("cpu", randomDouble())
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
            final var indexMetadata = clusterState.metadata().getProject().index(targetIndex);
            if (indexMetadata == null) {
                return false;
            }
            var downsampleStatus = IndexMetadata.INDEX_DOWNSAMPLE_STATUS.get(indexMetadata.getSettings());
            return downsampleStatus == IndexMetadata.DownsampleTaskStatus.SUCCESS;
        });
        safeAwait(listener);

        assertDownsampleIndexFieldsAndDimensions(sourceIndex, targetIndex, downsampleConfig);

        // remove old backing index and replace with downsampled index and delete old so old is not queried
        assertAcked(
            client().execute(
                ModifyDataStreamsAction.INSTANCE,
                new ModifyDataStreamsAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    List.of(
                        DataStreamAction.removeBackingIndex(dataStreamName, sourceIndex),
                        DataStreamAction.addBackingIndex(dataStreamName, targetIndex)
                    )
                )
            ).actionGet()
        );
        assertAcked(client().execute(TransportDeleteIndexAction.TYPE, new DeleteIndexRequest(sourceIndex)).actionGet());

        // index to the next backing index
        Supplier<XContentBuilder> nextSourceSupplier = () -> {
            String ts = randomDateForRange(now.plusSeconds(60 * 31).toEpochMilli(), now.plusSeconds(60 * 59).toEpochMilli());
            try {
                return XContentFactory.jsonBuilder()
                    .startObject()
                    .field("@timestamp", ts)
                    .field("host", randomFrom("host1", "host2", "host3"))
                    .field("cluster", randomFrom("cluster1", "cluster2", "cluster3"))
                    .field("cpu", randomDouble())
                    .endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        bulkIndex(dataStreamName, nextSourceSupplier, 100);

        // Since the downsampled field (cpu) is downsampled in one index and not in the other, we want to confirm
        // first that the field is unsupported and has 2 original types - double and aggregate_metric_double
        try (var resp = esqlCommand("TS " + dataStreamName + " | KEEP @timestamp, host, cluster, cpu")) {
            var columns = resp.columns();
            assertThat(columns, hasSize(4));
            assertThat(
                resp.columns(),
                equalTo(
                    List.of(
                        new ColumnInfoImpl("@timestamp", "date", null),
                        new ColumnInfoImpl("host", "keyword", null),
                        new ColumnInfoImpl("cluster", "keyword", null),
                        new ColumnInfoImpl("cpu", "unsupported", List.of("aggregate_metric_double", "double"))
                    )
                )
            );

        }
        // test that implicit casting within time aggregation query works
        try (
            var resp = esqlCommand(
                "TS "
                    + dataStreamName
                    + " | STATS min = sum(min_over_time(cpu)), max = sum(max_over_time(cpu)) by cluster, bucket(@timestamp, 1 hour)"
            )
        ) {
            var columns = resp.columns();
            assertThat(columns, hasSize(4));
            assertThat(
                resp.columns(),
                equalTo(
                    List.of(
                        new ColumnInfoImpl("min", "double", null),
                        new ColumnInfoImpl("max", "double", null),
                        new ColumnInfoImpl("cluster", "keyword", null),
                        new ColumnInfoImpl("bucket(@timestamp, 1 hour)", "date", null)
                    )
                )
            );
            // TODO: verify the numbers are accurate
        }
    }

    private EsqlQueryResponse esqlCommand(String command) throws IOException {
        if (command.toLowerCase(Locale.ROOT).contains("limit") == false) {
            // add a (high) limit to avoid warnings on default limit
            command += " | limit 10000000";
        }
        return client().execute(EsqlQueryAction.INSTANCE, new EsqlQueryRequest().query(command)).actionGet(30, TimeUnit.SECONDS);
    }
}
