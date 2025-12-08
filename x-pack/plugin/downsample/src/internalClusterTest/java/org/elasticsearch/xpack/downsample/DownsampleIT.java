/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.action.admin.cluster.node.capabilities.NodesCapabilitiesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.datastreams.ModifyDataStreamsAction;
import org.elasticsearch.action.downsample.DownsampleAction;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.metadata.DataStreamAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestRequest;
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
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.downsample.DownsampleDataStreamTests.TIMEOUT;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.AGGREGATE_METRIC_DOUBLE_V0;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class DownsampleIT extends DownsamplingIntegTestCase {

    public void testDownsamplingPassthroughDimensions() throws Exception {
        String dataStreamName = "metrics-foo";
        String mapping = """
            {
              "properties": {
                "attributes": {
                  "type": "passthrough",
                  "priority": 10,
                  "time_series_dimension": true,
                  "properties": {
                    "os.name": {
                      "type": "keyword",
                      "time_series_dimension": true
                    }
                  }
                },
                "metrics.cpu_usage": {
                  "type": "double",
                  "time_series_metric": "counter"
                }
              }
            }
            """;

        // Create data stream by indexing documents
        final Instant now = Instant.now();
        Supplier<XContentBuilder> sourceSupplier = () -> {
            String ts = randomDateForRange(now.minusSeconds(60 * 60).toEpochMilli(), now.plusSeconds(60 * 29).toEpochMilli());
            try {
                return XContentFactory.jsonBuilder()
                    .startObject()
                    .field("@timestamp", ts)
                    .field("attributes.host.name", randomFrom("host1", "host2", "host3"))
                    .field("attributes.os.name", randomFrom("linux", "windows", "macos"))
                    .field("metrics.cpu_usage", randomDouble())
                    .endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        downsampleAndAssert(dataStreamName, mapping, sourceSupplier, randomSamplingMethod());
    }

    public void testDownsamplingPassthroughMetrics() throws Exception {
        String dataStreamName = "metrics-foo";
        String mapping = """
            {
              "properties": {
                "attributes.os.name": {
                  "type": "keyword",
                  "time_series_dimension": true
                },
                "metrics": {
                  "type": "passthrough",
                  "priority": 10,
                  "properties": {
                    "cpu_usage": {
                        "type": "double",
                        "time_series_metric": "counter"
                    }
                  }
                }
              }
            }
            """;

        // Create data stream by indexing documents
        final Instant now = Instant.now();
        Supplier<XContentBuilder> sourceSupplier = () -> {
            String ts = randomDateForRange(now.minusSeconds(60 * 60).toEpochMilli(), now.plusSeconds(60 * 29).toEpochMilli());
            try {
                return XContentFactory.jsonBuilder()
                    .startObject()
                    .field("@timestamp", ts)
                    .field("attributes.os.name", randomFrom("linux", "windows", "macos"))
                    .field("metrics.cpu_usage", randomDouble())
                    .field("metrics.memory_usage", randomDouble())
                    .endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        downsampleAndAssert(dataStreamName, mapping, sourceSupplier, randomSamplingMethod());
    }

    public void testLastValueMode() throws Exception {
        String dataStreamName = "metrics-foo";
        String mapping = """
            {
              "properties": {
                "attributes": {
                  "type": "passthrough",
                  "priority": 10,
                  "time_series_dimension": true,
                  "properties": {
                    "os.name": {
                      "type": "keyword",
                      "time_series_dimension": true
                    }
                  }
                },
                "metrics.cpu_usage": {
                  "type": "double",
                  "time_series_metric": "gauge"
                },
                "my_labels": {
                  "properties": {
                    "my_histogram": {
                      "type": "histogram"
                    },
                    "my_aggregate": {
                      "type": "aggregate_metric_double",
                      "metrics": [ "min", "max", "sum", "value_count" ],
                      "default_metric": "max"
                    }
                  }
                }
              }
            }
            """;

        // Create data stream by indexing documents
        final Instant now = Instant.now();
        Supplier<XContentBuilder> sourceSupplier = () -> {
            String ts = randomDateForRange(now.minusSeconds(60 * 60).toEpochMilli(), now.plusSeconds(60 * 29).toEpochMilli());
            try {
                int maxHistogramSize = randomIntBetween(2, 10);
                return XContentFactory.jsonBuilder()
                    .startObject()
                    .field("@timestamp", ts)
                    .field("attributes.host.name", randomFrom("host1", "host2", "host3"))
                    .field("attributes.os.name", randomFrom("linux", "windows", "macos"))
                    .field("metrics.cpu_usage", randomDouble())

                    .startObject("my_labels.my_histogram")
                    .array("values", randomHistogramValues(maxHistogramSize))
                    .array("counts", randomHistogramValueCounts(maxHistogramSize))
                    .endObject()

                    .startObject("my_labels.my_aggregate")
                    .field("min", randomFloatBetween(0.0f, 10.0f, true))
                    .field("max", randomFloatBetween(10.0f, 20.0f, true))
                    .field("sum", randomFloatBetween(20.0f, 30.0f, true))
                    .field("value_count", randomIntBetween(1, 10))
                    .endObject()
                    .endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        downsampleAndAssert(dataStreamName, mapping, sourceSupplier, DownsampleConfig.SamplingMethod.LAST_VALUE);
    }

    /**
     * Create a data stream with the provided mapping and downsampled the first backing index of this data stream. After downsampling has
     *  completed, it asserts if the downsampled index is as expected.
     */
    private void downsampleAndAssert(
        String dataStreamName,
        String mapping,
        Supplier<XContentBuilder> sourceSupplier,
        DownsampleConfig.SamplingMethod samplingMethod
    ) throws Exception {
        // Set up template
        putTSDBIndexTemplate("my-template", List.of(dataStreamName), null, mapping, null, null);

        // Create data stream by indexing documents
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

        DownsampleConfig downsampleConfig = new DownsampleConfig(new DateHistogramInterval(interval), samplingMethod);
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

    public void testAggMetricInEsqlTSAfterDownsampling() throws Exception {
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
                },
                "request": {
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
                    .field("host", randomFrom("host1", "host2", "host3"))
                    .field("cluster", randomFrom("cluster1", "cluster2", "cluster3"))
                    .field("cpu", randomDouble())
                    .field("request", randomDoubleBetween(0, 100, true))
                    .endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        bulkIndex(dataStreamName, sourceSupplier, 100);

        DownsampleConfig downsampleConfig = new DownsampleConfig(new DateHistogramInterval("5m"), randomSamplingMethod());
        String secondBackingIndex = rolloverAndDownsample(dataStreamName, downsampleConfig);

        // index to the next backing index; random time between 31 and 59m in the future to because default look_ahead_time is 30m and we
        // don't want to conflict with the previous backing index
        Supplier<XContentBuilder> nextSourceSupplier = () -> {
            String ts = randomDateForRange(now.plusSeconds(60 * 31).toEpochMilli(), now.plusSeconds(60 * 59).toEpochMilli());
            try {
                return XContentFactory.jsonBuilder()
                    .startObject()
                    .field("@timestamp", ts)
                    .field("host", randomFrom("host1", "host2", "host3"))
                    .field("cluster", randomFrom("cluster1", "cluster2", "cluster3"))
                    .field("cpu", randomDouble())
                    .field("request", randomDoubleBetween(0, 100, true))
                    .endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        bulkIndex(dataStreamName, nextSourceSupplier, 100);

        // check that aggregate metric double is available
        var response = clusterAdmin().nodesCapabilities(
            new NodesCapabilitiesRequest().method(RestRequest.Method.POST)
                .path("/_query")
                .capabilities(AGGREGATE_METRIC_DOUBLE_V0.capabilityName())
        ).actionGet();
        assumeTrue("Require aggregate_metric_double casting", response.isSupported().orElse(Boolean.FALSE));

        // Since the downsampled field (cpu) is downsampled in one index and not in the other, we want to confirm
        // first that the field is unsupported and has 2 original types - double and aggregate_metric_double
        try (var resp = esqlCommand("TS " + dataStreamName + " | KEEP @timestamp, host, cluster, cpu, request")) {
            var columns = resp.columns();
            assertThat(columns, hasSize(5));
            assertThat(
                resp.columns(),
                equalTo(
                    List.of(
                        new ColumnInfoImpl("@timestamp", "date", null),
                        new ColumnInfoImpl("host", "keyword", null),
                        new ColumnInfoImpl("cluster", "keyword", null),
                        downsampleConfig.getSamplingMethodOrDefault() == DownsampleConfig.SamplingMethod.LAST_VALUE
                            ? new ColumnInfoImpl("cpu", "double", null)
                            : new ColumnInfoImpl("cpu", "unsupported", List.of("aggregate_metric_double", "double")),
                        new ColumnInfoImpl("request", "counter_double", null)
                    )
                )
            );
        }
        testEsqlMetrics(dataStreamName, secondBackingIndex);
    }

    public void testPartialNullMetricsAfterDownsampling() throws Exception {
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
                },
                "request": {
                  "type": "double",
                  "time_series_metric": "counter"
                }
              }
            }
            """, null, null);

        // Create data stream by indexing documents with no values in numerics
        final Instant now = Instant.now();
        Supplier<XContentBuilder> sourceSupplier = () -> {
            String ts = randomDateForRange(now.minusSeconds(60 * 60).toEpochMilli(), now.minusSeconds(60 * 15).toEpochMilli());
            try {
                return XContentFactory.jsonBuilder()
                    .startObject()
                    .field("@timestamp", ts)
                    .field("host", randomFrom("host1", "host2", "host3"))
                    .field("cluster", randomFrom("cluster1", "cluster2", "cluster3"))
                    .endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        bulkIndex(dataStreamName, sourceSupplier, 100);
        // And index documents with values
        sourceSupplier = () -> {
            String ts = randomDateForRange(now.minusSeconds(60 * 14).toEpochMilli(), now.plusSeconds(60 * 29).toEpochMilli());
            try {
                return XContentFactory.jsonBuilder()
                    .startObject()
                    .field("@timestamp", ts)
                    .field("host", randomFrom("host1", "host2", "host3"))
                    .field("cluster", randomFrom("cluster1", "cluster2", "cluster3"))
                    .field("cpu", randomDouble())
                    .field("request", randomDoubleBetween(0, 100, true))
                    .endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        bulkIndex(dataStreamName, sourceSupplier, 100);
        DownsampleConfig downsampleConfig = new DownsampleConfig(new DateHistogramInterval("5m"), randomSamplingMethod());
        String secondBackingIndex = rolloverAndDownsample(dataStreamName, downsampleConfig);

        Supplier<XContentBuilder> nextSourceSupplier = () -> {
            String ts = randomDateForRange(now.plusSeconds(60 * 31).toEpochMilli(), now.plusSeconds(60 * 59).toEpochMilli());
            try {
                return XContentFactory.jsonBuilder()
                    .startObject()
                    .field("@timestamp", ts)
                    .field("host", randomFrom("host1", "host2", "host3"))
                    .field("cluster", randomFrom("cluster1", "cluster2", "cluster3"))
                    .field("cpu", randomDouble())
                    .field("request", randomDoubleBetween(0, 100, true))
                    .endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        bulkIndex(dataStreamName, nextSourceSupplier, 100);

        // check that aggregate metric double is available
        var response = clusterAdmin().nodesCapabilities(
            new NodesCapabilitiesRequest().method(RestRequest.Method.POST)
                .path("/_query")
                .capabilities(AGGREGATE_METRIC_DOUBLE_V0.capabilityName())
        ).actionGet();
        assumeTrue("Require aggregate_metric_double casting", response.isSupported().orElse(Boolean.FALSE));

        testEsqlMetrics(dataStreamName, secondBackingIndex);
    }

    private void testEsqlMetrics(String dataStreamName, String nonDownsampledIndex) throws Exception {
        // test _over_time commands with implicit casting of aggregate_metric_double
        for (String outerCommand : List.of("min", "max", "sum", "count")) {
            String expectedType = outerCommand.equals("count") ? "long" : "double";
            for (String innerCommand : List.of("min_over_time", "max_over_time", "avg_over_time", "count_over_time")) {
                String command = outerCommand + " (" + innerCommand + "(cpu))";
                try (var resp = esqlCommand("TS " + dataStreamName + " | STATS " + command + " by cluster, bucket(@timestamp, 1 hour)")) {
                    var columns = resp.columns();
                    assertThat(columns, hasSize(3));
                    assertThat(
                        resp.columns(),
                        equalTo(
                            List.of(
                                new ColumnInfoImpl(command, innerCommand.equals("count_over_time") ? "long" : expectedType, null),
                                new ColumnInfoImpl("cluster", "keyword", null),
                                new ColumnInfoImpl("bucket(@timestamp, 1 hour)", "date", null)
                            )
                        )
                    );
                    // TODO: verify the numbers are accurate
                }
            }
            // tests on non-downsampled index
            // TODO: combine with above when support for aggregate_metric_double + implicit casting is added
            // TODO: add to counter tests below when support for counters is added
            for (String innerCommand : List.of("first_over_time", "last_over_time")) {
                String command = outerCommand + " (" + innerCommand + "(cpu))";
                try (
                    var resp = esqlCommand("TS " + nonDownsampledIndex + " | STATS " + command + " by cluster, bucket(@timestamp, 1 hour)")
                ) {
                    var columns = resp.columns();
                    assertThat(columns, hasSize(3));
                    assertThat(
                        "resp is " + resp,
                        columns,
                        equalTo(
                            List.of(
                                new ColumnInfoImpl(command, expectedType, null),
                                new ColumnInfoImpl("cluster", "keyword", null),
                                new ColumnInfoImpl("bucket(@timestamp, 1 hour)", "date", null)
                            )
                        )
                    );
                }
            }

            // tests on counter types
            for (String innerCommand : List.of("rate")) {
                String command = outerCommand + " (" + innerCommand + "(request))";
                String esqlQuery = "TS " + dataStreamName + " | STATS " + command + " by cluster, bucket(@timestamp, 1 hour)";
                try (
                    var resp = client().execute(EsqlQueryAction.INSTANCE, new EsqlQueryRequest().query(esqlQuery))
                        .actionGet(30, TimeUnit.SECONDS)
                ) {
                    var columns = resp.columns();
                    assertThat(columns, hasSize(3));
                    assertThat(
                        "resp is " + resp,
                        columns,
                        equalTo(
                            List.of(
                                new ColumnInfoImpl(command, expectedType, null),
                                new ColumnInfoImpl("cluster", "keyword", null),
                                new ColumnInfoImpl("bucket(@timestamp, 1 hour)", "date", null)
                            )
                        )
                    );
                }
            }
        }
    }

    private String rolloverAndDownsample(String dataStreamName, DownsampleConfig downsampleConfig) throws Exception {
        // returns the name of the new backing index
        // Rollover to ensure the index we will downsample is not the write index
        RolloverResponse rolloverResponse = safeGet(client().admin().indices().rolloverIndex(new RolloverRequest(dataStreamName, null)));
        assertThat(rolloverResponse.isRolledOver(), equalTo(true));
        String sourceIndex = rolloverResponse.getOldIndex();
        String newIndex = rolloverResponse.getNewIndex();
        String targetIndex = "downsample-" + downsampleConfig.getFixedInterval().toString() + "-" + sourceIndex;
        // Set the source index to read-only state
        assertAcked(
            indicesAdmin().prepareUpdateSettings(sourceIndex)
                .setSettings(Settings.builder().put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true).build())
        );

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

        return newIndex;
    }

    private EsqlQueryResponse esqlCommand(String command) throws IOException {
        return client().execute(EsqlQueryAction.INSTANCE, new EsqlQueryRequest().query(command)).actionGet(30, TimeUnit.SECONDS);
    }

    private static double[] randomHistogramValues(int size) {
        final double[] array = new double[size];
        double minHistogramValue = randomDoubleBetween(0.0, 0.1, true);
        for (int i = 0; i < array.length; i++) {
            array[i] = minHistogramValue += randomDoubleBetween(0.0, 0.5, true);
        }
        return array;
    }

    private static int[] randomHistogramValueCounts(int size) {
        final int[] array = new int[size];
        for (int i = 0; i < array.length; i++) {
            array[i] = randomIntBetween(1, 100);
        }
        return array;
    }
}
