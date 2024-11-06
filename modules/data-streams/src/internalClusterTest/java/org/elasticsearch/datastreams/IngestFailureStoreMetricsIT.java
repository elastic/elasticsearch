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
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.elasticsearch.action.admin.indices.readonly.TransportAddIndexBlockAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.FailureStoreMetrics;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.IngestTestPlugin;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.TestProcessor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * An integration test that verifies how different paths/scenarios affect the APM metrics for failure stores.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0, scope = ESIntegTestCase.Scope.SUITE)
public class IngestFailureStoreMetricsIT extends ESIntegTestCase {

    private static final List<String> METRICS = List.of(
        FailureStoreMetrics.METRIC_TOTAL,
        FailureStoreMetrics.METRIC_FAILURE_STORE,
        FailureStoreMetrics.METRIC_REJECTED
    );

    private String template;
    private String dataStream;
    private String pipeline;

    @Before
    public void initializeRandomNames() {
        template = "template-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        dataStream = "data-stream-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        pipeline = "pipeline-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        logger.info(
            "--> running [{}] with generated names data stream [{}], template [{}] and pipeline [{}]",
            getTestName(),
            dataStream,
            template,
            pipeline
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class, CustomIngestTestPlugin.class, TestTelemetryPlugin.class, MapperExtrasPlugin.class);
    }

    public void testNoPipelineNoFailures() throws IOException {
        putComposableIndexTemplate(true);
        createDataStream();

        int nrOfDocs = randomIntBetween(5, 10);
        indexDocs(dataStream, nrOfDocs, null);

        var measurements = collectTelemetry();
        assertMeasurements(measurements.get(FailureStoreMetrics.METRIC_TOTAL), nrOfDocs, dataStream);
        assertEquals(0, measurements.get(FailureStoreMetrics.METRIC_FAILURE_STORE).size());
        assertEquals(0, measurements.get(FailureStoreMetrics.METRIC_REJECTED).size());
    }

    public void testFailingPipelineNoFailureStore() throws IOException {
        putComposableIndexTemplate(false);
        createDataStream();
        createBasicPipeline("fail");

        int nrOfSuccessfulDocs = randomIntBetween(5, 10);
        indexDocs(dataStream, nrOfSuccessfulDocs, null);
        int nrOfFailingDocs = randomIntBetween(5, 10);
        indexDocs(dataStream, nrOfFailingDocs, pipeline);

        var measurements = collectTelemetry();
        assertMeasurements(measurements.get(FailureStoreMetrics.METRIC_TOTAL), nrOfSuccessfulDocs + nrOfFailingDocs, dataStream);
        assertEquals(0, measurements.get(FailureStoreMetrics.METRIC_FAILURE_STORE).size());
        assertMeasurements(
            measurements.get(FailureStoreMetrics.METRIC_REJECTED),
            nrOfFailingDocs,
            dataStream,
            FailureStoreMetrics.ErrorLocation.PIPELINE,
            false
        );
    }

    public void testFailingPipelineWithFailureStore() throws IOException {
        putComposableIndexTemplate(true);
        createDataStream();
        createBasicPipeline("fail");

        int nrOfSuccessfulDocs = randomIntBetween(5, 10);
        indexDocs(dataStream, nrOfSuccessfulDocs, null);
        int nrOfFailingDocs = randomIntBetween(5, 10);
        indexDocs(dataStream, nrOfFailingDocs, pipeline);

        var measurements = collectTelemetry();
        assertMeasurements(measurements.get(FailureStoreMetrics.METRIC_TOTAL), nrOfSuccessfulDocs + nrOfFailingDocs, dataStream);
        assertMeasurements(
            measurements.get(FailureStoreMetrics.METRIC_FAILURE_STORE),
            nrOfFailingDocs,
            dataStream,
            FailureStoreMetrics.ErrorLocation.PIPELINE
        );
        assertEquals(0, measurements.get(FailureStoreMetrics.METRIC_REJECTED).size());
    }

    public void testShardFailureNoFailureStore() throws IOException {
        putComposableIndexTemplate(false);
        createDataStream();

        int nrOfSuccessfulDocs = randomIntBetween(5, 10);
        indexDocs(dataStream, nrOfSuccessfulDocs, null);
        int nrOfFailingDocs = randomIntBetween(5, 10);
        indexDocs(dataStream, nrOfFailingDocs, "\"foo\"", null);

        var measurements = collectTelemetry();
        assertMeasurements(measurements.get(FailureStoreMetrics.METRIC_TOTAL), nrOfSuccessfulDocs + nrOfFailingDocs, dataStream);
        assertEquals(0, measurements.get(FailureStoreMetrics.METRIC_FAILURE_STORE).size());
        assertMeasurements(
            measurements.get(FailureStoreMetrics.METRIC_REJECTED),
            nrOfFailingDocs,
            dataStream,
            FailureStoreMetrics.ErrorLocation.SHARD,
            false
        );
    }

    public void testShardFailureWithFailureStore() throws IOException {
        putComposableIndexTemplate(true);
        createDataStream();

        int nrOfSuccessfulDocs = randomIntBetween(5, 10);
        indexDocs(dataStream, nrOfSuccessfulDocs, null);
        int nrOfFailingDocs = randomIntBetween(5, 10);
        indexDocs(dataStream, nrOfFailingDocs, "\"foo\"", null);

        var measurements = collectTelemetry();
        assertMeasurements(measurements.get(FailureStoreMetrics.METRIC_TOTAL), nrOfSuccessfulDocs + nrOfFailingDocs, dataStream);
        assertMeasurements(
            measurements.get(FailureStoreMetrics.METRIC_FAILURE_STORE),
            nrOfFailingDocs,
            dataStream,
            FailureStoreMetrics.ErrorLocation.SHARD
        );
        assertEquals(0, measurements.get(FailureStoreMetrics.METRIC_REJECTED).size());
    }

    /**
     * Make sure the rejected counter gets incremented when there were shard-level failures while trying to redirect a document to the
     * failure store.
     */
    public void testRejectionFromFailureStore() throws IOException {
        putComposableIndexTemplate(true);
        createDataStream();

        // Initialize failure store.
        var rolloverRequest = new RolloverRequest(dataStream, null);
        rolloverRequest.setIndicesOptions(
            IndicesOptions.builder(rolloverRequest.indicesOptions()).selectorOptions(IndicesOptions.SelectorOptions.FAILURES).build()
        );
        var rolloverResponse = client().execute(RolloverAction.INSTANCE, rolloverRequest).actionGet();
        var failureStoreIndex = rolloverResponse.getNewIndex();
        // Add a write block to the failure store index, which causes shard-level "failures".
        var addIndexBlockRequest = new AddIndexBlockRequest(IndexMetadata.APIBlock.WRITE, failureStoreIndex);
        client().execute(TransportAddIndexBlockAction.TYPE, addIndexBlockRequest).actionGet();

        int nrOfSuccessfulDocs = randomIntBetween(5, 10);
        indexDocs(dataStream, nrOfSuccessfulDocs, null);
        int nrOfFailingDocs = randomIntBetween(5, 10);
        indexDocs(dataStream, nrOfFailingDocs, "\"foo\"", null);

        var measurements = collectTelemetry();
        assertMeasurements(measurements.get(FailureStoreMetrics.METRIC_TOTAL), nrOfSuccessfulDocs + nrOfFailingDocs, dataStream);
        assertMeasurements(
            measurements.get(FailureStoreMetrics.METRIC_FAILURE_STORE),
            nrOfFailingDocs,
            dataStream,
            FailureStoreMetrics.ErrorLocation.SHARD
        );
        assertMeasurements(
            measurements.get(FailureStoreMetrics.METRIC_REJECTED),
            nrOfFailingDocs,
            dataStream,
            FailureStoreMetrics.ErrorLocation.SHARD,
            true
        );
    }

    /**
     * Make sure metrics get the correct <code>data_stream</code> attribute after a reroute.
     */
    public void testRerouteSuccessfulCorrectName() throws IOException {
        putComposableIndexTemplate(false);
        createDataStream();

        String destination = dataStream + "-destination";
        final var createDataStreamRequest = new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, destination);
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).actionGet());
        createReroutePipeline(destination);

        int nrOfDocs = randomIntBetween(5, 10);
        indexDocs(dataStream, nrOfDocs, pipeline);

        var measurements = collectTelemetry();
        assertMeasurements(measurements.get(FailureStoreMetrics.METRIC_TOTAL), nrOfDocs, destination);
        assertEquals(0, measurements.get(FailureStoreMetrics.METRIC_FAILURE_STORE).size());
        assertEquals(0, measurements.get(FailureStoreMetrics.METRIC_REJECTED).size());
    }

    public void testDropping() throws IOException {
        putComposableIndexTemplate(true);
        createDataStream();
        createBasicPipeline("drop");

        int nrOfDocs = randomIntBetween(5, 10);
        indexDocs(dataStream, nrOfDocs, pipeline);

        var measurements = collectTelemetry();
        assertMeasurements(measurements.get(FailureStoreMetrics.METRIC_TOTAL), nrOfDocs, dataStream);
        assertEquals(0, measurements.get(FailureStoreMetrics.METRIC_FAILURE_STORE).size());
        assertEquals(0, measurements.get(FailureStoreMetrics.METRIC_REJECTED).size());
    }

    public void testDataStreamAlias() throws IOException {
        putComposableIndexTemplate(false);
        createDataStream();
        var indicesAliasesRequest = new IndicesAliasesRequest();
        indicesAliasesRequest.addAliasAction(
            IndicesAliasesRequest.AliasActions.add().alias("some-alias").index(dataStream).writeIndex(true)
        );
        client().execute(TransportIndicesAliasesAction.TYPE, indicesAliasesRequest).actionGet();

        int nrOfDocs = randomIntBetween(5, 10);
        indexDocs("some-alias", nrOfDocs, null);

        var measurements = collectTelemetry();
        assertMeasurements(measurements.get(FailureStoreMetrics.METRIC_TOTAL), nrOfDocs, dataStream);
        assertEquals(0, measurements.get(FailureStoreMetrics.METRIC_FAILURE_STORE).size());
        assertEquals(0, measurements.get(FailureStoreMetrics.METRIC_REJECTED).size());
    }

    private void putComposableIndexTemplate(boolean failureStore) throws IOException {
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(template);
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(dataStream + "*"))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false, failureStore))
                .template(new Template(null, new CompressedXContent("""
                    {
                      "dynamic": false,
                      "properties": {
                        "@timestamp": {
                          "type": "date"
                        },
                        "count": {
                            "type": "long"
                        }
                      }
                    }"""), null))
                .build()
        );
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();
    }

    private void createDataStream() {
        final var createDataStreamRequest = new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStream);
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).actionGet());
    }

    private void createBasicPipeline(String processorType) {
        createPipeline(Strings.format("\"%s\": {}", processorType));
    }

    private void createReroutePipeline(String destination) {
        createPipeline(Strings.format("\"reroute\": {\"destination\": \"%s\"}", destination));
    }

    private void createPipeline(String processor) {
        putJsonPipeline(pipeline, Strings.format("{\"processors\": [{%s}]}", processor));
    }

    private void indexDocs(String dataStream, int numDocs, String pipeline) {
        indexDocs(dataStream, numDocs, "1", pipeline);
    }

    private void indexDocs(String dataStream, int numDocs, String value, String pipeline) {
        BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < numDocs; i++) {
            String time = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
            bulkRequest.add(
                new IndexRequest(dataStream).opType(DocWriteRequest.OpType.CREATE)
                    .source(Strings.format("{\"%s\":\"%s\", \"count\": %s}", DEFAULT_TIMESTAMP_FIELD, time, value), XContentType.JSON)
                    .setPipeline(pipeline)
            );
        }
        client().bulk(bulkRequest).actionGet();
    }

    private static Map<String, List<Measurement>> collectTelemetry() {
        Map<String, List<Measurement>> measurements = new HashMap<>();
        for (PluginsService pluginsService : internalCluster().getInstances(PluginsService.class)) {
            final TestTelemetryPlugin telemetryPlugin = pluginsService.filterPlugins(TestTelemetryPlugin.class).findFirst().orElseThrow();

            telemetryPlugin.collect();

            for (String metricName : METRICS) {
                measurements.put(metricName, telemetryPlugin.getLongCounterMeasurement(metricName));
            }
        }
        return measurements;
    }

    private void assertMeasurements(List<Measurement> measurements, int expectedSize, String expectedDataStream) {
        assertMeasurements(measurements, expectedSize, expectedDataStream, (Consumer<Measurement>) null);
    }

    private void assertMeasurements(
        List<Measurement> measurements,
        int expectedSize,
        String expectedDataStream,
        FailureStoreMetrics.ErrorLocation location
    ) {
        assertMeasurements(
            measurements,
            expectedSize,
            expectedDataStream,
            measurement -> assertEquals(location.name(), measurement.attributes().get("error_location"))
        );
    }

    private void assertMeasurements(
        List<Measurement> measurements,
        int expectedSize,
        String expectedDataStream,
        FailureStoreMetrics.ErrorLocation location,
        boolean failureStore
    ) {
        assertMeasurements(measurements, expectedSize, expectedDataStream, measurement -> {
            assertEquals(location.name(), measurement.attributes().get("error_location"));
            assertEquals(failureStore, measurement.attributes().get("failure_store"));
        });
    }

    private void assertMeasurements(
        List<Measurement> measurements,
        int expectedSize,
        String expectedDataStream,
        Consumer<Measurement> customAssertion
    ) {
        assertEquals(expectedSize, measurements.size());
        for (Measurement measurement : measurements) {
            assertEquals(expectedDataStream, measurement.attributes().get("data_stream"));
            if (customAssertion != null) {
                customAssertion.accept(measurement);
            }
        }
    }

    public static class CustomIngestTestPlugin extends IngestTestPlugin {
        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            Map<String, Processor.Factory> processors = new HashMap<>();
            processors.put(
                "drop",
                (factories, tag, description, config) -> new TestProcessor(tag, "drop", description, ingestDocument -> null)
            );
            processors.put("reroute", (factories, tag, description, config) -> {
                String destination = (String) config.remove("destination");
                return new TestProcessor(
                    tag,
                    "reroute",
                    description,
                    (Consumer<IngestDocument>) ingestDocument -> ingestDocument.reroute(destination)
                );
            });
            processors.put(
                "fail",
                (processorFactories, tag, description, config) -> new TestProcessor(tag, "fail", description, new RuntimeException())
            );
            return processors;
        }
    }
}
