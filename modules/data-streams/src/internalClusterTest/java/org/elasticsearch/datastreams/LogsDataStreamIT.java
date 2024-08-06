/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;

public class LogsDataStreamIT extends ESSingleNodeTestCase {

    private static final String LOGS_OR_STANDARD_MAPPING = """
        {
            "properties": {
              "@timestamp" : {
                "type": "date"
              },
              "host.name": {
                "type": "keyword"
              },
              "pid": {
                "type": "long"
              },
              "method": {
                "type": "keyword"
              },
              "message": {
                "type": "text"
              },
              "ip_address": {
                "type": "ip"
              }
            }
        }""";

    private static final String TIME_SERIES_MAPPING = """
        {
            "properties": {
              "@timestamp" : {
                "type": "date"
              },
              "host.name": {
                "type": "keyword",
                "time_series_dimension": "true"
              },
              "pid": {
                "type": "long",
                "time_series_dimension": "true"
              },
              "method": {
                "type": "keyword"
              },
              "ip_address": {
                "type": "ip"
              },
              "cpu_usage": {
                "type": "float",
                "time_series_metric": "gauge"
              }
            }
        }""";

    private static final String LOG_DOC_TEMPLATE = """
        {
            "@timestamp": "%s",
            "host.name": "%s",
            "pid": "%d",
            "method": "%s",
            "message": "%s",
            "ip_address": "%s"
        }
        """;

    private static final String TIME_SERIES_DOC_TEMPLATE = """
        {
            "@timestamp": "%s",
            "host.name": "%s",
            "pid": "%d",
            "method": "%s",
            "ip_address": "%s",
            "cpu_usage": "%f"
        }
        """;

    private static String toIsoTimestamp(final Instant instant) {
        return DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(instant);
    }

    private static String createLogDocument(
        final Instant timestamp,
        final String hostname,
        long pid,
        final String method,
        final String message,
        final String ipAddress
    ) {
        return Strings.format(LOG_DOC_TEMPLATE, toIsoTimestamp(timestamp), hostname, pid, method, message, ipAddress);
    }

    private static String createTimeSeriesDocument(
        final Instant timestamp,
        final String hostname,
        long pid,
        final String method,
        final String ipAddress,
        double cpuUsage
    ) {
        return Strings.format(TIME_SERIES_DOC_TEMPLATE, toIsoTimestamp(timestamp), hostname, pid, method, ipAddress, cpuUsage);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(DataStreamsPlugin.class, InternalSettingsPlugin.class);
    }

    public void testLogsIndexModeDataStreamIndexing() throws IOException, ExecutionException, InterruptedException {
        putComposableIndexTemplate(
            client(),
            "logs-composable-template",
            LOGS_OR_STANDARD_MAPPING,
            Map.of("index.mode", "logsdb"),
            List.of("logs-*-*")
        );
        final String dataStreamName = generateDataStreamName("logs");
        createDataStream(client(), dataStreamName);
        indexLogOrStandardDocuments(client(), randomIntBetween(10, 20), randomIntBetween(32, 64), dataStreamName);
        rolloverDataStream(dataStreamName);
        indexLogOrStandardDocuments(client(), randomIntBetween(10, 20), randomIntBetween(32, 64), dataStreamName);
    }

    public void testIndexModeLogsAndStandardSwitching() throws IOException, ExecutionException, InterruptedException {
        final List<IndexMode> indexModes = new ArrayList<>();
        final String dataStreamName = generateDataStreamName("logs");
        indexModes.add(IndexMode.STANDARD);
        putComposableIndexTemplate(
            client(),
            "logs-composable-template",
            LOGS_OR_STANDARD_MAPPING,
            Map.of("index.mode", "standard"),
            List.of("logs-*-*")
        );
        createDataStream(client(), dataStreamName);
        for (int i = 0; i < randomIntBetween(5, 10); i++) {
            final IndexMode indexMode = i % 2 == 0 ? IndexMode.LOGSDB : IndexMode.STANDARD;
            indexModes.add(indexMode);
            updateComposableIndexTemplate(
                client(),
                "logs-composable-template",
                LOGS_OR_STANDARD_MAPPING,
                Map.of("index.mode", indexMode.getName()),
                List.of("logs-*-*")
            );
            indexLogOrStandardDocuments(client(), randomIntBetween(10, 20), randomIntBetween(32, 64), dataStreamName);
            rolloverDataStream(dataStreamName);
        }
        assertDataStreamBackingIndicesModes(dataStreamName, indexModes);
    }

    public void testIndexModeLogsAndTimeSeriesSwitching() throws IOException, ExecutionException, InterruptedException {
        final String dataStreamName = generateDataStreamName("custom");
        final List<String> indexPatterns = List.of("custom-*-*");
        final Map<String, String> logsSettings = Map.of("index.mode", "logsdb");
        final Map<String, String> timeSeriesSettings = Map.of("index.mode", "time_series", "index.routing_path", "host.name");

        putComposableIndexTemplate(client(), "custom-composable-template", LOGS_OR_STANDARD_MAPPING, logsSettings, indexPatterns);
        createDataStream(client(), dataStreamName);
        indexLogOrStandardDocuments(client(), randomIntBetween(10, 20), randomIntBetween(32, 64), dataStreamName);

        updateComposableIndexTemplate(client(), "custom-composable-template", TIME_SERIES_MAPPING, timeSeriesSettings, indexPatterns);
        rolloverDataStream(dataStreamName);
        indexTimeSeriesDocuments(client(), randomIntBetween(10, 20), randomIntBetween(32, 64), dataStreamName);

        updateComposableIndexTemplate(client(), "custom-composable-template", LOGS_OR_STANDARD_MAPPING, logsSettings, indexPatterns);
        rolloverDataStream(dataStreamName);
        indexLogOrStandardDocuments(client(), randomIntBetween(10, 20), randomIntBetween(32, 64), dataStreamName);

        assertDataStreamBackingIndicesModes(dataStreamName, List.of(IndexMode.LOGSDB, IndexMode.TIME_SERIES, IndexMode.LOGSDB));
    }

    public void testInvalidIndexModeTimeSeriesSwitchWithoutRoutingPath() throws IOException, ExecutionException, InterruptedException {
        final String dataStreamName = generateDataStreamName("custom");
        final List<String> indexPatterns = List.of("custom-*-*");
        final Map<String, String> logsSettings = Map.of("index.mode", "logsdb");
        final Map<String, String> timeSeriesSettings = Map.of("index.mode", "time_series");

        putComposableIndexTemplate(client(), "custom-composable-template", LOGS_OR_STANDARD_MAPPING, logsSettings, indexPatterns);
        createDataStream(client(), dataStreamName);
        indexLogOrStandardDocuments(client(), randomIntBetween(10, 20), randomIntBetween(32, 64), dataStreamName);

        expectThrows(
            InvalidIndexTemplateException.class,
            () -> updateComposableIndexTemplate(
                client(),
                "custom-composable-template",
                LOGS_OR_STANDARD_MAPPING,
                timeSeriesSettings,
                indexPatterns
            )
        );
    }

    public void testInvalidIndexModeTimeSeriesSwitchWithoutDimensions() throws IOException, ExecutionException, InterruptedException {
        final String dataStreamName = generateDataStreamName("custom");
        final List<String> indexPatterns = List.of("custom-*-*");
        final Map<String, String> logsSettings = Map.of("index.mode", "logsdb");
        final Map<String, String> timeSeriesSettings = Map.of("index.mode", "time_series", "index.routing_path", "host.name");

        putComposableIndexTemplate(client(), "custom-composable-template", LOGS_OR_STANDARD_MAPPING, logsSettings, indexPatterns);
        createDataStream(client(), dataStreamName);
        indexLogOrStandardDocuments(client(), randomIntBetween(10, 20), randomIntBetween(32, 64), dataStreamName);

        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            updateComposableIndexTemplate(
                client(),
                "custom-composable-template",
                LOGS_OR_STANDARD_MAPPING,
                timeSeriesSettings,
                indexPatterns
            );

        });
        assertThat(
            exception.getCause().getCause().getMessage(),
            Matchers.equalTo(
                "All fields that match routing_path must be configured with [time_series_dimension: true] or flattened fields "
                    + "with a list of dimensions in [time_series_dimensions] and without the [script] parameter. [host.name] was not a "
                    + "dimension."
            )
        );
    }

    private void assertDataStreamBackingIndicesModes(final String dataStreamName, final List<IndexMode> modes) {
        final GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            new String[] { dataStreamName }
        );
        final GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
            .actionGet();
        final DataStream dataStream = getDataStreamResponse.getDataStreams().get(0).getDataStream();
        final DataStream.DataStreamIndices backingIndices = dataStream.getBackingIndices();
        final Iterator<IndexMode> indexModesIterator = modes.iterator();
        assertThat(backingIndices.getIndices().size(), Matchers.equalTo(modes.size()));
        for (final Index index : backingIndices.getIndices()) {
            final GetSettingsResponse getSettingsResponse = indicesAdmin().getSettings(
                new GetSettingsRequest().indices(index.getName()).includeDefaults(true)
            ).actionGet();
            final Settings settings = getSettingsResponse.getIndexToSettings().get(index.getName());
            assertThat(settings.get("index.mode"), Matchers.equalTo(indexModesIterator.next().getName()));
        }
    }

    final String generateDataStreamName(final String prefix) {
        return String.format(Locale.ROOT, "%s-%s-%s", prefix, randomFrom("apache", "nginx", "system"), randomFrom("dev", "qa", "prod"));
    }

    private void rolloverDataStream(final String dataStreamName) {
        assertAcked(indicesAdmin().rolloverIndex(new RolloverRequest(dataStreamName, null)).actionGet());
    }

    private void indexLogOrStandardDocuments(
        final Client client,
        int numBulkRequests,
        int numDocsPerBulkRequest,
        final String dataStreamName
    ) {
        {
            for (int i = 0; i < numBulkRequests; i++) {
                BulkRequest bulkRequest = new BulkRequest(dataStreamName);
                for (int j = 0; j < numDocsPerBulkRequest; j++) {
                    var indexRequest = new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE);
                    final String doc = createLogDocument(
                        Instant.now(),
                        randomAlphaOfLength(7),
                        randomIntBetween(100, 200),
                        randomFrom("POST", "PUT", "GET"),
                        randomAlphaOfLengthBetween(256, 512),
                        InetAddresses.toAddrString(randomIp(randomBoolean()))
                    );
                    indexRequest.source(doc, XContentType.JSON);
                    bulkRequest.add(indexRequest);
                }
                final BulkResponse bulkResponse = client.bulk(bulkRequest).actionGet();
                assertThat(bulkResponse.hasFailures(), is(false));
            }
            final BroadcastResponse refreshResponse = client.admin().indices().refresh(new RefreshRequest(dataStreamName)).actionGet();
            assertThat(refreshResponse.getStatus(), is(RestStatus.OK));
        }
    }

    private void indexTimeSeriesDocuments(
        final Client client,
        int numBulkRequests,
        int numDocsPerBulkRequest,
        final String dataStreamName
    ) {
        {
            for (int i = 0; i < numBulkRequests; i++) {
                BulkRequest bulkRequest = new BulkRequest(dataStreamName);
                for (int j = 0; j < numDocsPerBulkRequest; j++) {
                    var indexRequest = new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE);
                    final String doc = createTimeSeriesDocument(
                        Instant.now(),
                        randomAlphaOfLength(12),
                        randomIntBetween(100, 200),
                        randomFrom("POST", "PUT", "GET"),
                        InetAddresses.toAddrString(randomIp(randomBoolean())),
                        randomDoubleBetween(0.0D, 1.0D, false)
                    );
                    indexRequest.source(doc, XContentType.JSON);
                    bulkRequest.add(indexRequest);
                }
                final BulkResponse bulkResponse = client.bulk(bulkRequest).actionGet();
                assertThat(bulkResponse.hasFailures(), is(false));
            }
            final BroadcastResponse refreshResponse = client.admin().indices().refresh(new RefreshRequest(dataStreamName)).actionGet();
            assertThat(refreshResponse.getStatus(), is(RestStatus.OK));
        }
    }

    private void createDataStream(final Client client, final String dataStreamName) throws InterruptedException, ExecutionException {
        final CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            dataStreamName
        );
        final AcknowledgedResponse createDataStreamResponse = client.execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest)
            .get();
        assertThat(createDataStreamResponse.isAcknowledged(), is(true));
    }

    private static void updateComposableIndexTemplate(
        final Client client,
        final String templateName,
        final String mapping,
        final Map<String, String> settings,
        final List<String> indexPatterns
    ) throws IOException {
        putComposableIndexTemplate(client, templateName, mapping, settings, indexPatterns);
    }

    private static void putComposableIndexTemplate(
        final Client client,
        final String templateName,
        final String mapping,
        final Map<String, String> settings,
        final List<String> indexPatterns
    ) throws IOException {
        final Settings.Builder templateSettings = Settings.builder();
        for (Map.Entry<String, String> setting : settings.entrySet()) {
            templateSettings.put(setting.getKey(), setting.getValue());
        }
        final TransportPutComposableIndexTemplateAction.Request putComposableTemplateRequest =
            new TransportPutComposableIndexTemplateAction.Request(templateName);
        putComposableTemplateRequest.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(indexPatterns)
                .template(new Template(templateSettings.build(), new CompressedXContent(mapping), null))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                .build()
        );
        final AcknowledgedResponse putComposableTemplateResponse = client.execute(
            TransportPutComposableIndexTemplateAction.TYPE,
            putComposableTemplateRequest
        ).actionGet();
        assertThat(putComposableTemplateResponse.isAcknowledged(), is(true));
    }
}
