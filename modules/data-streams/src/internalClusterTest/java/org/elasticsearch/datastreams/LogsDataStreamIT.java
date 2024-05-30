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
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.is;

public class LogsDataStreamIT extends ESSingleNodeTestCase {

    public static final String LOG_MAPPING = """
        {
          "_doc":{
            "properties": {
              "@timestamp" : {
                "type": "date"
              },
              "hostname": {
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
          }
        }""";

    private static final String DOC_TEMPLATE = """
        {
            "@timestamp": "%s",
            "pid": "%d",
            "method": "%s",
            "message": "%s",
            "ip_address": "%s"
        }
        """;

    static String toIsoTimestamp(Instant instant) {
        return DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(instant);
    }

    private static String createLogDocument(
        final Instant instant,
        long pid,
        final String method,
        final String message,
        final String ipAddress
    ) {
        return Strings.format(DOC_TEMPLATE, toIsoTimestamp(instant), pid, method, message, ipAddress);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(DataStreamsPlugin.class, InternalSettingsPlugin.class);
    }

    public void testLogsIndexModeDataStreamIndexing() throws IOException, ExecutionException, InterruptedException {
        putComposableIndexTemplate(client(), IndexMode.LOGS, "logs-composable-template", List.of("logs-*-*"));
        final String dataStreamName = generateDataStreamName();
        createLogsDataStream(client(), dataStreamName);
        indexLogDocuments(client(), randomIntBetween(32, 64), randomIntBetween(128, 256), dataStreamName);
        rolloverDataStream(dataStreamName);
        indexLogDocuments(client(), randomIntBetween(32, 64), randomIntBetween(128, 256), dataStreamName);
    }

    private void rolloverDataStream(final String dataStreamName) {
        assertThat(indicesAdmin().rolloverIndex(new RolloverRequest(dataStreamName, null)).actionGet().isRolledOver(), is(true));
    }

    private void indexLogDocuments(final Client client, int numBulkRequests, int numDocsPerBulkRequest, final String dataStreamName) {
        {
            Instant timestamp = Instant.now();
            for (int i = 0; i < numBulkRequests; i++) {
                BulkRequest bulkRequest = new BulkRequest(dataStreamName);
                for (int j = 0; j < numDocsPerBulkRequest; j++) {
                    var indexRequest = new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE);
                    indexRequest.source(
                        createLogDocument(
                            timestamp,
                            randomIntBetween(100, 200),
                            randomFrom("POST", "PUT", "GET"),
                            randomAlphaOfLengthBetween(256, 512),
                            InetAddresses.toAddrString(randomIp(randomBoolean()))
                        ),
                        XContentType.JSON
                    );
                    bulkRequest.add(indexRequest);
                    timestamp = timestamp.plusSeconds(1);
                }
                final BulkResponse bulkResponse = client.bulk(bulkRequest).actionGet();
                assertThat(bulkResponse.hasFailures(), is(false));
            }
            final BroadcastResponse refreshResponse = client.admin().indices().refresh(new RefreshRequest(dataStreamName)).actionGet();
            assertThat(refreshResponse.getStatus(), is(RestStatus.OK));
        }
    }

    private void createLogsDataStream(final Client client, final String dataStreamName) throws InterruptedException, ExecutionException {
        final CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        final AcknowledgedResponse createDataStreamResponse = client.execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest)
            .get();
        assertThat(createDataStreamResponse.isAcknowledged(), is(true));
    }

    private static String generateDataStreamName() {
        return Strings.format("logs-%s-%s", randomFrom("apache", "nginx", "system"), randomFrom("dev", "qa", "prod"));
    }

    private static void putComposableIndexTemplate(
        final Client client,
        final IndexMode indexMode,
        final String templateName,
        final List<String> indexPatterns
    ) throws IOException {
        final Settings.Builder templateSettings = Settings.builder().put("index.mode", indexMode);
        final TransportPutComposableIndexTemplateAction.Request putComposableTemplateRequest =
            new TransportPutComposableIndexTemplateAction.Request(templateName);
        putComposableTemplateRequest.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(indexPatterns)
                .template(new Template(templateSettings.build(), new CompressedXContent(LOG_MAPPING), null))
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
