/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineTransportAction;
import org.elasticsearch.action.ingest.ReservedPipelineAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ReservedStateErrorMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateHandlerMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.bytes.BytesReference.bytes;
import static org.elasticsearch.ingest.IngestPipelineTestUtils.putJsonPipelineRequest;
import static org.elasticsearch.xcontent.XContentType.JSON;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class IngestFileSettingsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CustomIngestTestPlugin.class);
    }

    private static final AtomicLong versionCounter = new AtomicLong(1);

    private static final String testJSON = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "ingest_pipelines": {
                   "my_ingest_pipeline": {
                       "description": "_description",
                       "processors": [
                          {
                            "test" : {
                              "field": "pipeline",
                              "value": "pipeline"
                            }
                          }
                       ]
                   },
                   "my_ingest_pipeline_1": {
                       "description": "_description",
                       "processors": [
                          {
                            "test" : {
                              "field": "pipeline",
                              "value": "pipeline"
                            }
                          }
                       ]
                   }
                 }
             }
        }""";

    private static final String testErrorJSON = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "ingest_pipelines": {
                   "my_ingest_pipeline": {
                       "description": "_description",
                       "processors":
                          {
                            "foo" : {
                              "field": "pipeline",
                              "value": "pipeline"
                            }
                          }
                       ]
                   }
                 }
             }
        }""";

    private void writeJSONFile(String node, String json) throws Exception {
        long version = versionCounter.incrementAndGet();

        FileSettingsService fileSettingsService = internalCluster().getInstance(FileSettingsService.class, node);
        assertTrue(fileSettingsService.watching());

        Files.deleteIfExists(fileSettingsService.watchedFile());

        Files.createDirectories(fileSettingsService.watchedFileDir());
        Path tempFilePath = createTempFile();

        logger.info("--> writing JSON config to node {} with path {}", node, tempFilePath);
        logger.info(Strings.format(json, version));
        Files.writeString(tempFilePath, Strings.format(json, version));
        Files.move(tempFilePath, fileSettingsService.watchedFile(), StandardCopyOption.ATOMIC_MOVE);
    }

    private Tuple<CountDownLatch, AtomicLong> setupClusterStateListener(String node) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(1);
        AtomicLong metadataVersion = new AtomicLong(-1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                ReservedStateMetadata reservedState = event.state().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
                if (reservedState != null) {
                    ReservedStateHandlerMetadata handlerMetadata = reservedState.handlers().get(ReservedPipelineAction.NAME);
                    if (handlerMetadata != null && handlerMetadata.keys().contains("my_ingest_pipeline")) {
                        clusterService.removeListener(this);
                        metadataVersion.set(event.state().metadata().version());
                        savedClusterState.countDown();
                    }
                }
            }
        });

        return new Tuple<>(savedClusterState, metadataVersion);
    }

    private void assertPipelinesSaveOK(CountDownLatch savedClusterState, AtomicLong metadataVersion) throws Exception {
        boolean awaitSuccessful = savedClusterState.await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        final ClusterStateResponse clusterStateResponse = clusterAdmin().state(
            new ClusterStateRequest(TEST_REQUEST_TIMEOUT).waitForMetadataVersion(metadataVersion.get())
        ).get();

        ReservedStateMetadata reservedState = clusterStateResponse.getState()
            .metadata()
            .reservedStateMetadata()
            .get(FileSettingsService.NAMESPACE);

        ReservedStateHandlerMetadata handlerMetadata = reservedState.handlers().get(ReservedPipelineAction.NAME);

        assertThat(handlerMetadata.keys(), allOf(notNullValue(), containsInAnyOrder("my_ingest_pipeline", "my_ingest_pipeline_1")));

        // Try using the REST API to update the my_autoscaling_policy policy
        // This should fail, we have reserved certain autoscaling policies in operator mode
        assertEquals(
            "Failed to process request [org.elasticsearch.action.ingest.PutPipelineRequest/unset] with errors: "
                + "[[my_ingest_pipeline] set as read-only by [file_settings]]",
            expectThrows(
                IllegalArgumentException.class,
                client().execute(PutPipelineTransportAction.TYPE, sampleRestRequest("my_ingest_pipeline"))
            ).getMessage()
        );
    }

    public void testPoliciesApplied() throws Exception {
        ensureGreen();

        var savedClusterState = setupClusterStateListener(internalCluster().getMasterName());
        writeJSONFile(internalCluster().getMasterName(), testJSON);

        assertPipelinesSaveOK(savedClusterState.v1(), savedClusterState.v2());
    }

    private Tuple<CountDownLatch, AtomicLong> setupClusterStateListenerForError(String node) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(1);
        AtomicLong metadataVersion = new AtomicLong(-1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                ReservedStateMetadata reservedState = event.state().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
                if (reservedState != null && reservedState.errorMetadata() != null) {
                    clusterService.removeListener(this);
                    metadataVersion.set(event.state().metadata().version());
                    savedClusterState.countDown();
                    assertEquals(ReservedStateErrorMetadata.ErrorKind.PARSING, reservedState.errorMetadata().errorKind());
                    assertThat(reservedState.errorMetadata().errors(), allOf(notNullValue(), hasSize(1)));
                    assertThat(
                        reservedState.errorMetadata().errors().get(0),
                        containsString("org.elasticsearch.xcontent.XContentParseException: [17:16] [reserved_state_chunk] failed")
                    );
                }
            }
        });

        return new Tuple<>(savedClusterState, metadataVersion);
    }

    private void assertPipelinesNotSaved(CountDownLatch savedClusterState, AtomicLong metadataVersion) throws Exception {
        boolean awaitSuccessful = savedClusterState.await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        // This should succeed, nothing was reserved
        client().execute(PutPipelineTransportAction.TYPE, sampleRestRequest("my_ingest_pipeline_bad")).get();
    }

    public void testErrorSaved() throws Exception {
        ensureGreen();
        var savedClusterState = setupClusterStateListenerForError(internalCluster().getMasterName());

        writeJSONFile(internalCluster().getMasterName(), testErrorJSON);
        assertPipelinesNotSaved(savedClusterState.v1(), savedClusterState.v2());
    }

    private PutPipelineRequest sampleRestRequest(String id) throws Exception {
        var json = """
            {
               "description": "_description",
               "processors": [
                  {
                    "test" : {
                      "field": "_foo",
                      "value": "_bar"
                    }
                  }
               ]
            }""";

        try (
            var bis = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
            var parser = JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bis);
            var builder = XContentFactory.contentBuilder(JSON)
        ) {
            builder.map(parser.map());
            return putJsonPipelineRequest(id, bytes(builder));
        }
    }

    public static class CustomIngestTestPlugin extends IngestTestPlugin {
        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            Map<String, Processor.Factory> processors = new HashMap<>();
            processors.put("test", (factories, tag, description, config, projectId) -> {
                String field = (String) config.remove("field");
                String value = (String) config.remove("value");
                return new FakeProcessor("test", tag, description, (ingestDocument) -> ingestDocument.setFieldValue(field, value));
            });

            return processors;
        }
    }
}
