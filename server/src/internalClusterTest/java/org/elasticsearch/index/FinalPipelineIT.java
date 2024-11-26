/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.ingest.GetPipelineResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasToString;

public class FinalPipelineIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestPlugin.class);
    }

    @After
    public void cleanUpPipelines() {
        indicesAdmin().prepareDelete("*").get();

        final GetPipelineResponse response = getPipelines("default_pipeline", "final_pipeline", "request_pipeline");
        for (final PipelineConfiguration pipeline : response.pipelines()) {
            deletePipeline(pipeline.getId());
        }
    }

    public void testFinalPipelineCantChangeDestination() {
        final Settings settings = Settings.builder().put(IndexSettings.FINAL_PIPELINE.getKey(), "final_pipeline").build();
        createIndex("index", settings);

        putJsonPipeline("final_pipeline", """
            {"processors": [{"changing_dest": {}}]}""");

        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            prepareIndex("index").setId("1").setSource(Map.of("field", "value"))
        );
        assertThat(
            e,
            hasToString(
                endsWith("final pipeline [final_pipeline] can't change the target index (from [index] to [target]) for document [1]")
            )
        );
    }

    public void testFinalPipelineCantRerouteDestination() {
        final Settings settings = Settings.builder().put(IndexSettings.FINAL_PIPELINE.getKey(), "final_pipeline").build();
        createIndex("index", settings);

        putJsonPipeline("final_pipeline", """
            {"processors": [{"reroute": {}}]}""");

        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            prepareIndex("index").setId("1").setSource(Map.of("field", "value"))
        );
        assertThat(
            e,
            hasToString(
                endsWith("final pipeline [final_pipeline] can't change the target index (from [index] to [target]) for document [1]")
            )
        );
    }

    public void testFinalPipelineOfOldDestinationIsNotInvoked() {
        Settings settings = Settings.builder()
            .put(IndexSettings.DEFAULT_PIPELINE.getKey(), "default_pipeline")
            .put(IndexSettings.FINAL_PIPELINE.getKey(), "final_pipeline")
            .build();
        createIndex("index", settings);

        putJsonPipeline("default_pipeline", """
            {"processors": [{"changing_dest": {}}]}""");

        putJsonPipeline("final_pipeline", """
            {"processors": [{"final": {"exists":"no_such_field"}}]}""");

        DocWriteResponse indexResponse = prepareIndex("index").setId("1")
            .setSource(Map.of("field", "value"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertEquals(RestStatus.CREATED, indexResponse.status());
        assertResponse(prepareSearch("target"), response -> {
            assertEquals(1, response.getHits().getTotalHits().value());
            assertFalse(response.getHits().getAt(0).getSourceAsMap().containsKey("final"));
        });
    }

    public void testFinalPipelineOfNewDestinationIsInvoked() {
        Settings settings = Settings.builder().put(IndexSettings.DEFAULT_PIPELINE.getKey(), "default_pipeline").build();
        createIndex("index", settings);

        settings = Settings.builder().put(IndexSettings.FINAL_PIPELINE.getKey(), "final_pipeline").build();
        createIndex("target", settings);

        putJsonPipeline("default_pipeline", """
            {"processors": [{"changing_dest": {}}]}""");

        putJsonPipeline("final_pipeline", """
            {"processors": [{"final": {}}]}""");

        DocWriteResponse indexResponse = prepareIndex("index").setId("1")
            .setSource(Map.of("field", "value"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertEquals(RestStatus.CREATED, indexResponse.status());
        assertResponse(prepareSearch("target"), response -> {
            assertEquals(1, response.getHits().getTotalHits().value());
            assertEquals(true, response.getHits().getAt(0).getSourceAsMap().get("final"));
        });
    }

    public void testDefaultPipelineOfNewDestinationIsNotInvoked() {
        Settings settings = Settings.builder().put(IndexSettings.DEFAULT_PIPELINE.getKey(), "default_pipeline").build();
        createIndex("index", settings);

        settings = Settings.builder().put(IndexSettings.DEFAULT_PIPELINE.getKey(), "target_default_pipeline").build();
        createIndex("target", settings);

        putJsonPipeline("default_pipeline", """
            {"processors": [{"changing_dest": {}}]}""");

        putJsonPipeline("target_default_pipeline", """
            {"processors": [{"final": {}}]}""");

        DocWriteResponse indexResponse = prepareIndex("index").setId("1")
            .setSource(Map.of("field", "value"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertEquals(RestStatus.CREATED, indexResponse.status());
        assertResponse(prepareSearch("target"), response -> {
            assertEquals(1, response.getHits().getTotalHits().value());
            assertFalse(response.getHits().getAt(0).getSourceAsMap().containsKey("final"));
        });
    }

    public void testDefaultPipelineOfRerouteDestinationIsInvoked() {
        Settings settings = Settings.builder().put(IndexSettings.DEFAULT_PIPELINE.getKey(), "default_pipeline").build();
        createIndex("index", settings);

        settings = Settings.builder().put(IndexSettings.DEFAULT_PIPELINE.getKey(), "target_default_pipeline").build();
        createIndex("target", settings);

        putJsonPipeline("default_pipeline", """
            {"processors": [{"reroute": {}}]}""");

        putJsonPipeline("target_default_pipeline", """
            {"processors": [{"final": {}}]}""");

        DocWriteResponse indexResponse = prepareIndex("index").setId("1")
            .setSource(Map.of("field", "value"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertEquals(RestStatus.CREATED, indexResponse.status());
        assertResponse(prepareSearch("target"), response -> {
            assertEquals(1, response.getHits().getTotalHits().value());
            assertTrue(response.getHits().getAt(0).getSourceAsMap().containsKey("final"));
        });
    }

    public void testAvoidIndexingLoop() {
        Settings settings = Settings.builder().put(IndexSettings.DEFAULT_PIPELINE.getKey(), "default_pipeline").build();
        createIndex("index", settings);

        settings = Settings.builder().put(IndexSettings.DEFAULT_PIPELINE.getKey(), "target_default_pipeline").build();
        createIndex("target", settings);

        putJsonPipeline("default_pipeline", """
            {"processors": [{"reroute": {"dest": "target"}}]}""");

        putJsonPipeline("target_default_pipeline", """
            {"processors": [{"reroute": {"dest": "index"}}]}""");

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            prepareIndex("index").setId("1").setSource(Map.of("dest", "index")).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        );
        assertThat(
            exception.getMessage(),
            equalTo("index cycle detected while processing pipeline [target_default_pipeline] for document [1]: [index, target, index]")
        );
    }

    public void testFinalPipeline() {
        final Settings settings = Settings.builder().put(IndexSettings.FINAL_PIPELINE.getKey(), "final_pipeline").build();
        createIndex("index", settings);

        // this asserts that the final_pipeline was used, without us having to actually create the pipeline etc.
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            prepareIndex("index").setId("1").setSource(Map.of("field", "value"))
        );
        assertThat(e, hasToString(containsString("pipeline with id [final_pipeline] does not exist")));
    }

    public void testRequestPipelineAndFinalPipeline() {
        putJsonPipeline("request_pipeline", """
            {"processors": [{"request": {}}]}""");
        putJsonPipeline("final_pipeline", """
            {"processors": [{"final": {"exists":"request"}}]}""");
        final Settings settings = Settings.builder().put(IndexSettings.FINAL_PIPELINE.getKey(), "final_pipeline").build();
        createIndex("index", settings);
        final IndexRequestBuilder index = prepareIndex("index").setId("1");
        index.setSource(Map.of("field", "value"));
        index.setPipeline("request_pipeline");
        index.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        final DocWriteResponse response = index.get();
        assertThat(response.status(), equalTo(RestStatus.CREATED));
        final GetRequestBuilder get = client().prepareGet("index", "1");
        final GetResponse getResponse = get.get();
        assertTrue(getResponse.isExists());
        final Map<String, Object> source = getResponse.getSourceAsMap();
        assertThat(source, hasKey("request"));
        assertTrue((boolean) source.get("request"));
        assertThat(source, hasKey("final"));
        assertTrue((boolean) source.get("final"));
    }

    public void testDefaultAndFinalPipeline() {
        putJsonPipeline("default_pipeline", """
            {"processors": [{"default": {}}]}""");
        putJsonPipeline("final_pipeline", """
            {"processors": [{"final": {"exists":"default"}}]}""");
        final Settings settings = Settings.builder()
            .put(IndexSettings.DEFAULT_PIPELINE.getKey(), "default_pipeline")
            .put(IndexSettings.FINAL_PIPELINE.getKey(), "final_pipeline")
            .build();
        createIndex("index", settings);
        final IndexRequestBuilder index = prepareIndex("index").setId("1");
        index.setSource(Map.of("field", "value"));
        index.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        final DocWriteResponse response = index.get();
        assertThat(response.status(), equalTo(RestStatus.CREATED));
        final GetRequestBuilder get = client().prepareGet("index", "1");
        final GetResponse getResponse = get.get();
        assertTrue(getResponse.isExists());
        final Map<String, Object> source = getResponse.getSourceAsMap();
        assertThat(source, hasKey("default"));
        assertTrue((boolean) source.get("default"));
        assertThat(source, hasKey("final"));
        assertTrue((boolean) source.get("final"));
    }

    public void testDefaultAndFinalPipelineFromTemplates() {
        putJsonPipeline("default_pipeline", """
            {"processors": [{"default": {}}]}""");
        putJsonPipeline("final_pipeline", """
            {"processors": [{"final": {"exists":"default"}}]}""");
        final int lowOrder = randomIntBetween(0, Integer.MAX_VALUE - 1);
        final int highOrder = randomIntBetween(lowOrder + 1, Integer.MAX_VALUE);
        final int finalPipelineOrder;
        final int defaultPipelineOrder;
        if (randomBoolean()) {
            defaultPipelineOrder = lowOrder;
            finalPipelineOrder = highOrder;
        } else {
            defaultPipelineOrder = highOrder;
            finalPipelineOrder = lowOrder;
        }
        final Settings defaultPipelineSettings = Settings.builder()
            .put(IndexSettings.DEFAULT_PIPELINE.getKey(), "default_pipeline")
            .build();
        indicesAdmin().preparePutTemplate("default")
            .setPatterns(List.of("index*"))
            .setOrder(defaultPipelineOrder)
            .setSettings(defaultPipelineSettings)
            .get();
        final Settings finalPipelineSettings = Settings.builder().put(IndexSettings.FINAL_PIPELINE.getKey(), "final_pipeline").build();
        indicesAdmin().preparePutTemplate("final")
            .setPatterns(List.of("index*"))
            .setOrder(finalPipelineOrder)
            .setSettings(finalPipelineSettings)
            .get();
        final IndexRequestBuilder index = prepareIndex("index").setId("1");
        index.setSource(Map.of("field", "value"));
        index.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        final DocWriteResponse response = index.get();
        assertThat(response.status(), equalTo(RestStatus.CREATED));
        final GetRequestBuilder get = client().prepareGet("index", "1");
        final GetResponse getResponse = get.get();
        assertTrue(getResponse.isExists());
        final Map<String, Object> source = getResponse.getSourceAsMap();
        assertThat(source, hasKey("default"));
        assertTrue((boolean) source.get("default"));
        assertThat(source, hasKey("final"));
        assertTrue((boolean) source.get("final"));
    }

    public void testHighOrderFinalPipelinePreferred() throws IOException {
        final int lowOrder = randomIntBetween(0, Integer.MAX_VALUE - 1);
        final int highOrder = randomIntBetween(lowOrder + 1, Integer.MAX_VALUE);
        final Settings lowOrderFinalPipelineSettings = Settings.builder()
            .put(IndexSettings.FINAL_PIPELINE.getKey(), "low_order_final_pipeline")
            .build();
        indicesAdmin().preparePutTemplate("low_order")
            .setPatterns(List.of("index*"))
            .setOrder(lowOrder)
            .setSettings(lowOrderFinalPipelineSettings)
            .get();
        final Settings highOrderFinalPipelineSettings = Settings.builder()
            .put(IndexSettings.FINAL_PIPELINE.getKey(), "high_order_final_pipeline")
            .build();
        indicesAdmin().preparePutTemplate("high_order")
            .setPatterns(List.of("index*"))
            .setOrder(highOrder)
            .setSettings(highOrderFinalPipelineSettings)
            .get();

        // this asserts that the high_order_final_pipeline was selected, without us having to actually create the pipeline etc.
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            prepareIndex("index").setId("1").setSource(Map.of("field", "value"))
        );
        assertThat(e, hasToString(containsString("pipeline with id [high_order_final_pipeline] does not exist")));
    }

    public static class TestPlugin extends Plugin implements IngestPlugin {

        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            return Map.of(
                "default",
                getDefault(parameters, randomBoolean()),
                "final",
                getFinal(parameters, randomBoolean()),
                "request",
                (processorFactories, tag, description, config) -> new AbstractProcessor(tag, description) {
                    @Override
                    public IngestDocument execute(final IngestDocument ingestDocument) throws Exception {
                        ingestDocument.setFieldValue("request", true);
                        return ingestDocument;
                    }

                    @Override
                    public String getType() {
                        return "request";
                    }
                },
                "changing_dest",
                (processorFactories, tag, description, config) -> new AbstractProcessor(tag, description) {
                    @Override
                    public IngestDocument execute(final IngestDocument ingestDocument) throws Exception {
                        ingestDocument.setFieldValue(IngestDocument.Metadata.INDEX.getFieldName(), "target");
                        return ingestDocument;
                    }

                    @Override
                    public String getType() {
                        return "changing_dest";
                    }

                },
                "reroute",
                (processorFactories, tag, description, config) -> {
                    final String dest = Objects.requireNonNullElse(
                        ConfigurationUtils.readOptionalStringProperty(description, tag, config, "dest"),
                        "target"
                    );
                    return new AbstractProcessor(tag, description) {
                        @Override
                        public IngestDocument execute(final IngestDocument ingestDocument) throws Exception {
                            ingestDocument.reroute(dest);
                            return ingestDocument;
                        }

                        @Override
                        public String getType() {
                            return "reroute";
                        }

                    };
                }
            );
        }

        private static Processor.Factory getDefault(Processor.Parameters parameters, boolean async) {
            return (factories, tag, description, config) -> new AbstractProcessor(tag, description) {

                @Override
                public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
                    if (async) {
                        // randomize over sync and async execution
                        randomFrom(parameters.genericExecutor, Runnable::run).accept(() -> {
                            ingestDocument.setFieldValue("default", true);
                            handler.accept(ingestDocument, null);
                        });
                    } else {
                        throw new AssertionError("should not be called");
                    }
                }

                @Override
                public IngestDocument execute(IngestDocument ingestDocument) {
                    if (async) {
                        throw new AssertionError("should not be called");
                    } else {
                        ingestDocument.setFieldValue("default", true);
                        return ingestDocument;
                    }
                }

                @Override
                public String getType() {
                    return "default";
                }

                @Override
                public boolean isAsync() {
                    return async;
                }
            };
        }

        private static Processor.Factory getFinal(Processor.Parameters parameters, boolean async) {
            return (processorFactories, tag, description, config) -> {
                final String exists = (String) config.remove("exists");
                return new AbstractProcessor(tag, description) {

                    @Override
                    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
                        if (async) {
                            // randomize over sync and async execution
                            randomFrom(parameters.genericExecutor, Runnable::run).accept(() -> {
                                if (exists != null) {
                                    if (ingestDocument.getSourceAndMetadata().containsKey(exists) == false) {
                                        handler.accept(
                                            null,
                                            new IllegalStateException(
                                                "expected document to contain ["
                                                    + exists
                                                    + "] but was ["
                                                    + ingestDocument.getSourceAndMetadata()
                                            )
                                        );
                                    }
                                }
                                ingestDocument.setFieldValue("final", true);
                                handler.accept(ingestDocument, null);
                            });
                        } else {
                            throw new AssertionError("should not be called");
                        }
                    }

                    @Override
                    public IngestDocument execute(final IngestDocument ingestDocument) throws Exception {
                        if (async) {
                            throw new AssertionError("should not be called");
                        } else {
                            // this asserts that this pipeline is the final pipeline executed
                            if (exists != null) {
                                if (ingestDocument.getSourceAndMetadata().containsKey(exists) == false) {
                                    throw new AssertionError(
                                        "expected document to contain [" + exists + "] but was [" + ingestDocument.getSourceAndMetadata()
                                    );
                                }
                            }
                            ingestDocument.setFieldValue("final", true);
                            return ingestDocument;
                        }
                    }

                    @Override
                    public String getType() {
                        return "final";
                    }

                    @Override
                    public boolean isAsync() {
                        return async;
                    }
                };
            };
        }
    }

}
