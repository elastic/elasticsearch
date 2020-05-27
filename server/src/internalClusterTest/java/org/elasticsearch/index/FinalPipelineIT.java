/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index;

import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.GetPipelineRequest;
import org.elasticsearch.action.ingest.GetPipelineResponse;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
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
        final GetPipelineResponse response = client().admin()
            .cluster()
            .getPipeline(new GetPipelineRequest("default_pipeline", "final_pipeline", "request_pipeline"))
            .actionGet();
        for (final PipelineConfiguration pipeline : response.pipelines()) {
            client().admin().cluster().deletePipeline(new DeletePipelineRequest(pipeline.getId())).actionGet();
        }
    }

    public void testFinalPipeline() {
        final Settings settings = Settings.builder().put(IndexSettings.FINAL_PIPELINE.getKey(), "final_pipeline").build();
        createIndex("index", settings);

        // this asserts that the final_pipeline was used, without us having to actually create the pipeline etc.
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareIndex("index").setId("1").setSource(Map.of("field", "value")).get());
        assertThat(e, hasToString(containsString("pipeline with id [final_pipeline] does not exist")));
    }

    public void testRequestPipelineAndFinalPipeline() {
        final BytesReference requestPipelineBody = new BytesArray("{\"processors\": [{\"request\": {}}]}");
        client().admin()
            .cluster()
            .putPipeline(new PutPipelineRequest("request_pipeline", requestPipelineBody, XContentType.JSON))
            .actionGet();
        final BytesReference finalPipelineBody = new BytesArray("{\"processors\": [{\"final\": {\"exists\":\"request\"}}]}");
        client().admin()
            .cluster()
            .putPipeline(new PutPipelineRequest("final_pipeline", finalPipelineBody, XContentType.JSON))
            .actionGet();
        final Settings settings = Settings.builder().put(IndexSettings.FINAL_PIPELINE.getKey(), "final_pipeline").build();
        createIndex("index", settings);
        final IndexRequestBuilder index = client().prepareIndex("index").setId("1");
        index.setSource(Map.of("field", "value"));
        index.setPipeline("request_pipeline");
        index.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        final IndexResponse response = index.get();
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
        final BytesReference defaultPipelineBody = new BytesArray("{\"processors\": [{\"default\": {}}]}");
        client().admin()
            .cluster()
            .putPipeline(new PutPipelineRequest("default_pipeline", defaultPipelineBody, XContentType.JSON))
            .actionGet();
        final BytesReference finalPipelineBody = new BytesArray("{\"processors\": [{\"final\": {\"exists\":\"default\"}}]}");
        client().admin()
            .cluster()
            .putPipeline(new PutPipelineRequest("final_pipeline", finalPipelineBody, XContentType.JSON))
            .actionGet();
        final Settings settings = Settings.builder()
            .put(IndexSettings.DEFAULT_PIPELINE.getKey(), "default_pipeline")
            .put(IndexSettings.FINAL_PIPELINE.getKey(), "final_pipeline")
            .build();
        createIndex("index", settings);
        final IndexRequestBuilder index = client().prepareIndex("index").setId("1");
        index.setSource(Map.of("field", "value"));
        index.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        final IndexResponse response = index.get();
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
        final BytesReference defaultPipelineBody = new BytesArray("{\"processors\": [{\"default\": {}}]}");
        client().admin()
            .cluster()
            .putPipeline(new PutPipelineRequest("default_pipeline", defaultPipelineBody, XContentType.JSON))
            .actionGet();
        final BytesReference finalPipelineBody = new BytesArray("{\"processors\": [{\"final\": {\"exists\":\"default\"}}]}");
        client().admin()
            .cluster()
            .putPipeline(new PutPipelineRequest("final_pipeline", finalPipelineBody, XContentType.JSON))
            .actionGet();
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
        final Settings defaultPipelineSettings =
            Settings.builder().put(IndexSettings.DEFAULT_PIPELINE.getKey(), "default_pipeline").build();
        admin().indices()
            .preparePutTemplate("default")
            .setPatterns(List.of("index*"))
            .setOrder(defaultPipelineOrder)
            .setSettings(defaultPipelineSettings)
            .get();
        final Settings finalPipelineSettings =
            Settings.builder().put(IndexSettings.FINAL_PIPELINE.getKey(), "final_pipeline").build();
        admin().indices()
            .preparePutTemplate("final")
            .setPatterns(List.of("index*"))
            .setOrder(finalPipelineOrder)
            .setSettings(finalPipelineSettings)
            .get();
        final IndexRequestBuilder index = client().prepareIndex("index").setId("1");
        index.setSource(Map.of("field", "value"));
        index.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        final IndexResponse response = index.get();
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
        final Settings lowOrderFinalPipelineSettings =
            Settings.builder().put(IndexSettings.FINAL_PIPELINE.getKey(), "low_order_final_pipeline").build();
        admin().indices()
            .preparePutTemplate("low_order")
            .setPatterns(List.of("index*"))
            .setOrder(lowOrder)
            .setSettings(lowOrderFinalPipelineSettings)
            .get();
        final Settings highOrderFinalPipelineSettings =
            Settings.builder().put(IndexSettings.FINAL_PIPELINE.getKey(), "high_order_final_pipeline").build();
        admin().indices()
            .preparePutTemplate("high_order")
            .setPatterns(List.of("index*"))
            .setOrder(highOrder)
            .setSettings(highOrderFinalPipelineSettings)
            .get();

        // this asserts that the high_order_final_pipeline was selected, without us having to actually create the pipeline etc.
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareIndex("index").setId("1").setSource(Map.of("field", "value")).get());
        assertThat(e, hasToString(containsString("pipeline with id [high_order_final_pipeline] does not exist")));
    }

    public static class TestPlugin extends Plugin implements IngestPlugin {

        @Override
        public Collection<Object> createComponents(
            final Client client,
            final ClusterService clusterService,
            final ThreadPool threadPool,
            final ResourceWatcherService resourceWatcherService,
            final ScriptService scriptService,
            final NamedXContentRegistry xContentRegistry,
            final Environment environment,
            final NodeEnvironment nodeEnvironment,
            final NamedWriteableRegistry namedWriteableRegistry,
            final IndexNameExpressionResolver expressionResolver,
            final Supplier<RepositoriesService> repositoriesServiceSupplier) {
            return List.of();
        }

        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            return Map.of(
                "default",
                (factories, tag, config) ->
                    new AbstractProcessor(tag) {

                        @Override
                        public IngestDocument execute(final IngestDocument ingestDocument) throws Exception {
                            ingestDocument.setFieldValue("default", true);
                            return ingestDocument;
                        }

                        @Override
                        public String getType() {
                            return "default";
                        }
                    },
                "final",
                (processorFactories, tag, config) -> {
                    final String exists = (String) config.remove("exists");
                    return new AbstractProcessor(tag) {
                        @Override
                        public IngestDocument execute(final IngestDocument ingestDocument) throws Exception {
                            // this asserts that this pipeline is the final pipeline executed
                            if (exists != null) {
                                if (ingestDocument.getSourceAndMetadata().containsKey(exists) == false) {
                                    throw new AssertionError(
                                        "expected document to contain [" + exists + "] but was [" + ingestDocument.getSourceAndMetadata());
                                }
                            }
                            ingestDocument.setFieldValue("final", true);
                            return ingestDocument;
                        }

                        @Override
                        public String getType() {
                            return "final";
                        }
                    };
                },
                "request",
                (processorFactories, tag, config) ->
                    new AbstractProcessor(tag) {
                        @Override
                        public IngestDocument execute(final IngestDocument ingestDocument) throws Exception {
                            ingestDocument.setFieldValue("request", true);
                            return ingestDocument;
                        }

                        @Override
                        public String getType() {
                            return "request";
                        }
                    }
            );
        }
    }

}
