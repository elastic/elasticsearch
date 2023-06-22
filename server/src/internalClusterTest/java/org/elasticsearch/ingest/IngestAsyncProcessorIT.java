/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.ingest;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

/**
 * The purpose of this test is to verify that when a processor executes an operation asynchronously that
 * the expected result is the same as if the same operation happens synchronously.
 *
 * In this test two test processor are defined that basically do the same operation, but a single processor
 * executes asynchronously. The result of the operation should be the same and also the order in which the
 * bulk responses are returned should be the same as how the corresponding index requests were defined.
 */
public class IngestAsyncProcessorIT extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(TestPlugin.class);
    }

    public void testAsyncProcessorImplementation() {
        // A pipeline with 2 processors: the test async processor and sync test processor.
        BytesReference pipelineBody = new BytesArray("{\"processors\": [{\"test-async\": {}, \"test\": {}}]}");
        clusterAdmin().putPipeline(new PutPipelineRequest("_id", pipelineBody, XContentType.JSON)).actionGet();

        BulkRequest bulkRequest = new BulkRequest();
        int numDocs = randomIntBetween(8, 256);
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(new IndexRequest("foobar").id(Integer.toString(i)).source("{}", XContentType.JSON).setPipeline("_id"));
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.getItems().length, equalTo(numDocs));
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            assertThat(bulkResponse.getItems()[i].getId(), equalTo(id));
            GetResponse getResponse = client().get(new GetRequest("foobar", id)).actionGet();
            // The expected result of async test processor:
            assertThat(getResponse.getSource().get("foo"), equalTo("bar-" + id));
            // The expected result of sync test processor:
            assertThat(getResponse.getSource().get("bar"), equalTo("baz-" + id));
        }
    }

    public static class TestPlugin extends Plugin implements IngestPlugin {

        private ThreadPool threadPool;

        @Override
        public Collection<Object> createComponents(
            Client client,
            ClusterService clusterService,
            ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService,
            ScriptService scriptService,
            NamedXContentRegistry xContentRegistry,
            Environment environment,
            NodeEnvironment nodeEnvironment,
            NamedWriteableRegistry namedWriteableRegistry,
            IndexNameExpressionResolver expressionResolver,
            Supplier<RepositoriesService> repositoriesServiceSupplier,
            Tracer tracer,
            AllocationService allocationService
        ) {
            this.threadPool = threadPool;
            return List.of();
        }

        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            return Map.of("test-async", (factories, tag, description, config) -> new AbstractProcessor(tag, description) {

                @Override
                public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
                    threadPool.generic().execute(() -> {
                        String id = (String) ingestDocument.getSourceAndMetadata().get("_id");
                        if (usually()) {
                            try {
                                Thread.sleep(10);
                            } catch (InterruptedException e) {
                                // ignore
                            }
                        }
                        ingestDocument.setFieldValue("foo", "bar-" + id);
                        handler.accept(ingestDocument, null);
                    });
                }

                @Override
                public String getType() {
                    return "test-async";
                }

                @Override
                public boolean isAsync() {
                    return true;
                }

            }, "test", (processorFactories, tag, description, config) -> new AbstractProcessor(tag, description) {
                @Override
                public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
                    String id = (String) ingestDocument.getSourceAndMetadata().get("_id");
                    ingestDocument.setFieldValue("bar", "baz-" + id);
                    return ingestDocument;
                }

                @Override
                public String getType() {
                    return "test";
                }
            });
        }
    }
}
