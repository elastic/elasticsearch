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
package org.elasticsearch.ingest;

import org.apache.lucene.document.Document;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class IngestSearcherProviderTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(TestPlugin.class);
    }

    public void testSearchProvider() throws Exception {
        client().index(new IndexRequest("reference-index").id("1").source("{}", XContentType.JSON)).actionGet();
        client().admin().indices().refresh(new RefreshRequest("reference-index")).actionGet();

        BytesReference source = BytesReference.bytes(jsonBuilder().startObject()
            .startArray("processors")
            .startObject()
            .startObject(TestProcessor.NAME)
            .endObject()
            .endObject()
            .endArray()
            .endObject());
        PutPipelineRequest putPipelineRequest = new PutPipelineRequest("my-pipeline", source, XContentType.JSON);
        client().admin().cluster().putPipeline(putPipelineRequest).get();

        client().index(new IndexRequest("my-index").id("1").source("{}", XContentType.JSON).setPipeline("my-pipeline")).actionGet();
        client().admin().indices().refresh(new RefreshRequest("my-index")).actionGet();

        Map<String, Object> result = client().get(new GetRequest("my-index", "1")).actionGet().getSourceAsMap();
        assertThat(result.size(), equalTo(1));
        assertThat(result.get("id"), equalTo("1"));
    }

    public static class TestPlugin extends Plugin implements IngestPlugin {

        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            ClusterService clusterService = parameters.ingestService.getClusterService();
            IndexNameExpressionResolver resolver = new IndexNameExpressionResolver();
            return Collections.singletonMap(TestProcessor.NAME, new TestProcessor.Factory(
                parameters.searcherProvider,
                indexExpression -> {
                    ClusterState state = clusterService.state();
                    Index[] resolvedIndices = resolver.concreteIndices(state, IndicesOptions.STRICT_EXPAND_OPEN, indexExpression);
                    if (resolvedIndices.length != 1) {
                        throw new IllegalStateException("expression [" + indexExpression + "] can only point to a single concrete index");
                    }
                    return resolvedIndices[0];
                }
            ));
        }
    }

    static class TestProcessor extends AbstractProcessor {

        static final String NAME = "test_processor";

        private final Function<Index, Engine.Searcher> searchProvider;
        private final Function<String, Index> resolveIndexFunction;

        TestProcessor(String tag, Function<Index, Engine.Searcher> searchProvider, Function<String, Index> resolveIndexFunction) {
            super(tag);
            this.searchProvider = searchProvider;
            this.resolveIndexFunction = resolveIndexFunction;
        }

        @Override
        public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
            Index index = resolveIndexFunction.apply("reference-index");
            try (Engine.Searcher engineSearcher = searchProvider.apply(index)) {
                Document document = engineSearcher.searcher().doc(0);
                ingestDocument.setFieldValue("id", Uid.decodeId(document.getBinaryValue("_id").bytes));
            }
            return ingestDocument;
        }

        @Override
        public String getType() {
            return NAME;
        }

        static class Factory implements Processor.Factory {

            private final Function<Index, Engine.Searcher> searchProvider;
            private final Function<String, Index> resolveIndexFunction;

            Factory(Function<Index, Engine.Searcher> searchProvider, Function<String, Index> resolveIndexFunction) {
                this.searchProvider = searchProvider;
                this.resolveIndexFunction = resolveIndexFunction;
            }

            @Override
            public Processor create(Map<String, Processor.Factory> processorFactories,
                                    String tag, Map<String, Object> config) throws Exception {
                return new TestProcessor(tag, searchProvider, resolveIndexFunction);
            }
        }

    }

}
