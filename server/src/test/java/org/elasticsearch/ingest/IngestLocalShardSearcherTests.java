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
import org.apache.lucene.index.DirectoryReader;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexSearcherWrapper;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class IngestLocalShardSearcherTests extends ESSingleNodeTestCase {

    private static final AtomicBoolean SET_SEARCH_WRAPPER = new AtomicBoolean(false);
    private static final AtomicBoolean SEARCH_WRAPPER_APPLIED = new AtomicBoolean(false);

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(TestPlugin.class);
    }

    public void testLocalShardSearcher() throws Exception {
        client().index(new IndexRequest("reference-index").id("1").source("{}", XContentType.JSON)).actionGet();
        client().admin().indices().refresh(new RefreshRequest("reference-index")).actionGet();

        PutPipelineRequest putPipelineRequest = new PutPipelineRequest("my-pipeline", createPipelineSource(), XContentType.JSON);
        client().admin().cluster().putPipeline(putPipelineRequest).get();

        client().index(new IndexRequest("my-index").id("1").source("{}", XContentType.JSON).setPipeline("my-pipeline")).actionGet();
        client().admin().indices().refresh(new RefreshRequest("my-index")).actionGet();

        Map<String, Object> result = client().get(new GetRequest("my-index", "1")).actionGet().getSourceAsMap();
        assertThat(result.size(), equalTo(1));
        assertThat(result.get("id"), equalTo("1"));
        assertThat(SEARCH_WRAPPER_APPLIED.get(), is(false));
    }

    public void testMultipleIndicesAreResolved() throws Exception {
        createIndex("reference-index1", client().admin().indices().prepareCreate("reference-index1")
            .addAlias(new Alias("reference-index")));
        createIndex("reference-index2", client().admin().indices().prepareCreate("reference-index2")
            .addAlias(new Alias("reference-index")));

        PutPipelineRequest putPipelineRequest = new PutPipelineRequest("my-pipeline", createPipelineSource(), XContentType.JSON);
        client().admin().cluster().putPipeline(putPipelineRequest).get();

        IndexRequest indexRequest = new IndexRequest("my-index").id("1").source("{}", XContentType.JSON).setPipeline("my-pipeline");
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> client().index(indexRequest).actionGet());
        assertThat(e.getRootCause(), instanceOf(IllegalStateException.class));
        assertThat(e.getRootCause().getMessage(), equalTo("expression [reference-index] can only point to a single concrete index"));
    }

    public void testMoreThanOnePrimaryShard() throws Exception {
        createIndex("reference-index", Settings.builder().put("index.number_of_shards", 2).build());

        PutPipelineRequest putPipelineRequest = new PutPipelineRequest("my-pipeline", createPipelineSource(), XContentType.JSON);
        client().admin().cluster().putPipeline(putPipelineRequest).get();

        IndexRequest indexRequest = new IndexRequest("my-index").id("1").source("{}", XContentType.JSON).setPipeline("my-pipeline");
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> client().index(indexRequest).actionGet());
        assertThat(e.getRootCause(), instanceOf(IllegalStateException.class));
        assertThat(e.getRootCause().getMessage(), equalTo("index [reference-index] must have 1 shard, but has 2 shards"));
    }

    public void testFailWithFilteredAlias() throws Exception {
        createIndex("reference-index1", client().admin().indices().prepareCreate("reference-index1")
            .addAlias(new Alias("reference-index").filter(QueryBuilders.matchAllQuery())));

        PutPipelineRequest putPipelineRequest = new PutPipelineRequest("my-pipeline", createPipelineSource(), XContentType.JSON);
        client().admin().cluster().putPipeline(putPipelineRequest).get();

        IndexRequest indexRequest = new IndexRequest("my-index").id("1").source("{}", XContentType.JSON).setPipeline("my-pipeline");
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> client().index(indexRequest).actionGet());
        assertThat(e.getRootCause(), instanceOf(IllegalStateException.class));
        assertThat(e.getRootCause().getMessage(), equalTo("expression [reference-index] points an alias with a filter"));
    }

    public void testWithAlias() throws Exception {
        createIndex("reference-index1", client().admin().indices().prepareCreate("reference-index1")
            .addAlias(new Alias("reference-index")));

        client().index(new IndexRequest("reference-index1").id("1").source("{}", XContentType.JSON)).actionGet();
        client().admin().indices().refresh(new RefreshRequest("reference-index1")).actionGet();

        PutPipelineRequest putPipelineRequest = new PutPipelineRequest("my-pipeline", createPipelineSource(), XContentType.JSON);
        client().admin().cluster().putPipeline(putPipelineRequest).get();

        client().index(new IndexRequest("my-index").id("1").source("{}", XContentType.JSON).setPipeline("my-pipeline")).actionGet();
        client().admin().indices().refresh(new RefreshRequest("my-index")).actionGet();

        Map<String, Object> result = client().get(new GetRequest("my-index", "1")).actionGet().getSourceAsMap();
        assertThat(result.size(), equalTo(1));
        assertThat(result.get("id"), equalTo("1"));
    }

    public void testSearchWrapperIsApplied() throws Exception {
        try {
            SET_SEARCH_WRAPPER.set(true);
            client().index(new IndexRequest("reference-index").id("1").source("{}", XContentType.JSON)).actionGet();
            client().admin().indices().refresh(new RefreshRequest("reference-index")).actionGet();

            PutPipelineRequest putPipelineRequest = new PutPipelineRequest("my-pipeline", createPipelineSource(), XContentType.JSON);
            client().admin().cluster().putPipeline(putPipelineRequest).get();

            client().index(new IndexRequest("my-index").id("1").source("{}", XContentType.JSON).setPipeline("my-pipeline")).actionGet();
            client().admin().indices().refresh(new RefreshRequest("my-index")).actionGet();

            Map<String, Object> result = client().get(new GetRequest("my-index", "1")).actionGet().getSourceAsMap();
            assertThat(result.size(), equalTo(1));
            assertThat(result.get("id"), equalTo("1"));
            assertThat(SEARCH_WRAPPER_APPLIED.get(), is(true));
        } finally {
            SET_SEARCH_WRAPPER.set(false);
            SEARCH_WRAPPER_APPLIED.set(false);
        }
    }

    private static BytesReference createPipelineSource() throws IOException {
        return BytesReference.bytes(jsonBuilder().startObject()
            .startArray("processors")
            .startObject()
            .startObject(TestProcessor.NAME)
            .endObject()
            .endObject()
            .endArray()
            .endObject());
    }

    public static class TestPlugin extends Plugin implements IngestPlugin {

        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            return Collections.singletonMap(TestProcessor.NAME, new TestProcessor.Factory(parameters.localShardSearcher));
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            if (SET_SEARCH_WRAPPER.get()) {
                indexModule.setSearcherWrapper(indexService -> new IndexSearcherWrapper() {

                    @Override
                    protected DirectoryReader wrap(DirectoryReader reader) throws IOException {
                        SEARCH_WRAPPER_APPLIED.set(true);
                        return super.wrap(reader);
                    }
                });
            }
        }
    }

    static class TestProcessor extends AbstractProcessor {

        static final String NAME = "test_processor";

        private final Function<String, Engine.Searcher> localShardSearcher;

        TestProcessor(String tag, Function<String, Engine.Searcher> localShardSearcher) {
            super(tag);
            this.localShardSearcher = localShardSearcher;
        }

        @Override
        public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
            String indexExpression = "reference-index";
            try (Engine.Searcher engineSearcher = localShardSearcher.apply(indexExpression)) {
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

            private final Function<String, Engine.Searcher> localShardSearcher;

            Factory(Function<String, Engine.Searcher> localShardSearcher) {
                this.localShardSearcher = localShardSearcher;
            }

            @Override
            public Processor create(Map<String, Processor.Factory> processorFactories,
                                    String tag, Map<String, Object> config) throws Exception {
                return new TestProcessor(tag, localShardSearcher);
            }
        }

    }

}
