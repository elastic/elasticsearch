/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.FilterXContentParserWrapper;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class DocumentParsingObserverWithPipelinesIT extends ESIntegTestCase {

    private static String TEST_INDEX_NAME = "test-index-name";
    // the assertions are done in plugin which is static and will be created by ES server.
    // hence a static flag to make sure it is indeed used
    public static boolean hasWrappedParser;

    public void testDocumentIsReportedWithPipelines() throws IOException {
        hasWrappedParser = false;
        // pipeline adding fields, changing destination is not affecting reporting
        final BytesReference pipelineBody = new BytesArray("""
            {
              "processors": [
                {
                   "set": {
                     "field": "my-text-field",
                     "value": "xxxx"
                   }
                 },
                 {
                   "set": {
                     "field": "my-boolean-field",
                     "value": true
                   }
                 }
              ]
            }
            """);
        clusterAdmin().putPipeline(new PutPipelineRequest("pipeline", pipelineBody, XContentType.JSON)).actionGet();

        client().index(
            new IndexRequest(TEST_INDEX_NAME).setPipeline("pipeline")
                .id("1")
                .source(jsonBuilder().startObject().field("test", "I am sam i am").endObject())
        ).actionGet();
        assertTrue(hasWrappedParser);
        // there are more assertions in a TestDocumentParsingObserver
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestDocumentParsingObserverPlugin.class, IngestCommonPlugin.class);
    }

    public static class TestDocumentParsingObserverPlugin extends Plugin implements DocumentParsingObserverPlugin, IngestPlugin {

        private static final TestDocumentParsingObserver DOCUMENT_PARSING_OBSERVER = new TestDocumentParsingObserver();

        public TestDocumentParsingObserverPlugin() {}

        @Override
        public Supplier<DocumentParsingObserver> getDocumentParsingObserverSupplier() {
            // returns a static instance, because we want to assert that the wrapping is called only once
            return () -> DOCUMENT_PARSING_OBSERVER;
        }

    }

    public static class TestDocumentParsingObserver implements DocumentParsingObserver {
        long mapCounter = 0;
        long wrapperCounter = 0;
        String indexName;

        @Override
        public XContentParser wrapParser(XContentParser xContentParser) {
            wrapperCounter++;
            hasWrappedParser = true;
            return new FilterXContentParserWrapper(xContentParser) {

                @Override
                public Map<String, Object> map() throws IOException {
                    mapCounter++;
                    return super.map();
                }
            };
        }

        @Override
        public void setIndexName(String indexName) {
            this.indexName = indexName;
        }

        @Override
        public void close() {
            assertThat(indexName, equalTo(TEST_INDEX_NAME));
            assertThat(mapCounter, equalTo(1L));

            assertThat(
                "we only want to use a wrapped counter once, once document is reported it no longer needs to wrap",
                wrapperCounter,
                equalTo(1L)
            );
        }

    }

}
