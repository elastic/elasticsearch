/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal.documentreporting;

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

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class DocumentReporterWithPipelinesIT extends ESIntegTestCase {

    private static String TEST_INDEX_NAME = "test-index-name";

    public void testDocumentIsReportedWithPipelines() throws IOException {
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

        // assertion in a TestDocumentReporter
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestDocumentReporterPlugin.class, IngestCommonPlugin.class);
    }

    public static class TestDocumentReporterPlugin extends Plugin implements DocumentReporterPlugin, IngestPlugin {

        private static final TestDocumentReporter documentReporter = new TestDocumentReporter();

        public TestDocumentReporterPlugin() {}

        @Override
        public DocumentReporterFactory getDocumentReporter() {
            // returns a static instance, because we want to assert that the wrapping is called only once
            return () -> documentReporter;
        }

    }

    public static class TestDocumentReporter implements DocumentReporter {
        long mapCounter = 0;
        long wrapperCounter = 0;

        @Override
        public XContentParser wrapParser(XContentParser xContentParser) {
            wrapperCounter++;

            return new FilterXContentParserWrapper(xContentParser) {
                @Override
                public Map<String, Object> map() throws IOException {
                    mapCounter++;
                    return super.map();
                }
            };
        }

        @Override
        public void reportDocumentParsed(String indexName) {
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
