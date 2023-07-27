/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal.documentreporting;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.FilterXContentParserWrapper;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xcontent.XContentFactory.cborBuilder;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class DocumentReporterIT extends ESIntegTestCase {

    private static String TEST_INDEX_NAME = "test-index-name";

    public void testDocumentIsReportedUponBulk() throws IOException {
        client().index(
            new IndexRequest(TEST_INDEX_NAME).id("1").source(jsonBuilder().startObject().field("test", "I am sam i am").endObject())
        ).actionGet();

        // the format of the request does not matter
        client().index(
            new IndexRequest(TEST_INDEX_NAME).id("2").source(cborBuilder().startObject().field("test", "I am sam i am").endObject())
        ).actionGet();

        // white spaces does not matter
        client().index(new IndexRequest(TEST_INDEX_NAME).id("3").source("""
            {
            "test":

            "I am sam i am"
            }
            """, XContentType.JSON)).actionGet();

        // assertion in a TestDocumentReporter
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestDocumentReporterPlugin.class);
    }

    public static class TestDocumentReporterPlugin extends Plugin implements DocumentReporterPlugin, IngestPlugin {

        public TestDocumentReporterPlugin() {}

        @Override
        public DocumentReporterFactory getDocumentReporter() {
            return () -> new TestDocumentReporter();
        }
    }

    public static class TestDocumentReporter implements DocumentReporter {
        public long counter = 0;

        @Override
        public XContentParser wrapParser(XContentParser xContentParser) {
            return new FilterXContentParserWrapper(xContentParser) {
                @Override
                public Token nextToken() throws IOException {
                    counter++;
                    return super.nextToken();
                }
            };
        }

        @Override
        public void reportDocumentParsed(String indexName) {
            assertThat(indexName, equalTo(TEST_INDEX_NAME));
            assertThat(counter, equalTo(5L));
        }
    }
}
