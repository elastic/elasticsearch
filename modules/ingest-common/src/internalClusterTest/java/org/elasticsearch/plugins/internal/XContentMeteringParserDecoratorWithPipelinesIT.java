/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
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
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class XContentMeteringParserDecoratorWithPipelinesIT extends ESIntegTestCase {

    private static String TEST_INDEX_NAME = "test-index-name";
    // the assertions are done in plugin which is static and will be created by ES server.
    // hence a static flag to make sure it is indeed used
    public static volatile boolean hasWrappedParser;
    public static AtomicLong providedFixedSize = new AtomicLong();

    public void testDocumentIsReportedWithPipelines() throws Exception {
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
        assertBusy(() -> {
            // ingest node has used an observer that was counting #map operations
            // and passed that info to newFixedSize observer in TransportShardBulkAction
            assertTrue(hasWrappedParser);
            assertThat(providedFixedSize.get(), equalTo(1L));
        });
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestDocumentParsingProviderPlugin.class, IngestCommonPlugin.class);
    }

    public static class TestDocumentParsingProviderPlugin extends Plugin implements DocumentParsingProviderPlugin, IngestPlugin {

        public TestDocumentParsingProviderPlugin() {}

        @Override
        public DocumentParsingProvider getDocumentParsingProvider() {
            // returns a static instance, because we want to assert that the wrapping is called only once
            return new DocumentParsingProvider() {
                @Override
                public <T> XContentMeteringParserDecorator newMeteringParserDecorator(DocWriteRequest<T> request) {
                    if (request instanceof IndexRequest indexRequest && indexRequest.getNormalisedBytesParsed() > 0) {
                        long normalisedBytesParsed = indexRequest.getNormalisedBytesParsed();
                        providedFixedSize.set(normalisedBytesParsed);
                        return new TestXContentMeteringParserDecorator(normalisedBytesParsed);
                    }
                    return new TestXContentMeteringParserDecorator(0L);
                }

                @Override
                public DocumentSizeReporter newDocumentSizeReporter(
                    String indexName,
                    MapperService mapperService,
                    DocumentSizeAccumulator documentSizeAccumulator
                ) {
                    return DocumentSizeReporter.EMPTY_INSTANCE;
                }
            };
        }
    }

    public static class TestXContentMeteringParserDecorator implements XContentMeteringParserDecorator {
        long mapCounter = 0;

        public TestXContentMeteringParserDecorator(long mapCounter) {
            this.mapCounter = mapCounter;
        }

        @Override
        public XContentParser decorate(XContentParser xContentParser) {
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
        public ParsedDocument.DocumentSize meteredDocumentSize() {
            return new ParsedDocument.DocumentSize(mapCounter, 0);
        }
    }

}
