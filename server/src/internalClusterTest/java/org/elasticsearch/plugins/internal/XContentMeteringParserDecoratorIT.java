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
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.FilterXContentParserWrapper;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.xcontent.XContentFactory.cborBuilder;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class XContentMeteringParserDecoratorIT extends ESIntegTestCase {

    private static String TEST_INDEX_NAME = "test-index-name";

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestDocumentParsingProviderPlugin.class, TestEnginePlugin.class);
    }

    // the assertions are done in plugin which is static and will be created by ES server.
    // hence a static flag to make sure it is indeed used
    public static boolean hasWrappedParser;
    public static AtomicLong COUNTER = new AtomicLong();

    public void testDocumentIsReportedUponBulk() throws Exception {
        hasWrappedParser = false;
        client().index(
            new IndexRequest(TEST_INDEX_NAME).id("1").source(jsonBuilder().startObject().field("test", "I am sam i am").endObject())
        ).actionGet();
        assertTrue(hasWrappedParser);
        assertDocumentReported();

        hasWrappedParser = false;
        // the format of the request does not matter
        client().index(
            new IndexRequest(TEST_INDEX_NAME).id("2").source(cborBuilder().startObject().field("test", "I am sam i am").endObject())
        ).actionGet();
        assertTrue(hasWrappedParser);
        assertDocumentReported();

        hasWrappedParser = false;
        // white spaces does not matter
        client().index(new IndexRequest(TEST_INDEX_NAME).id("3").source("""
            {
            "test":

            "I am sam i am"
            }
            """, XContentType.JSON)).actionGet();
        assertTrue(hasWrappedParser);
        assertDocumentReported();
    }

    private void assertDocumentReported() throws Exception {
        assertBusy(() -> assertThat(COUNTER.get(), equalTo(5L)));
        COUNTER.set(0);
    }

    public static class TestEnginePlugin extends Plugin implements EnginePlugin {
        DocumentParsingProvider documentParsingProvider;

        @Override
        public Collection<?> createComponents(PluginServices services) {
            documentParsingProvider = services.documentParsingProvider();
            return super.createComponents(services);
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.of(config -> new InternalEngine(config) {
                @Override
                public IndexResult index(Index index) throws IOException {
                    IndexResult result = super.index(index);

                    DocumentSizeReporter documentParsingReporter = documentParsingProvider.newDocumentSizeReporter(
                        shardId.getIndexName(),
                        config().getMapperService(),
                        DocumentSizeAccumulator.EMPTY_INSTANCE
                    );
                    ParsedDocument parsedDocument = index.parsedDoc();
                    documentParsingReporter.onIndexingCompleted(parsedDocument);

                    return result;
                }
            });
        }
    }

    public static class TestDocumentParsingProviderPlugin extends Plugin implements DocumentParsingProviderPlugin, IngestPlugin {

        public TestDocumentParsingProviderPlugin() {}

        @Override
        public DocumentParsingProvider getDocumentParsingProvider() {
            return new DocumentParsingProvider() {
                @Override
                public <T> XContentMeteringParserDecorator newMeteringParserDecorator(DocWriteRequest<T> request) {
                    return new TestXContentMeteringParserDecorator(0L);
                }

                @Override
                public DocumentSizeReporter newDocumentSizeReporter(
                    String indexName,
                    MapperService mapperService,
                    DocumentSizeAccumulator documentSizeAccumulator
                ) {
                    return new TestDocumentSizeReporter(indexName);
                }
            };
        }
    }

    public static class TestDocumentSizeReporter implements DocumentSizeReporter {

        private final String indexName;

        public TestDocumentSizeReporter(String indexName) {
            this.indexName = indexName;
        }

        @Override
        public void onIndexingCompleted(ParsedDocument parsedDocument) {
            long delta = parsedDocument.getNormalizedSize().ingestedBytes();
            if (delta > 0) {
                COUNTER.addAndGet(delta);
            }
            assertThat(indexName, equalTo(TEST_INDEX_NAME));
        }
    }

    public static class TestXContentMeteringParserDecorator implements XContentMeteringParserDecorator {
        long counter = 0;

        public TestXContentMeteringParserDecorator(long counter) {
            this.counter = counter;
        }

        @Override
        public XContentParser decorate(XContentParser xContentParser) {
            hasWrappedParser = true;
            return new FilterXContentParserWrapper(xContentParser) {

                @Override
                public Token nextToken() throws IOException {
                    counter++;
                    return super.nextToken();
                }
            };
        }

        @Override
        public ParsedDocument.DocumentSize meteredDocumentSize() {
            return new ParsedDocument.DocumentSize(counter, counter);
        }
    }
}
