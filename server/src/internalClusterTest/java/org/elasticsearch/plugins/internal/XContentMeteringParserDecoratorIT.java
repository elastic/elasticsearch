/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins.internal;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BatchIndexingEnabled;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.ShardBatchIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineBatch;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xcontent.FilterXContentParserWrapper;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xcontent.XContentFactory.cborBuilder;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class XContentMeteringParserDecoratorIT extends ESIntegTestCase {

    private static final String TEST_INDEX_NAME = "test-index-name";

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        if (BatchIndexingEnabled.FEATURE_FLAG.isEnabled()) {
            // the setting rejects true while the feature flag is off, so only opt in where the flag is on
            settings.put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true);
        }
        return settings.build();
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

    public void testDocumentIsReportedUponBatchBulk() throws Exception {
        assumeTrue("batch indexing requires the batch_indexing feature flag", BatchIndexingEnabled.FEATURE_FLAG.isEnabled());
        assertAcked(
            indicesAdmin().prepareCreate(TEST_INDEX_NAME)
                .setSettings(
                    Settings.builder()
                        .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                        .put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true)
                )
                .setMapping("""
                    { "dynamic": "strict", "properties": { "test": { "type": "keyword" } } }
                    """)
        );
        ensureGreen(TEST_INDEX_NAME);

        hasWrappedParser = false;
        final int numDocs = randomIntBetween(2, 10);
        final BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(
                new IndexRequest(TEST_INDEX_NAME).id(String.valueOf(i))
                    .opType(DocWriteRequest.OpType.CREATE)
                    .source(jsonBuilder().startObject().field("test", "I am sam i am").endObject())
            );
        }

        final Logger batchLogger = LogManager.getLogger(ShardBatchIndexer.class);
        final Level origLevel = batchLogger.getLevel();
        Loggers.setLevel(batchLogger, Level.TRACE);
        try (var mockLog = MockLog.capture(ShardBatchIndexer.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "batch indexed on primary",
                    ShardBatchIndexer.class.getName(),
                    Level.TRACE,
                    "batch indexed * operations on primary shard *"
                )
            );
            assertNoFailures(client().bulk(bulkRequest).actionGet());
            mockLog.assertAllExpectationsMatched();
        } finally {
            Loggers.setLevel(batchLogger, origLevel);
        }
        // every row was streamed through the metering decorator and reported once, with the same token count as the
        // sequential path produces for the same document
        assertTrue(hasWrappedParser);
        assertBusy(() -> assertThat(COUNTER.get(), equalTo(5L * numDocs)));
        COUNTER.set(0);
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
                    reportDocumentSize(index.parsedDoc());
                    return result;
                }

                @Override
                public List<IndexResult> indexBatch(EngineBatch batch) throws IOException {
                    List<Index> operations = batch.batch().materializeIndexOps();
                    List<IndexResult> results = super.indexBatch(batch);
                    for (int i = 0; i < results.size(); i++) {
                        if (results.get(i).getResultType() == Result.Type.SUCCESS) {
                            reportDocumentSize(operations.get(i).parsedDoc());
                        }
                    }
                    return results;
                }

                private void reportDocumentSize(ParsedDocument parsedDocument) {
                    DocumentSizeReporter documentParsingReporter = documentParsingProvider.newDocumentSizeReporter(
                        shardId.getIndex(),
                        config().getMapperService(),
                        DocumentSizeAccumulator.EMPTY_INSTANCE
                    );
                    documentParsingReporter.onIndexingCompleted(parsedDocument, Engine.Operation.Origin.PRIMARY);
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
                public XContentMeteringParserDecorator newMeteringParserDecorator() {
                    return new TestXContentMeteringParserDecorator(0L);
                }

                @Override
                public DocumentSizeReporter newDocumentSizeReporter(
                    Index index,
                    MapperService mapperService,
                    DocumentSizeAccumulator documentSizeAccumulator
                ) {
                    return new TestDocumentSizeReporter(index);
                }
            };
        }
    }

    public static class TestDocumentSizeReporter implements DocumentSizeReporter {

        private final Index index;

        public TestDocumentSizeReporter(Index index) {
            this.index = index;
        }

        @Override
        public void onIndexingCompleted(ParsedDocument parsedDocument, Engine.Operation.Origin origin) {
            long delta = parsedDocument.getNormalizedSize();
            if (delta > XContentMeteringParserDecorator.UNKNOWN_SIZE) {
                COUNTER.addAndGet(delta);
            }
            assertThat(index.getName(), equalTo(TEST_INDEX_NAME));
        }
    }

    public static class TestXContentMeteringParserDecorator implements XContentMeteringParserDecorator {
        long counter = 0;

        public TestXContentMeteringParserDecorator(long counter) {
            this.counter = counter;
        }

        @Override
        public XContentParser decorate(XContentParser xContentParser, Mapping mapping) {
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
        public long meteredDocumentSize() {
            return counter;
        }
    }
}
