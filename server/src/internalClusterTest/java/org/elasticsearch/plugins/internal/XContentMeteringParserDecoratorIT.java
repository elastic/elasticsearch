/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins.internal;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.UnicodeUtil;
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
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.mapper.BatchMappingContext;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.sourcebatch.LuceneColumn;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xcontent.FilterXContentParserWrapper;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.IntToLongFunction;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xcontent.XContentFactory.cborBuilder;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class XContentMeteringParserDecoratorIT extends ESIntegTestCase {

    private static final String TEST_INDEX_NAME = "test-index-name";
    private static final String TEST_VALUE = "I am sam i am";
    // the test accounting charges the UTF-8 bytes of string values and nothing else
    private static final long SIZE_PER_DOCUMENT = TEST_VALUE.length();

    private final boolean batchIndexing;

    public XContentMeteringParserDecoratorIT(boolean batchIndexing) {
        this.batchIndexing = batchIndexing;
    }

    @ParametersFactory(argumentFormatting = "batchIndexing=%s")
    public static Iterable<Object[]> parameters() {
        final List<Object[]> parameters = new ArrayList<>();
        if (BatchIndexingEnabled.FEATURE_FLAG.isEnabled()) {
            parameters.add(new Object[] { true });
        }
        parameters.add(new Object[] { false });
        return parameters;
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        if (BatchIndexingEnabled.FEATURE_FLAG.isEnabled()) {
            // the setting rejects true while the feature flag is off, so only opt in where the flag is on
            settings.put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), batchIndexing);
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
    public static boolean hasMeteredBatch;
    public static AtomicLong COUNTER = new AtomicLong();

    public void testDocumentIsReportedUponBulk() throws Exception {
        hasWrappedParser = false;
        client().index(new IndexRequest(TEST_INDEX_NAME).id("1").source(jsonBuilder().startObject().field("test", TEST_VALUE).endObject()))
            .actionGet();
        assertTrue(hasWrappedParser);
        assertDocumentReported();

        hasWrappedParser = false;
        // the format of the request does not matter
        client().index(new IndexRequest(TEST_INDEX_NAME).id("2").source(cborBuilder().startObject().field("test", TEST_VALUE).endObject()))
            .actionGet();
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
        assertAcked(
            indicesAdmin().prepareCreate(TEST_INDEX_NAME)
                .setSettings(Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()))
                .setMapping("""
                    { "dynamic": "strict", "properties": { "test": { "type": "keyword" } } }
                    """)
        );
        ensureGreen(TEST_INDEX_NAME);

        hasWrappedParser = false;
        hasMeteredBatch = false;
        final int numDocs = randomIntBetween(2, 10);
        final BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(
                new IndexRequest(TEST_INDEX_NAME).id(String.valueOf(i))
                    .opType(DocWriteRequest.OpType.CREATE)
                    .source(jsonBuilder().startObject().field("test", TEST_VALUE).endObject())
            );
        }
        final Logger batchLogger = LogManager.getLogger(ShardBatchIndexer.class);
        final Level origLevel = batchLogger.getLevel();
        Loggers.setLevel(batchLogger, Level.TRACE);
        try (var mockLog = MockLog.capture(ShardBatchIndexer.class)) {
            final String batchIndexed = "batch indexed * operations on primary shard *";
            mockLog.addExpectation(
                batchIndexing
                    ? new MockLog.SeenEventExpectation(
                        "batch indexed on primary",
                        ShardBatchIndexer.class.getName(),
                        Level.TRACE,
                        batchIndexed
                    )
                    : new MockLog.UnseenEventExpectation(
                        "no batch on primary",
                        ShardBatchIndexer.class.getName(),
                        Level.TRACE,
                        batchIndexed
                    )
            );
            assertNoFailures(client().bulk(bulkRequest).actionGet());
            mockLog.assertAllExpectationsMatched();
        } finally {
            Loggers.setLevel(batchLogger, origLevel);
        }
        // the batch path charges the chunk under the model and never decorates a parser, the sequential path does the opposite, and
        // both report the same size per document
        assertThat("batch path metered the chunk", hasMeteredBatch, equalTo(batchIndexing));
        assertThat("sequential path decorated the parser", hasWrappedParser, equalTo(batchIndexing == false));
        assertBusy(() -> assertThat(COUNTER.get(), equalTo(SIZE_PER_DOCUMENT * numDocs)));
        COUNTER.set(0);
    }

    private void assertDocumentReported() throws Exception {
        assertBusy(() -> assertThat(COUNTER.get(), equalTo(SIZE_PER_DOCUMENT)));
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
                public void onBatchParsingCompleted(BatchMappingContext context) {
                    // the batch path reports the whole chunk through this hook, the way the stateless IndexEngine does
                    newDocumentSizeReporter().onBatchParsingCompleted(context.docCount(), context::normalizedSize, context::addColumn);
                }

                private void reportDocumentSize(ParsedDocument parsedDocument) {
                    newDocumentSizeReporter().onIndexingCompleted(parsedDocument, Engine.Operation.Origin.PRIMARY);
                }

                private DocumentSizeReporter newDocumentSizeReporter() {
                    return documentParsingProvider.newDocumentSizeReporter(
                        shardId.getIndex(),
                        config().getMapperService(),
                        DocumentSizeAccumulator.EMPTY_INSTANCE
                    );
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
                public MeteringModel newMeteringModel() {
                    return TEST_MODEL;
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

    // the accounting of TestXContentMeteringParserDecorator per value, for the batch path
    private static final MeteringModel TEST_MODEL = new MeteringModel() {
        @Override
        public boolean hasExcludedRootField(String fieldName) {
            return false;
        }

        @Override
        public long stringBytes(int utf8Length) {
            hasMeteredBatch = true;
            return utf8Length;
        }

        @Override
        public long binaryBytes(int length) {
            return 0;
        }

        @Override
        public long numberBytes() {
            return 0;
        }

        @Override
        public long booleanBytes() {
            return 0;
        }
    };

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

        @Override
        public void onBatchParsingCompleted(int docCount, IntToLongFunction normalizedSizeByDoc, Consumer<LuceneColumn> columnConsumer) {
            for (int doc = 0; doc < docCount; doc++) {
                long delta = normalizedSizeByDoc.applyAsLong(doc);
                if (delta > XContentMeteringParserDecorator.UNKNOWN_SIZE) {
                    COUNTER.addAndGet(delta);
                }
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
                    final Token token = super.nextToken();
                    if (token == Token.VALUE_STRING) {
                        final String text = text();
                        counter += UnicodeUtil.calcUTF16toUTF8Length(text, 0, text.length());
                    }
                    return token;
                }
            };
        }

        @Override
        public long meteredDocumentSize() {
            return counter;
        }
    }
}
