/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.logging.MockAppender;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.engine.IndexOperationBatch;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.xcontent.XContentType;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexingFailuresDiagnosticListenerTests extends ESTestCase {

    private static final long WINDOW_NANOS = TimeUnit.HOURS.toNanos(1);
    private static final int MAX_WARNINGS_PER_WINDOW = 5;

    static MockAppender appender;
    static Logger testLogger1 = LogManager.getLogger(IndexingFailuresDiagnosticListener.class);
    static Level origLogLevel = testLogger1.getLevel();
    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings(
        "index",
        Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.getName()).build()
    );

    @BeforeClass
    public static void init() throws IllegalAccessException {
        appender = new MockAppender("mock_appender");
        appender.start();
        Loggers.addAppender(testLogger1, appender);
        Loggers.setLevel(testLogger1, randomBoolean() ? Level.DEBUG : Level.TRACE);
    }

    @AfterClass
    public static void cleanup() {
        Loggers.removeAppender(testLogger1, appender);
        appender.stop();

        Loggers.setLevel(testLogger1, origLogLevel);
    }

    public void testPostIndexException() {
        var shardId = ShardId.fromString("[index][123]");
        var mockShard = mock(IndexShard.class);
        var shardRouting = TestShardRouting.newShardRouting(shardId, "node-id", true, ShardRoutingState.STARTED);
        when(mockShard.routingEntry()).thenReturn(shardRouting);
        when(mockShard.getOperationPrimaryTerm()).thenReturn(1L);
        when(mockShard.indexSettings()).thenReturn(INDEX_SETTINGS);
        IndexingFailuresDiagnosticListener indexingFailuresDiagnosticListener = new IndexingFailuresDiagnosticListener(mockShard);

        ParsedDocument doc = EngineTestCase.createParsedDoc("1", null);
        Engine.Index index = new Engine.Index(Uid.encodeId("doc_id"), 1, doc);
        indexingFailuresDiagnosticListener.postIndex(shardId, index, new RuntimeException("test exception"));
        String message = appender.getLastEventAndReset().getMessage().getFormattedMessage();
        assertThat(
            message,
            equalTo(
                "index-fail [1] seq# [-2] allocation-id ["
                    + shardRouting.allocationId()
                    + "] primaryTerm [1] operationPrimaryTerm [1] origin [PRIMARY] index [index] index-mode [logsdb]"
            )
        );
    }

    public void testPostIndexExceptionInfoLevel() {
        var previousLevel = testLogger1.getLevel();
        try {
            Loggers.setLevel(testLogger1, randomBoolean() ? Level.INFO : Level.WARN);
            var shardId = ShardId.fromString("[index][123]");
            var mockShard = mock(IndexShard.class);
            var shardRouting = TestShardRouting.newShardRouting(shardId, "node-id", true, ShardRoutingState.STARTED);
            when(mockShard.routingEntry()).thenReturn(shardRouting);
            when(mockShard.getOperationPrimaryTerm()).thenReturn(1L);
            when(mockShard.indexSettings()).thenReturn(INDEX_SETTINGS);
            IndexingFailuresDiagnosticListener indexingFailuresDiagnosticListener = new IndexingFailuresDiagnosticListener(mockShard);

            ParsedDocument doc = EngineTestCase.createParsedDoc("1", null);
            Engine.Index index = new Engine.Index(Uid.encodeId("doc_id"), 1, doc);
            indexingFailuresDiagnosticListener.postIndex(shardId, index, new RuntimeException("test exception"));
            assertThat(appender.getLastEventAndReset(), nullValue());
        } finally {
            Loggers.setLevel(testLogger1, previousLevel);
        }
    }

    public void testPostIndexFailure() {
        var shardId = ShardId.fromString("[index][123]");
        var mockShard = mock(IndexShard.class);
        var shardRouting = TestShardRouting.newShardRouting(shardId, "node-id", true, ShardRoutingState.STARTED);
        when(mockShard.routingEntry()).thenReturn(shardRouting);
        when(mockShard.getOperationPrimaryTerm()).thenReturn(1L);
        when(mockShard.indexSettings()).thenReturn(INDEX_SETTINGS);
        IndexingFailuresDiagnosticListener indexingFailuresDiagnosticListener = new IndexingFailuresDiagnosticListener(mockShard);

        ParsedDocument doc = EngineTestCase.createParsedDoc("1", null);
        Engine.Index index = new Engine.Index(Uid.encodeId("doc_id"), 1, doc);
        Engine.IndexResult indexResult = mock(Engine.IndexResult.class);
        when(indexResult.getResultType()).thenReturn(Engine.Result.Type.FAILURE);
        when(indexResult.getFailure()).thenReturn(new RuntimeException("test exception"));
        indexingFailuresDiagnosticListener.postIndex(shardId, index, indexResult);
        String message = appender.getLastEventAndReset().getMessage().getFormattedMessage();
        assertThat(
            message,
            equalTo(
                "index-fail [1] seq# [-2] allocation-id ["
                    + shardRouting.allocationId()
                    + "] primaryTerm [1] operationPrimaryTerm [1] origin [PRIMARY] index [index] index-mode [logsdb]"
            )
        );
    }

    public void testPostIndex() {
        var shardId = ShardId.fromString("[index][123]");
        var mockShard = mock(IndexShard.class);
        var shardRouting = TestShardRouting.newShardRouting(shardId, "node-id", true, ShardRoutingState.STARTED);
        when(mockShard.routingEntry()).thenReturn(shardRouting);
        when(mockShard.getOperationPrimaryTerm()).thenReturn(1L);
        when(mockShard.indexSettings()).thenReturn(INDEX_SETTINGS);
        IndexingFailuresDiagnosticListener indexingFailuresDiagnosticListener = new IndexingFailuresDiagnosticListener(mockShard);

        ParsedDocument doc = EngineTestCase.createParsedDoc("1", null);
        Engine.Index index = new Engine.Index(Uid.encodeId("doc_id"), 1, doc);
        Engine.IndexResult indexResult = mock(Engine.IndexResult.class);
        when(indexResult.getResultType()).thenReturn(Engine.Result.Type.SUCCESS);
        when(indexResult.getFailure()).thenReturn(new RuntimeException("test exception"));
        indexingFailuresDiagnosticListener.postIndex(shardId, index, indexResult);
        assertThat(appender.getLastEventAndReset(), nullValue());
    }

    public void testPostIndexBatchFailure() {
        var shardId = ShardId.fromString("[index][123]");
        var mockShard = mock(IndexShard.class);
        var shardRouting = TestShardRouting.newShardRouting(shardId, "node-id", true, ShardRoutingState.STARTED);
        when(mockShard.routingEntry()).thenReturn(shardRouting);
        when(mockShard.getOperationPrimaryTerm()).thenReturn(1L);
        when(mockShard.indexSettings()).thenReturn(INDEX_SETTINGS);
        IndexingFailuresDiagnosticListener listener = new IndexingFailuresDiagnosticListener(mockShard);

        IndexOperationBatch batch = primaryBatch(1);
        Engine.IndexResult indexResult = mock(Engine.IndexResult.class);
        when(indexResult.getResultType()).thenReturn(Engine.Result.Type.FAILURE);
        when(indexResult.getFailure()).thenReturn(new RuntimeException("test exception"));
        listener.postIndexBatch(shardId, batch, List.of(indexResult));
        String message = appender.getLastEventAndReset().getMessage().getFormattedMessage();
        assertThat(
            message,
            equalTo(
                "real-index-fail [doc-0] seq# [-2] allocation-id ["
                    + shardRouting.allocationId()
                    + "] primaryTerm [1] operationPrimaryTerm [1] origin [PRIMARY] index [index] index-mode [logsdb]"
            )
        );
    }

    public void testPostIndexBatchEngineException() {
        var shardId = ShardId.fromString("[index][123]");
        var mockShard = mock(IndexShard.class);
        var shardRouting = TestShardRouting.newShardRouting(shardId, "node-id", true, ShardRoutingState.STARTED);
        when(mockShard.routingEntry()).thenReturn(shardRouting);
        when(mockShard.getOperationPrimaryTerm()).thenReturn(1L);
        when(mockShard.indexSettings()).thenReturn(INDEX_SETTINGS);
        IndexingFailuresDiagnosticListener listener = new IndexingFailuresDiagnosticListener(mockShard);

        final int docCount = randomIntBetween(1, 8);
        IndexOperationBatch batch = primaryBatch(docCount);
        listener.postIndexBatch(shardId, batch, new RuntimeException("test exception"));
        // engine-level failure: one line summarises the whole batch, not one line per doc
        String message = appender.getLastEventAndReset().getMessage().getFormattedMessage();
        assertThat(
            message,
            equalTo(
                "index-fail batch docCount ["
                    + docCount
                    + "] startingSeqNo [-2] allocation-id ["
                    + shardRouting.allocationId()
                    + "] primaryTerm [1] operationPrimaryTerm [1] origin [PRIMARY] index [index] index-mode [logsdb]"
            )
        );
    }

    public void testPostIndexBatchSuccessNotLogged() {
        var shardId = ShardId.fromString("[index][123]");
        var mockShard = mock(IndexShard.class);
        var shardRouting = TestShardRouting.newShardRouting(shardId, "node-id", true, ShardRoutingState.STARTED);
        when(mockShard.routingEntry()).thenReturn(shardRouting);
        when(mockShard.getOperationPrimaryTerm()).thenReturn(1L);
        when(mockShard.indexSettings()).thenReturn(INDEX_SETTINGS);
        IndexingFailuresDiagnosticListener listener = new IndexingFailuresDiagnosticListener(mockShard);

        IndexOperationBatch batch = primaryBatch(1);
        Engine.IndexResult indexResult = mock(Engine.IndexResult.class);
        when(indexResult.getResultType()).thenReturn(Engine.Result.Type.SUCCESS);
        listener.postIndexBatch(shardId, batch, List.of(indexResult));
        assertThat(appender.getLastEventAndReset(), nullValue());
    }

    public void testPostIndexBatchUserErrorLoggedAtTrace() {
        LogEvent event = postIndexBatchFailure(Level.TRACE, randomUserError());
        assertThat(event.getLevel(), equalTo(Level.TRACE));
        assertThat(event.getMessage().getFormattedMessage(), startsWith("index-fail [doc-0]"));
    }

    public void testPostIndexBatchUserErrorNotLoggedAtDebug() {
        assertThat(postIndexBatchFailure(Level.DEBUG, randomUserError()), nullValue());
    }

    public void testPostIndexBatchRealErrorLoggedAtWarnWhenWithinRateLimit() {
        Exception realError = randomFrom(
            new RuntimeException("test exception"),
            new IllegalArgumentException("other message"),
            new IllegalArgumentException()
        );
        LogEvent event = postIndexBatchFailure(Level.TRACE, realError);
        assertThat(event.getLevel(), equalTo(Level.WARN));
        assertThat(event.getMessage().getFormattedMessage(), startsWith("real-index-fail [doc-0]"));
    }

    public void testRealErrorsLoggedAtWarnUpToLimitThenDebugWithinWindow() {
        var clock = new AtomicLong(randomWindowIndex() * WINDOW_NANOS + randomLongBetween(0, WINDOW_NANOS - 1));
        var listener = listenerWithClock(clock);
        var shardId = ShardId.fromString("[index][123]");
        var result = failedResult(new RuntimeException("test exception"));

        int failures = randomIntBetween(MAX_WARNINGS_PER_WINDOW + 1, 50);
        for (int i = 0; i < failures; i++) {
            appender.getLastEventAndReset();
            listener.postIndexBatch(shardId, primaryBatch(1), List.of(result));
            Level expected = i < MAX_WARNINGS_PER_WINDOW ? Level.WARN : Level.DEBUG;
            assertThat("failure " + i, appender.getLastEventAndReset().getLevel(), equalTo(expected));
        }
    }

    public void testPermitsResetInEachNewWindow() {
        var clock = new AtomicLong();
        var listener = listenerWithClock(clock);
        long windowIndex = randomWindowIndex();

        int windows = randomIntBetween(2, 5);
        for (int w = 0; w < windows; w++) {
            clock.set(windowIndex * WINDOW_NANOS + randomLongBetween(0, WINDOW_NANOS - 1));
            for (int i = 0; i < MAX_WARNINGS_PER_WINDOW; i++) {
                assertTrue("window " + w + " permit " + i, listener.withinRateLimit());
            }
            int extraCalls = randomIntBetween(1, 20);
            for (int i = 0; i < extraCalls; i++) {
                assertFalse("window " + w + " extra call " + i, listener.withinRateLimit());
            }
            windowIndex += randomLongBetween(1, 100);
        }
    }

    public void testWindowBoundaryIsExact() {
        long windowStart = randomWindowIndex() * WINDOW_NANOS;
        var clock = new AtomicLong(windowStart);
        var listener = listenerWithClock(clock);
        for (int i = 0; i < MAX_WARNINGS_PER_WINDOW; i++) {
            assertTrue(listener.withinRateLimit());
        }

        clock.set(windowStart + WINDOW_NANOS - 1);
        assertFalse(listener.withinRateLimit());

        clock.set(windowStart + WINDOW_NANOS);
        assertTrue(listener.withinRateLimit());
    }

    public void testConcurrentCallsGrantExactlyOneWindowOfPermits() {
        var listener = listenerWithClock(new AtomicLong(randomWindowIndex() * WINDOW_NANOS));
        var granted = new AtomicInteger();
        int callsPerThread = randomIntBetween(10, 200);

        startInParallel(randomIntBetween(2, 8), thread -> {
            for (int i = 0; i < callsPerThread; i++) {
                if (listener.withinRateLimit()) {
                    granted.incrementAndGet();
                }
            }
        });

        assertThat(granted.get(), equalTo(MAX_WARNINGS_PER_WINDOW));
    }

    private static long randomWindowIndex() {
        return randomLongBetween(-1000, 1000);
    }

    private static Engine.IndexResult failedResult(Exception failure) {
        Engine.IndexResult indexResult = mock(Engine.IndexResult.class);
        when(indexResult.getResultType()).thenReturn(Engine.Result.Type.FAILURE);
        when(indexResult.getFailure()).thenReturn(failure);
        return indexResult;
    }

    /**
     * The shard is mocked because a real {@link IndexShard} needs a full node environment, and the listener only reads
     * its routing entry, primary term and index settings.
     */
    private static IndexingFailuresDiagnosticListener listenerWithClock(AtomicLong clock) {
        var shardId = ShardId.fromString("[index][123]");
        var mockShard = mock(IndexShard.class);
        when(mockShard.routingEntry()).thenReturn(TestShardRouting.newShardRouting(shardId, "node-id", true, ShardRoutingState.STARTED));
        when(mockShard.getOperationPrimaryTerm()).thenReturn(1L);
        when(mockShard.indexSettings()).thenReturn(INDEX_SETTINGS);
        return new IndexingFailuresDiagnosticListener(mockShard, clock::get);
    }

    private static Exception randomUserError() {
        var shardId = ShardId.fromString("[index][123]");
        return randomFrom(
            new DocumentMissingException(shardId, "doc-0"),
            new VersionConflictEngineException(shardId, "[doc-0]", "explanation"),
            new IllegalArgumentException("Number of documents in the shard cannot exceed [1]; for more information, see [url]")
        );
    }

    private static LogEvent postIndexBatchFailure(Level level, Exception failure) {
        var previousLevel = testLogger1.getLevel();
        try {
            Loggers.setLevel(testLogger1, level);
            var shardId = ShardId.fromString("[index][123]");
            var mockShard = mock(IndexShard.class);
            var shardRouting = TestShardRouting.newShardRouting(shardId, "node-id", true, ShardRoutingState.STARTED);
            when(mockShard.routingEntry()).thenReturn(shardRouting);
            when(mockShard.getOperationPrimaryTerm()).thenReturn(1L);
            when(mockShard.indexSettings()).thenReturn(INDEX_SETTINGS);
            Engine.IndexResult indexResult = mock(Engine.IndexResult.class);
            when(indexResult.getResultType()).thenReturn(Engine.Result.Type.FAILURE);
            when(indexResult.getFailure()).thenReturn(failure);
            appender.getLastEventAndReset();
            new IndexingFailuresDiagnosticListener(mockShard).postIndexBatch(shardId, primaryBatch(1), List.of(indexResult));
            return appender.getLastEventAndReset();
        } finally {
            Loggers.setLevel(testLogger1, previousLevel);
        }
    }

    private static IndexOperationBatch primaryBatch(int docCount) {
        final BulkItemRequest[] items = new BulkItemRequest[docCount];
        for (int d = 0; d < docCount; d++) {
            items[d] = new BulkItemRequest(
                d,
                new IndexRequest("index").id("doc-" + d).source(new BytesArray("{\"n\":" + d + "}"), XContentType.JSON)
            );
        }
        return IndexOperationBatch.initFromBulk(
            items,
            0,
            docCount,
            null,
            IndexMode.STANDARD,
            Engine.Operation.Origin.PRIMARY,
            1L,
            0L,
            null
        );
    }
}
