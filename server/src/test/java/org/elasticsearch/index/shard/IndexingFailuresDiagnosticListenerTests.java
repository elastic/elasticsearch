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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.engine.IndexOperationBatch;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.xcontent.XContentType;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexingFailuresDiagnosticListenerTests extends ESTestCase {

    private static final TimeValue WARN_WINDOW = TimeValue.timeValueHours(1);
    private static final long WINDOW_NANOS = TimeValue.timeValueHours(1).nanos();
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
    private static IndexingFailuresDiagnosticListener listenerWithLimiter(WindowedRateLimiter limiter) {
        var shardId = ShardId.fromString("[index][123]");
        var mockShard = mock(IndexShard.class);
        when(mockShard.routingEntry()).thenReturn(TestShardRouting.newShardRouting(shardId, "node-id", true, ShardRoutingState.STARTED));
        when(mockShard.getOperationPrimaryTerm()).thenReturn(1L);
        when(mockShard.indexSettings()).thenReturn(INDEX_SETTINGS);
        return new IndexingFailuresDiagnosticListener(mockShard, limiter);
    }

    private static IndexingFailuresDiagnosticListener listenerWithClock(AtomicLong clock) {
        return listenerWithLimiter(limiterWithClock(clock));
    }

    private static WindowedRateLimiter limiterWithClock(AtomicLong clock) {
        return new WindowedRateLimiter(MAX_WARNINGS_PER_WINDOW, WARN_WINDOW, clock::get);
    }

    private static WindowedRateLimiter newLimiter() {
        return new WindowedRateLimiter(MAX_WARNINGS_PER_WINDOW, WARN_WINDOW, System::nanoTime);
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
            new IndexingFailuresDiagnosticListener(mockShard, newLimiter()).postIndexBatch(shardId, primaryBatch(1), List.of(indexResult));
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

    private static Engine.Index singleDocIndex() {
        return new Engine.Index(Uid.encodeId("doc_id"), 1, EngineTestCase.createParsedDoc("1", null));
    }

    private static LogEvent postIndexFailure(Level level, Exception failure, boolean asResult) {
        var previousLevel = testLogger1.getLevel();
        try {
            Loggers.setLevel(testLogger1, level);
            appender.getLastEventAndReset();
            var listener = listenerWithLimiter(newLimiter());
            var shardId = ShardId.fromString("[index][123]");
            if (asResult) {
                listener.postIndex(shardId, singleDocIndex(), failedResult(failure));
            } else {
                listener.postIndex(shardId, singleDocIndex(), failure);
            }
            return appender.getLastEventAndReset();
        } finally {
            Loggers.setLevel(testLogger1, previousLevel);
        }
    }

    public void testPostIndexException() {
        var shardId = ShardId.fromString("[index][123]");
        var mockShard = mock(IndexShard.class);
        var shardRouting = TestShardRouting.newShardRouting(shardId, "node-id", true, ShardRoutingState.STARTED);
        when(mockShard.routingEntry()).thenReturn(shardRouting);
        when(mockShard.getOperationPrimaryTerm()).thenReturn(1L);
        when(mockShard.indexSettings()).thenReturn(INDEX_SETTINGS);
        var listener = new IndexingFailuresDiagnosticListener(mockShard, newLimiter());

        listener.postIndex(shardId, singleDocIndex(), new RuntimeException("test exception"));
        String message = appender.getLastEventAndReset().getMessage().getFormattedMessage();
        assertThat(
            message,
            equalTo(
                "real-index-fail [1] seq# [-2] allocation-id ["
                    + shardRouting.allocationId()
                    + "] primaryTerm [1] operationPrimaryTerm [1] origin [PRIMARY] index [index] index-mode [logsdb]"
            )
        );
    }

    public void testPostIndexRealErrorLoggedAtWarnEvenWhenDebugDisabled() {
        Level level = randomFrom(Level.INFO, Level.WARN);
        LogEvent event = postIndexFailure(level, new RuntimeException("test exception"), randomBoolean());
        assertThat(event.getLevel(), equalTo(Level.WARN));
        assertThat(event.getMessage().getFormattedMessage(), startsWith("real-index-fail [1]"));
    }

    public void testPostIndexUserErrorLoggedAtTraceOnly() {
        Exception userError = randomUserError();
        boolean asResult = randomBoolean();
        LogEvent traceEvent = postIndexFailure(Level.TRACE, userError, asResult);
        assertThat(traceEvent.getLevel(), equalTo(Level.TRACE));
        assertThat(traceEvent.getMessage().getFormattedMessage(), startsWith("index-fail [1]"));
        assertThat(postIndexFailure(randomFrom(Level.DEBUG, Level.INFO), userError, asResult), nullValue());
    }

    public void testPostIndexFailureResult() {
        var shardId = ShardId.fromString("[index][123]");
        var mockShard = mock(IndexShard.class);
        var shardRouting = TestShardRouting.newShardRouting(shardId, "node-id", true, ShardRoutingState.STARTED);
        when(mockShard.routingEntry()).thenReturn(shardRouting);
        when(mockShard.getOperationPrimaryTerm()).thenReturn(1L);
        when(mockShard.indexSettings()).thenReturn(INDEX_SETTINGS);
        var listener = new IndexingFailuresDiagnosticListener(mockShard, newLimiter());

        listener.postIndex(shardId, singleDocIndex(), failedResult(new RuntimeException("test exception")));
        String message = appender.getLastEventAndReset().getMessage().getFormattedMessage();
        assertThat(
            message,
            equalTo(
                "real-index-fail [1] seq# [-2] allocation-id ["
                    + shardRouting.allocationId()
                    + "] primaryTerm [1] operationPrimaryTerm [1] origin [PRIMARY] index [index] index-mode [logsdb]"
            )
        );
    }

    public void testPostIndex() {
        var listener = listenerWithLimiter(newLimiter());
        Engine.IndexResult indexResult = mock(Engine.IndexResult.class);
        when(indexResult.getResultType()).thenReturn(Engine.Result.Type.SUCCESS);
        when(indexResult.getFailure()).thenReturn(new RuntimeException("test exception"));
        listener.postIndex(ShardId.fromString("[index][123]"), singleDocIndex(), indexResult);
        assertThat(appender.getLastEventAndReset(), nullValue());
    }

    public void testPerDocumentRealErrorsLoggedAtWarnUpToLimitThenDebugWithinWindow() {
        var clock = new AtomicLong(randomWindowIndex() * WINDOW_NANOS + randomLongBetween(0, WINDOW_NANOS - 1));
        var listener = listenerWithClock(clock);
        var shardId = ShardId.fromString("[index][123]");
        boolean asResult = randomBoolean();

        int failures = randomIntBetween(MAX_WARNINGS_PER_WINDOW + 1, 50);
        for (int i = 0; i < failures; i++) {
            appender.getLastEventAndReset();
            if (asResult) {
                listener.postIndex(shardId, singleDocIndex(), failedResult(new RuntimeException("test exception")));
            } else {
                listener.postIndex(shardId, singleDocIndex(), new RuntimeException("test exception"));
            }
            Level expected = i < MAX_WARNINGS_PER_WINDOW ? Level.WARN : Level.DEBUG;
            assertThat("failure " + i, appender.getLastEventAndReset().getLevel(), equalTo(expected));
        }
    }

    public void testWarnLimitIsSharedBetweenPerDocumentAndBatchFailures() throws IllegalAccessException {
        var listener = listenerWithClock(new AtomicLong(randomWindowIndex() * WINDOW_NANOS));
        var shardId = ShardId.fromString("[index][123]");
        int failures = randomIntBetween(MAX_WARNINGS_PER_WINDOW + 1, 50);

        List<Level> levels = levelsLoggedDuring(() -> {
            for (int i = 0; i < failures; i++) {
                if (randomBoolean()) {
                    listener.postIndex(shardId, singleDocIndex(), new RuntimeException("test exception"));
                } else {
                    listener.postIndexBatch(shardId, primaryBatch(1), List.of(failedResult(new RuntimeException("test exception"))));
                }
            }
        });

        assertThat(Collections.frequency(levels, Level.WARN), equalTo(MAX_WARNINGS_PER_WINDOW));
    }

    public void testPostIndexBatchFailure() {
        var shardId = ShardId.fromString("[index][123]");
        var mockShard = mock(IndexShard.class);
        var shardRouting = TestShardRouting.newShardRouting(shardId, "node-id", true, ShardRoutingState.STARTED);
        when(mockShard.routingEntry()).thenReturn(shardRouting);
        when(mockShard.getOperationPrimaryTerm()).thenReturn(1L);
        when(mockShard.indexSettings()).thenReturn(INDEX_SETTINGS);
        IndexingFailuresDiagnosticListener listener = new IndexingFailuresDiagnosticListener(mockShard, newLimiter());

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

    public void testRealErrorsLoggedAtWarnInEachWindowWhenDebugDisabled() {
        var previousLevel = testLogger1.getLevel();
        try {
            Loggers.setLevel(testLogger1, randomFrom(Level.INFO, Level.WARN));
            var clock = new AtomicLong(randomWindowIndex() * WINDOW_NANOS);
            var listener = listenerWithClock(clock);
            var shardId = ShardId.fromString("[index][123]");
            var failure = failedResult(new RuntimeException("test exception"));
            var success = mock(Engine.IndexResult.class);
            when(success.getResultType()).thenReturn(Engine.Result.Type.SUCCESS);

            int windows = randomIntBetween(2, 5);
            for (int w = 0; w < windows; w++) {
                int successfulBatches = randomIntBetween(0, 10);
                for (int i = 0; i < successfulBatches; i++) {
                    listener.postIndexBatch(shardId, primaryBatch(1), List.of(success));
                }
                int failures = MAX_WARNINGS_PER_WINDOW + randomIntBetween(1, 10);
                for (int i = 0; i < failures; i++) {
                    appender.getLastEventAndReset();
                    listener.postIndexBatch(shardId, primaryBatch(1), List.of(failure));
                    LogEvent event = appender.getLastEventAndReset();
                    if (i < MAX_WARNINGS_PER_WINDOW) {
                        assertThat("window " + w + " failure " + i, event.getLevel(), equalTo(Level.WARN));
                    } else {
                        assertThat("window " + w + " failure " + i, event, nullValue());
                    }
                }
                clock.addAndGet(WINDOW_NANOS * randomLongBetween(1, 100));
            }
        } finally {
            Loggers.setLevel(testLogger1, previousLevel);
        }
    }

    public void testLimitIsSharedAcrossListeners() {
        var limiter = limiterWithClock(new AtomicLong(randomWindowIndex() * WINDOW_NANOS));
        var shardId = ShardId.fromString("[index][123]");
        var result = failedResult(new RuntimeException("test exception"));
        int listeners = randomIntBetween(2, 8);

        int warnings = 0;
        for (int i = 0; i < listeners * MAX_WARNINGS_PER_WINDOW; i++) {
            appender.getLastEventAndReset();
            listenerWithLimiter(limiter).postIndexBatch(shardId, primaryBatch(1), List.of(result));
            if (appender.getLastEventAndReset().getLevel() == Level.WARN) {
                warnings++;
            }
        }

        assertThat(warnings, equalTo(MAX_WARNINGS_PER_WINDOW));
    }

    public void testWarnPermitIsTakenPerFailedDocumentWithinOneBatch() throws IllegalAccessException {
        var listener = listenerWithClock(new AtomicLong(randomWindowIndex() * WINDOW_NANOS));
        int failedDocs = randomIntBetween(MAX_WARNINGS_PER_WINDOW + 1, 50);
        List<Engine.IndexResult> results = new ArrayList<>();
        for (int i = 0; i < failedDocs; i++) {
            results.add(failedResult(new RuntimeException("test exception")));
        }

        List<Level> levels = levelsLoggedDuring(
            () -> listener.postIndexBatch(ShardId.fromString("[index][123]"), primaryBatch(failedDocs), results)
        );

        assertThat(Collections.frequency(levels, Level.WARN), equalTo(MAX_WARNINGS_PER_WINDOW));
        assertThat(Collections.frequency(levels, Level.DEBUG), equalTo(failedDocs - MAX_WARNINGS_PER_WINDOW));
    }

    public void testUserErrorsDoNotUseWarnPermits() throws IllegalAccessException {
        var listener = listenerWithClock(new AtomicLong(randomWindowIndex() * WINDOW_NANOS));
        int userErrors = randomIntBetween(MAX_WARNINGS_PER_WINDOW + 1, 50);
        int realErrors = MAX_WARNINGS_PER_WINDOW + randomIntBetween(0, 3);
        List<Engine.IndexResult> results = new ArrayList<>();
        for (int i = 0; i < userErrors; i++) {
            results.add(failedResult(randomUserError()));
        }
        for (int i = 0; i < realErrors; i++) {
            results.add(failedResult(new RuntimeException("test exception")));
        }

        List<Level> levels = levelsLoggedDuring(
            () -> listener.postIndexBatch(ShardId.fromString("[index][123]"), primaryBatch(results.size()), results)
        );

        assertThat(Collections.frequency(levels, Level.WARN), equalTo(MAX_WARNINGS_PER_WINDOW));
    }

    private static List<Level> levelsLoggedDuring(Runnable action) throws IllegalAccessException {
        var recorder = new LevelRecordingAppender();
        recorder.start();
        Loggers.addAppender(testLogger1, recorder);
        try {
            action.run();
        } finally {
            Loggers.removeAppender(testLogger1, recorder);
            recorder.stop();
            appender.getLastEventAndReset();
        }
        return recorder.levels;
    }

    /**
     * {@link MockAppender} keeps only the last event, so counting the events logged by one call needs an appender that
     * records every level.
     */
    private static final class LevelRecordingAppender extends MockAppender {
        private final List<Level> levels = new ArrayList<>();

        LevelRecordingAppender() throws IllegalAccessException {
            super("level_recording_appender");
        }

        @Override
        public void append(LogEvent event) {
            levels.add(event.getLevel());
            super.append(event);
        }
    }
}
