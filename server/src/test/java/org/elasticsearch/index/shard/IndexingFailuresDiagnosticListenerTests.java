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
import org.elasticsearch.cluster.routing.ShardRouting;
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
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexingFailuresDiagnosticListenerTests extends ESTestCase {

    private static final int MAX_WARNINGS_PER_WINDOW = 5;
    private static final TimeValue WARN_WINDOW = TimeValue.timeValueHours(1);
    private static final ShardId SHARD_ID = ShardId.fromString("[index][123]");
    private static final Logger LOGGER = LogManager.getLogger(IndexingFailuresDiagnosticListener.class);
    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings(
        "index",
        Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.getName()).build()
    );

    /** The three ways a single failed document reaches the listener. */
    private enum FailurePath {
        INDEX_RESULT,
        INDEX_EXCEPTION,
        BATCH_RESULT
    }

    private final ShardRouting shardRouting = TestShardRouting.newShardRouting(SHARD_ID, "node-id", true, ShardRoutingState.STARTED);
    private final AtomicLong clock = new AtomicLong(
        randomLongBetween(-1000, 1000) * WARN_WINDOW.nanos() + randomLongBetween(0, WARN_WINDOW.nanos() - 1)
    );
    private final WindowedRateLimiter sharedLimiter = newLimiter();
    private EventRecordingAppender appender;
    private Level originalLevel;

    @Before
    public void addAppender() throws IllegalAccessException {
        originalLevel = LOGGER.getLevel();
        appender = new EventRecordingAppender();
        appender.start();
        Loggers.addAppender(LOGGER, appender);
    }

    @After
    public void removeAppender() {
        Loggers.removeAppender(LOGGER, appender);
        appender.stop();
        Loggers.setLevel(LOGGER, originalLevel);
    }

    public void testLevelAndMessageForEveryPathErrorTypeAndLogLevel() {
        for (FailurePath path : FailurePath.values()) {
            for (Level level : List.of(Level.TRACE, Level.DEBUG, Level.INFO, Level.WARN)) {
                for (boolean userError : new boolean[] { true, false }) {
                    Loggers.setLevel(LOGGER, level);
                    appender.events.clear();
                    logFailure(listener(newLimiter()), path, userError ? randomUserError() : randomRealError());
                    String docId = path == FailurePath.BATCH_RESULT ? "doc-0" : "1";
                    String context = path + " " + level + " userError=" + userError;
                    if (userError == false) {
                        assertSingleEvent(context, Level.WARN, expectedMessage("real-index-fail [" + docId + "] seq# [-2]"));
                    } else if (level == Level.TRACE) {
                        assertSingleEvent(context, Level.TRACE, expectedMessage("index-fail [" + docId + "] seq# [-2]"));
                    } else {
                        assertThat(context, appender.events, empty());
                    }
                }
            }
        }
    }

    public void testSuccessIsNotLogged() {
        Loggers.setLevel(LOGGER, Level.TRACE);
        var listener = listener(sharedLimiter);
        listener.postIndex(SHARD_ID, singleDocIndex(), successResult());
        listener.postIndexBatch(SHARD_ID, primaryBatch(1), List.of(successResult()));
        assertThat(appender.events, empty());
    }

    public void testBatchEngineExceptionLoggedAsOneLine() {
        Loggers.setLevel(LOGGER, randomFrom(Level.TRACE, Level.DEBUG));
        int docCount = randomIntBetween(1, 8);
        listener(sharedLimiter).postIndexBatch(SHARD_ID, primaryBatch(docCount), new RuntimeException("test exception"));
        assertSingleEvent(
            "engine exception",
            Level.DEBUG,
            expectedMessage("index-fail batch docCount [" + docCount + "] startingSeqNo [-2]")
        );
    }

    /**
     * Real failures from all paths and from many listeners share one limit: WARN up to the limit, then DEBUG if enabled,
     * with a fresh limit in each window. Successes in between must not use permits.
     */
    public void testWarnLimitIsSharedAndResetsEachWindow() {
        boolean debugEnabled = randomBoolean();
        Loggers.setLevel(LOGGER, debugEnabled ? randomFrom(Level.TRACE, Level.DEBUG) : randomFrom(Level.INFO, Level.WARN));
        FailurePath[] paths = FailurePath.values();
        int windows = randomIntBetween(1, 4);
        for (int w = 0; w < windows; w++) {
            appender.events.clear();
            List<Level> expected = new ArrayList<>();
            int failures = MAX_WARNINGS_PER_WINDOW + randomIntBetween(1, 10);
            for (int i = 0; i < failures; i++) {
                var listener = listener(sharedLimiter);
                if (randomBoolean()) {
                    listener.postIndex(SHARD_ID, singleDocIndex(), successResult());
                } else {
                    listener.postIndexBatch(SHARD_ID, primaryBatch(1), List.of(successResult()));
                }
                logFailure(listener, paths[i % paths.length], randomRealError());
                if (i < MAX_WARNINGS_PER_WINDOW) {
                    expected.add(Level.WARN);
                } else if (debugEnabled) {
                    expected.add(Level.DEBUG);
                }
            }
            assertThat("window " + w, levels(), equalTo(expected));
            clock.addAndGet(WARN_WINDOW.nanos() * randomLongBetween(1, 100));
        }
    }

    public void testBatchTakesOnePermitPerRealFailureAndUserErrorsTakeNone() {
        Loggers.setLevel(LOGGER, Level.DEBUG);
        FailurePath[] paths = FailurePath.values();
        int userErrors = randomIntBetween(MAX_WARNINGS_PER_WINDOW + 1, 20);
        for (int i = 0; i < userErrors; i++) {
            logFailure(listener(sharedLimiter), paths[i % paths.length], randomUserError());
        }
        int realErrors = randomIntBetween(MAX_WARNINGS_PER_WINDOW + 1, 50);
        List<Engine.IndexResult> results = new ArrayList<>();
        for (int i = 0; i < realErrors; i++) {
            results.add(failedResult(randomRealError()));
        }

        listener(sharedLimiter).postIndexBatch(SHARD_ID, primaryBatch(realErrors), results);

        List<Level> expected = new ArrayList<>(Collections.nCopies(MAX_WARNINGS_PER_WINDOW, Level.WARN));
        expected.addAll(Collections.nCopies(realErrors - MAX_WARNINGS_PER_WINDOW, Level.DEBUG));
        assertThat(levels(), equalTo(expected));
    }

    private void logFailure(IndexingFailuresDiagnosticListener listener, FailurePath path, Exception failure) {
        switch (path) {
            case INDEX_RESULT -> listener.postIndex(SHARD_ID, singleDocIndex(), failedResult(failure));
            case INDEX_EXCEPTION -> listener.postIndex(SHARD_ID, singleDocIndex(), failure);
            case BATCH_RESULT -> listener.postIndexBatch(SHARD_ID, primaryBatch(1), List.of(failedResult(failure)));
        }
    }

    /**
     * The shard is mocked because a real {@link IndexShard} needs a full node environment, and the listener only reads
     * its routing entry, primary term and index settings.
     */
    private IndexingFailuresDiagnosticListener listener(WindowedRateLimiter limiter) {
        var shard = mock(IndexShard.class);
        when(shard.routingEntry()).thenReturn(shardRouting);
        when(shard.getOperationPrimaryTerm()).thenReturn(1L);
        when(shard.indexSettings()).thenReturn(INDEX_SETTINGS);
        return new IndexingFailuresDiagnosticListener(shard, limiter);
    }

    private WindowedRateLimiter newLimiter() {
        return new WindowedRateLimiter(MAX_WARNINGS_PER_WINDOW, WARN_WINDOW, clock::get);
    }

    private String expectedMessage(String operationPart) {
        return operationPart
            + " allocation-id ["
            + shardRouting.allocationId()
            + "] primaryTerm [1] operationPrimaryTerm [1] origin [PRIMARY] index [index] index-mode [logsdb]";
    }

    private void assertSingleEvent(String context, Level level, String message) {
        assertThat(context, appender.events, hasSize(1));
        assertThat(context, appender.events.get(0).getLevel(), equalTo(level));
        assertThat(context, appender.events.get(0).getMessage().getFormattedMessage(), equalTo(message));
    }

    private List<Level> levels() {
        return appender.events.stream().map(LogEvent::getLevel).toList();
    }

    private static Engine.IndexResult failedResult(Exception failure) {
        Engine.IndexResult result = mock(Engine.IndexResult.class);
        when(result.getResultType()).thenReturn(Engine.Result.Type.FAILURE);
        when(result.getFailure()).thenReturn(failure);
        return result;
    }

    /** The failure is set so that the test fails if the listener reads it without checking the result type. */
    private static Engine.IndexResult successResult() {
        Engine.IndexResult result = mock(Engine.IndexResult.class);
        when(result.getResultType()).thenReturn(Engine.Result.Type.SUCCESS);
        when(result.getFailure()).thenReturn(new RuntimeException("test exception"));
        return result;
    }

    private static Exception randomUserError() {
        return randomFrom(
            new DocumentMissingException(SHARD_ID, "doc-0"),
            new VersionConflictEngineException(SHARD_ID, "[doc-0]", "explanation"),
            new IllegalArgumentException("Number of documents in the shard cannot exceed [1]; for more information, see [url]")
        );
    }

    private static Exception randomRealError() {
        return randomFrom(
            new RuntimeException("test exception"),
            new IllegalArgumentException("other message"),
            new IllegalArgumentException()
        );
    }

    private static Engine.Index singleDocIndex() {
        return new Engine.Index(Uid.encodeId("doc_id"), 1, EngineTestCase.createParsedDoc("1", null));
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

    /** {@link MockAppender} keeps only the last event, but these tests check every event that one call logs. */
    private static final class EventRecordingAppender extends MockAppender {
        private final List<LogEvent> events = new ArrayList<>();

        EventRecordingAppender() throws IllegalAccessException {
            super("event_recording_appender");
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.toImmutable());
        }
    }
}
