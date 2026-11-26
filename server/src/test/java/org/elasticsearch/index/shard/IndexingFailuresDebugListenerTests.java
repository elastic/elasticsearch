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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexingFailuresDebugListenerTests extends ESTestCase {

    static MockAppender appender;
    static Logger testLogger1 = LogManager.getLogger(IndexingFailuresDebugListener.class);
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
        IndexingFailuresDebugListener indexingFailuresDebugListener = new IndexingFailuresDebugListener(mockShard);

        ParsedDocument doc = EngineTestCase.createParsedDoc("1", null);
        Engine.Index index = new Engine.Index(Uid.encodeId("doc_id"), 1, doc);
        indexingFailuresDebugListener.postIndex(shardId, index, new RuntimeException("test exception"));
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
            IndexingFailuresDebugListener indexingFailuresDebugListener = new IndexingFailuresDebugListener(mockShard);

            ParsedDocument doc = EngineTestCase.createParsedDoc("1", null);
            Engine.Index index = new Engine.Index(Uid.encodeId("doc_id"), 1, doc);
            indexingFailuresDebugListener.postIndex(shardId, index, new RuntimeException("test exception"));
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
        IndexingFailuresDebugListener indexingFailuresDebugListener = new IndexingFailuresDebugListener(mockShard);

        ParsedDocument doc = EngineTestCase.createParsedDoc("1", null);
        Engine.Index index = new Engine.Index(Uid.encodeId("doc_id"), 1, doc);
        Engine.IndexResult indexResult = mock(Engine.IndexResult.class);
        when(indexResult.getResultType()).thenReturn(Engine.Result.Type.FAILURE);
        when(indexResult.getFailure()).thenReturn(new RuntimeException("test exception"));
        indexingFailuresDebugListener.postIndex(shardId, index, indexResult);
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
        IndexingFailuresDebugListener indexingFailuresDebugListener = new IndexingFailuresDebugListener(mockShard);

        ParsedDocument doc = EngineTestCase.createParsedDoc("1", null);
        Engine.Index index = new Engine.Index(Uid.encodeId("doc_id"), 1, doc);
        Engine.IndexResult indexResult = mock(Engine.IndexResult.class);
        when(indexResult.getResultType()).thenReturn(Engine.Result.Type.SUCCESS);
        when(indexResult.getFailure()).thenReturn(new RuntimeException("test exception"));
        indexingFailuresDebugListener.postIndex(shardId, index, indexResult);
        assertThat(appender.getLastEventAndReset(), nullValue());
    }

    public void testPostIndexBatchFailure() {
        var shardId = ShardId.fromString("[index][123]");
        var mockShard = mock(IndexShard.class);
        var shardRouting = TestShardRouting.newShardRouting(shardId, "node-id", true, ShardRoutingState.STARTED);
        when(mockShard.routingEntry()).thenReturn(shardRouting);
        when(mockShard.getOperationPrimaryTerm()).thenReturn(1L);
        when(mockShard.indexSettings()).thenReturn(INDEX_SETTINGS);
        IndexingFailuresDebugListener listener = new IndexingFailuresDebugListener(mockShard);

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
        IndexingFailuresDebugListener listener = new IndexingFailuresDebugListener(mockShard);

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
        IndexingFailuresDebugListener listener = new IndexingFailuresDebugListener(mockShard);

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

    public void testPostIndexBatchRealErrorLoggedAtDebug() {
        Exception realError = randomFrom(
            new RuntimeException("test exception"),
            new IllegalArgumentException("other message"),
            new IllegalArgumentException()
        );
        LogEvent event = postIndexBatchFailure(Level.TRACE, realError);
        assertThat(event.getLevel(), equalTo(Level.DEBUG));
        assertThat(event.getMessage().getFormattedMessage(), startsWith("real-index-fail [doc-0]"));
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
            new IndexingFailuresDebugListener(mockShard).postIndexBatch(shardId, primaryBatch(1), List.of(indexResult));
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
        return IndexOperationBatch.initFromBulk(items, 0, docCount, null, Engine.Operation.Origin.PRIMARY, 1L, 0L, null);
    }
}
