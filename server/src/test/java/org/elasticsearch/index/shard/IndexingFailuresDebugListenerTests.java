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
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.logging.MockAppender;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexingFailuresDebugListenerTests extends ESTestCase {

    static MockAppender appender;
    static Logger testLogger1 = LogManager.getLogger(IndexingFailuresDebugListener.class);
    static Level origLogLevel = testLogger1.getLevel();

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
                    + "] primaryTerm [1] operationPrimaryTerm [1] origin [PRIMARY]"
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
                    + "] primaryTerm [1] operationPrimaryTerm [1] origin [PRIMARY]"
            )
        );
    }

    public void testPostIndex() {
        var shardId = ShardId.fromString("[index][123]");
        var mockShard = mock(IndexShard.class);
        var shardRouting = TestShardRouting.newShardRouting(shardId, "node-id", true, ShardRoutingState.STARTED);
        when(mockShard.routingEntry()).thenReturn(shardRouting);
        when(mockShard.getOperationPrimaryTerm()).thenReturn(1L);
        IndexingFailuresDebugListener indexingFailuresDebugListener = new IndexingFailuresDebugListener(mockShard);

        ParsedDocument doc = EngineTestCase.createParsedDoc("1", null);
        Engine.Index index = new Engine.Index(Uid.encodeId("doc_id"), 1, doc);
        Engine.IndexResult indexResult = mock(Engine.IndexResult.class);
        when(indexResult.getResultType()).thenReturn(Engine.Result.Type.SUCCESS);
        when(indexResult.getFailure()).thenReturn(new RuntimeException("test exception"));
        indexingFailuresDebugListener.postIndex(shardId, index, indexResult);
        assertThat(appender.getLastEventAndReset(), nullValue());
    }

}
