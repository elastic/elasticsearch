/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.shard;

import org.apache.lucene.index.Term;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;

public class IndexingOperationListenerTests extends ESTestCase {

    // this test also tests if calls are correct if one or more listeners throw exceptions
    public void testListenersAreExecuted() {
        AtomicInteger preIndex = new AtomicInteger();
        AtomicInteger postIndex = new AtomicInteger();
        AtomicInteger postIndexException = new AtomicInteger();
        AtomicInteger preDelete = new AtomicInteger();
        AtomicInteger postDelete = new AtomicInteger();
        AtomicInteger postDeleteException = new AtomicInteger();
        ShardId randomShardId = new ShardId(new Index(randomAlphaOfLength(10), randomAlphaOfLength(10)), randomIntBetween(1, 10));
        IndexingOperationListener listener = new IndexingOperationListener() {
            @Override
            public Engine.Index preIndex(ShardId shardId, Engine.Index operation) {
                assertThat(shardId, is(randomShardId));
                preIndex.incrementAndGet();
                return operation;
            }

            @Override
            public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
                assertThat(shardId, is(randomShardId));
                switch (result.getResultType()) {
                    case SUCCESS -> postIndex.incrementAndGet();
                    case FAILURE -> postIndex(shardId, index, result.getFailure());
                    default -> throw new IllegalArgumentException("unknown result type: " + result.getResultType());
                }
            }

            @Override
            public void postIndex(ShardId shardId, Engine.Index index, Exception ex) {
                assertThat(shardId, is(randomShardId));
                postIndexException.incrementAndGet();
            }

            @Override
            public Engine.Delete preDelete(ShardId shardId, Engine.Delete delete) {
                assertThat(shardId, is(randomShardId));
                preDelete.incrementAndGet();
                return delete;
            }

            @Override
            public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
                assertThat(shardId, is(randomShardId));
                switch (result.getResultType()) {
                    case SUCCESS -> postDelete.incrementAndGet();
                    case FAILURE -> postDelete(shardId, delete, result.getFailure());
                    default -> throw new IllegalArgumentException("unknown result type: " + result.getResultType());
                }
            }

            @Override
            public void postDelete(ShardId shardId, Engine.Delete delete, Exception ex) {
                assertThat(shardId, is(randomShardId));
                postDeleteException.incrementAndGet();
            }
        };

        IndexingOperationListener throwingListener = new IndexingOperationListener() {
            @Override
            public Engine.Index preIndex(ShardId shardId, Engine.Index operation) {
                throw new RuntimeException();
            }

            @Override
            public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
                throw new RuntimeException();
            }

            @Override
            public void postIndex(ShardId shardId, Engine.Index index, Exception ex) {
                throw new RuntimeException();
            }

            @Override
            public Engine.Delete preDelete(ShardId shardId, Engine.Delete delete) {
                throw new RuntimeException();
            }

            @Override
            public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
                throw new RuntimeException();
            }

            @Override
            public void postDelete(ShardId shardId, Engine.Delete delete, Exception ex) {
                throw new RuntimeException();
            }
        };
        final List<IndexingOperationListener> indexingOperationListeners = new ArrayList<>(Arrays.asList(listener, listener));
        if (randomBoolean()) {
            indexingOperationListeners.add(throwingListener);
            if (randomBoolean()) {
                indexingOperationListeners.add(throwingListener);
            }
        }
        Collections.shuffle(indexingOperationListeners, random());
        IndexingOperationListener.CompositeListener compositeListener = new IndexingOperationListener.CompositeListener(
            indexingOperationListeners,
            logger
        );
        ParsedDocument doc = EngineTestCase.createParsedDoc("1", null);
        Engine.Delete delete = new Engine.Delete("1", new Term("_id", Uid.encodeId(doc.id())), randomNonNegativeLong());
        Engine.Index index = new Engine.Index(new Term("_id", Uid.encodeId(doc.id())), randomNonNegativeLong(), doc);
        compositeListener.postDelete(
            randomShardId,
            delete,
            new Engine.DeleteResult(1, 0, SequenceNumbers.UNASSIGNED_SEQ_NO, true, delete.id())
        );
        assertEquals(0, preIndex.get());
        assertEquals(0, postIndex.get());
        assertEquals(0, postIndexException.get());
        assertEquals(0, preDelete.get());
        assertEquals(2, postDelete.get());
        assertEquals(0, postDeleteException.get());

        compositeListener.postDelete(randomShardId, delete, new RuntimeException());
        assertEquals(0, preIndex.get());
        assertEquals(0, postIndex.get());
        assertEquals(0, postIndexException.get());
        assertEquals(0, preDelete.get());
        assertEquals(2, postDelete.get());
        assertEquals(2, postDeleteException.get());

        compositeListener.preDelete(randomShardId, delete);
        assertEquals(0, preIndex.get());
        assertEquals(0, postIndex.get());
        assertEquals(0, postIndexException.get());
        assertEquals(2, preDelete.get());
        assertEquals(2, postDelete.get());
        assertEquals(2, postDeleteException.get());

        compositeListener.postIndex(
            randomShardId,
            index,
            new Engine.IndexResult(0, 0, SequenceNumbers.UNASSIGNED_SEQ_NO, false, index.id())
        );
        assertEquals(0, preIndex.get());
        assertEquals(2, postIndex.get());
        assertEquals(0, postIndexException.get());
        assertEquals(2, preDelete.get());
        assertEquals(2, postDelete.get());
        assertEquals(2, postDeleteException.get());

        compositeListener.postIndex(randomShardId, index, new RuntimeException());
        assertEquals(0, preIndex.get());
        assertEquals(2, postIndex.get());
        assertEquals(2, postIndexException.get());
        assertEquals(2, preDelete.get());
        assertEquals(2, postDelete.get());
        assertEquals(2, postDeleteException.get());

        compositeListener.preIndex(randomShardId, index);
        assertEquals(2, preIndex.get());
        assertEquals(2, postIndex.get());
        assertEquals(2, postIndexException.get());
        assertEquals(2, preDelete.get());
        assertEquals(2, postDelete.get());
        assertEquals(2, postDeleteException.get());
    }
}
