/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteTransportException;

public class TransportActionsTests extends ESTestCase {

    private static final ShardId SHARD_ID = new ShardId("index", "_na_", 0);

    public void testShardNotAvailableExceptionsAreRetriable() {
        assertTrue(TransportActions.isRetriableShardLevelException(new ShardNotFoundException(SHARD_ID)));
        assertTrue(TransportActions.isRetriableShardLevelException(new IndexNotFoundException("index")));
        assertTrue(TransportActions.isRetriableShardLevelException(new IllegalIndexShardStateException(SHARD_ID, null, "closed")));
        assertTrue(TransportActions.isRetriableShardLevelException(new NoShardAvailableActionException(SHARD_ID)));
        assertTrue(TransportActions.isRetriableShardLevelException(new UnavailableShardsException(SHARD_ID, "unavailable")));
        assertTrue(TransportActions.isRetriableShardLevelException(new AlreadyClosedException("already closed")));
    }

    public void testBadRequestExceptionsAreNotRetriable() {
        assertFalse(
            TransportActions.isRetriableShardLevelException(new QueryShardException(SHARD_ID.getIndex(), "unsupported query", null))
        );
        assertFalse(TransportActions.isRetriableShardLevelException(new ParsingException(0, 0, "parse error", null)));
        assertFalse(TransportActions.isRetriableShardLevelException(new AggregationExecutionException.InvalidPath("bad agg path")));
    }

    public void testWrappedBadRequestIsNotRetriable() {
        // RemoteTransportException wraps the underlying cause; status() unwraps transparently
        Exception wrapped = new RemoteTransportException("remote", new QueryShardException(SHARD_ID.getIndex(), "unsupported query", null));
        assertFalse(TransportActions.isRetriableShardLevelException(wrapped));
    }

    public void testGenericExceptionsAreRetriable() {
        assertTrue(TransportActions.isRetriableShardLevelException(new RuntimeException("unexpected")));
        assertTrue(TransportActions.isRetriableShardLevelException(new ElasticsearchException("internal error")));
    }
}
