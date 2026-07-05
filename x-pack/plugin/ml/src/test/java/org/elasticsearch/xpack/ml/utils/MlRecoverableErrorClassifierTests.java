/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.internal.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexPrimaryShardNotAllocatedException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;
import org.elasticsearch.transport.RemoteTransportException;

import java.util.EnumSet;
import java.util.Set;

public class MlRecoverableErrorClassifierTests extends ESTestCase {

    // -------------------------------------------------------------------------
    // Layer 1: Data Plane (always recoverable)
    // -------------------------------------------------------------------------

    public void testSearchPhaseExecutionException_isRecoverable() {
        var e = new SearchPhaseExecutionException("query", "partial results", ShardSearchFailure.EMPTY_ARRAY);
        assertTrue(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    public void testElasticsearchStatusException_serviceUnavailable_isRecoverable() {
        var e = new ElasticsearchStatusException("service unavailable", RestStatus.SERVICE_UNAVAILABLE);
        assertTrue(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    public void testIndexPrimaryShardNotAllocatedException_isRecoverable() {
        var e = new IndexPrimaryShardNotAllocatedException(new Index("my-index", "uuid"));
        assertTrue(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    public void testNoShardAvailableActionException_isRecoverable() {
        var e = new NoShardAvailableActionException(new ShardId("my-index", "uuid", 0));
        assertTrue(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    public void testIllegalIndexShardStateException_isRecoverable() {
        var e = new IllegalIndexShardStateException(new ShardId("my-index", "uuid", 0), IndexShardState.RECOVERING, "shard not ready");
        assertTrue(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    // -------------------------------------------------------------------------
    // Layer 2: Control Plane (conditional)
    // -------------------------------------------------------------------------

    public void testMasterNotDiscoveredException_isRecoverable() {
        var e = new MasterNotDiscoveredException();
        assertTrue(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    public void testNotMasterException_isRecoverable() {
        var e = new NotMasterException("not master");
        assertTrue(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    public void testClusterBlockException_retryableTrue_isRecoverable() {
        var block = new ClusterBlock(
            1,
            "test block",
            true,
            false,
            false,
            RestStatus.SERVICE_UNAVAILABLE,
            EnumSet.of(ClusterBlockLevel.READ)
        );
        var e = new ClusterBlockException(Set.of(block));
        assertTrue(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    public void testClusterBlockException_retryableFalse_isNotRecoverable() {
        var block = new ClusterBlock(2, "permanent block", false, false, false, RestStatus.FORBIDDEN, EnumSet.of(ClusterBlockLevel.WRITE));
        var e = new ClusterBlockException(Set.of(block));
        assertFalse(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    public void testTaskCancelledException_isNotRecoverable() {
        var e = new TaskCancelledException("task was cancelled");
        assertFalse(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    // -------------------------------------------------------------------------
    // Layer 3: Resource Plane (conditional)
    // -------------------------------------------------------------------------

    public void testCircuitBreakingException_transient_isRecoverable() {
        var e = new CircuitBreakingException("transient circuit break", CircuitBreaker.Durability.TRANSIENT);
        assertTrue(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    public void testCircuitBreakingException_permanent_isNotRecoverable() {
        var e = new CircuitBreakingException("permanent circuit break", CircuitBreaker.Durability.PERMANENT);
        assertFalse(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    public void testEsRejectedExecutionException_poolFull_isRecoverable() {
        var e = new EsRejectedExecutionException("pool full", false);
        assertTrue(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    public void testEsRejectedExecutionException_shutdown_isNotRecoverable() {
        var e = new EsRejectedExecutionException("executor shutdown", true);
        assertFalse(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    public void testVersionConflictEngineException_isRecoverable() {
        var shardId = new ShardId("index", "uuid", 0);
        var e = new VersionConflictEngineException(shardId, "doc1", "version conflict");
        assertTrue(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    public void testElasticsearchStatusException_tooManyRequests_isRecoverable() {
        var e = new ElasticsearchStatusException("too many requests", RestStatus.TOO_MANY_REQUESTS);
        assertTrue(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    // -------------------------------------------------------------------------
    // Layer 4: Transport Plane (always recoverable)
    // -------------------------------------------------------------------------

    public void testConnectTransportException_isRecoverable() {
        var node = DiscoveryNodeUtils.create("node1");
        var e = new ConnectTransportException(node, "connection failed");
        assertTrue(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    public void testNodeNotConnectedException_isRecoverable() {
        var node = DiscoveryNodeUtils.create("node1");
        var e = new NodeNotConnectedException(node, "node not connected");
        assertTrue(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    public void testReceiveTimeoutTransportException_isRecoverable() {
        var node = DiscoveryNodeUtils.create("node1");
        var e = new ReceiveTimeoutTransportException(node, "action", "timeout");
        assertTrue(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    public void testNodeClosedException_isRecoverable() {
        var e = new NodeClosedException((org.elasticsearch.cluster.node.DiscoveryNode) null);
        assertTrue(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    public void testNoNodeAvailableException_isRecoverable() {
        var e = new NoNodeAvailableException("no node available");
        assertTrue(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    public void testElasticsearchTimeoutException_isRecoverable() {
        var e = new ElasticsearchTimeoutException("timed out");
        assertTrue(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    // -------------------------------------------------------------------------
    // Irrecoverable: status-code-based
    // -------------------------------------------------------------------------

    public void testElasticsearchStatusException_badRequest_isNotRecoverable() {
        var e = new ElasticsearchStatusException("bad request", RestStatus.BAD_REQUEST);
        assertFalse(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    public void testElasticsearchStatusException_notFound_isNotRecoverable() {
        var e = new ElasticsearchStatusException("not found", RestStatus.NOT_FOUND);
        assertFalse(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    public void testElasticsearchStatusException_gone_isNotRecoverable() {
        var e = new ElasticsearchStatusException("gone", RestStatus.GONE);
        assertFalse(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    public void testElasticsearchStatusException_unauthorized_isNotRecoverable() {
        var e = new ElasticsearchStatusException("unauthorized", RestStatus.UNAUTHORIZED);
        assertFalse(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    public void testElasticsearchStatusException_forbidden_isNotRecoverable() {
        var e = new ElasticsearchStatusException("forbidden", RestStatus.FORBIDDEN);
        assertFalse(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    public void testResourceNotFoundException_isNotRecoverable() {
        var e = new ResourceNotFoundException("resource not found");
        assertFalse(MlRecoverableErrorClassifier.isRecoverable(e));
    }

    public void testIllegalArgumentException_isNotRecoverable() {
        assertFalse(MlRecoverableErrorClassifier.isRecoverable(new IllegalArgumentException("bad arg")));
    }

    public void testIllegalStateException_isNotRecoverable() {
        assertFalse(MlRecoverableErrorClassifier.isRecoverable(new IllegalStateException("bad state")));
    }

    // -------------------------------------------------------------------------
    // Edge cases
    // -------------------------------------------------------------------------

    public void testUnknownException_isNotRecoverable() {
        assertFalse(MlRecoverableErrorClassifier.isRecoverable(new RuntimeException("something weird")));
    }

    public void testNullPointerException_isNotRecoverable() {
        assertFalse(MlRecoverableErrorClassifier.isRecoverable(new NullPointerException("npe")));
    }

    public void testWrappedRecoverableException_isRecoverable() {
        var inner = new SearchPhaseExecutionException("query", "partial results", ShardSearchFailure.EMPTY_ARRAY);
        var wrapped = new RemoteTransportException("remote", inner);
        assertTrue(MlRecoverableErrorClassifier.isRecoverable(wrapped));
    }

    public void testWrappedIrrecoverableException_isNotRecoverable() {
        var inner = new IllegalArgumentException("bad arg");
        var wrapped = new RemoteTransportException("remote", inner);
        assertFalse(MlRecoverableErrorClassifier.isRecoverable(wrapped));
    }

    public void testWrappedTransientCircuitBreaker_isRecoverable() {
        var inner = new CircuitBreakingException("transient", CircuitBreaker.Durability.TRANSIENT);
        var wrapped = new RemoteTransportException("remote", inner);
        assertTrue(MlRecoverableErrorClassifier.isRecoverable(wrapped));
    }

    public void testWrappedPermanentCircuitBreaker_isNotRecoverable() {
        var inner = new CircuitBreakingException("permanent", CircuitBreaker.Durability.PERMANENT);
        var wrapped = new RemoteTransportException("remote", inner);
        assertFalse(MlRecoverableErrorClassifier.isRecoverable(wrapped));
    }
}
