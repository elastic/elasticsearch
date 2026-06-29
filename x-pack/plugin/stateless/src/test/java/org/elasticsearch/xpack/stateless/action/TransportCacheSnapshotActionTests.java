/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.action;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

public class TransportCacheSnapshotActionTests extends ESTestCase {

    // ------------------------------------------------------------------
    // CacheSnapshotNodeRequest
    // ------------------------------------------------------------------

    /**
     * {@link CacheSnapshotNodeRequest} carries no fields beyond the base transport-request
     * header; verify that serialize → deserialize is a no-op.
     */
    public void testCacheSnapshotNodeRequestRoundTrip() throws IOException {
        CacheSnapshotNodeRequest original = new CacheSnapshotNodeRequest();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            CacheSnapshotNodeRequest copy = new CacheSnapshotNodeRequest(out.bytes().streamInput());
            // No fields to compare; successful construction is the assertion.
            assertNotNull(copy);
        }
    }

    // ------------------------------------------------------------------
    // CacheSnapshotNodeResponse
    // ------------------------------------------------------------------

    /**
     * Verify that the snapshot ID survives a write → read round-trip for
     * {@link CacheSnapshotNodeResponse}.
     */
    public void testCacheSnapshotNodeResponseRoundTrip() throws IOException {
        DiscoveryNode node = DiscoveryNodeUtils.create("node-1");
        String snapshotId = "snap-" + randomAlphaOfLength(8);
        CacheSnapshotNodeResponse original = new CacheSnapshotNodeResponse(node, snapshotId);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            CacheSnapshotNodeResponse copy = new CacheSnapshotNodeResponse(out.bytes().streamInput());
            assertEquals(snapshotId, copy.snapshotId());
        }
    }

    /**
     * Direct assertion that {@link CacheSnapshotNodeResponse#snapshotId()} returns exactly
     * the value passed to the constructor.
     */
    public void testSnapshotIdIsPreservedInResponse() {
        DiscoveryNode node = DiscoveryNodeUtils.create("node-abc");
        CacheSnapshotNodeResponse response = new CacheSnapshotNodeResponse(node, "snap-abc");
        assertEquals("snap-abc", response.snapshotId());
    }

    // ------------------------------------------------------------------
    // CacheSnapshotRequest
    // ------------------------------------------------------------------

    /**
     * {@link CacheSnapshotRequest} is a {@link org.elasticsearch.action.support.nodes.BaseNodesRequest};
     * verify it can be constructed with a target node. {@code BaseNodesRequest} is local-only
     * (its {@code writeTo} throws), so there is nothing to serialize.
     */
    public void testCacheSnapshotRequestRoundTrip() {
        DiscoveryNode target = DiscoveryNodeUtils.create("target-node");
        // Verify construction succeeds and returns a non-null instance.
        CacheSnapshotRequest request = new CacheSnapshotRequest(target);
        assertNotNull(request);
        // nodesIds() is null when constructed from a DiscoveryNode (concrete-node path).
        assertNull(request.nodesIds());
    }

    // ------------------------------------------------------------------
    // CacheSnapshotResponse
    // ------------------------------------------------------------------

    /**
     * Verify that {@link CacheSnapshotResponse} round-trips correctly and that
     * {@link CacheSnapshotResponse#snapshotId()} returns the value from the wrapped node response.
     */
    public void testCacheSnapshotResponseRoundTrip() throws IOException {
        DiscoveryNode node = DiscoveryNodeUtils.create("resp-node");
        String snapshotId = "snap-resp-" + randomAlphaOfLength(6);
        CacheSnapshotNodeResponse nodeResponse = new CacheSnapshotNodeResponse(node, snapshotId);
        CacheSnapshotResponse original = new CacheSnapshotResponse(ClusterName.DEFAULT, List.of(nodeResponse), List.of());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            CacheSnapshotResponse copy = new CacheSnapshotResponse(out.bytes().streamInput());
            assertEquals(snapshotId, copy.snapshotId());
        }
    }

    /**
     * When there are no node responses, {@link CacheSnapshotResponse#snapshotId()} must return
     * {@code null} rather than throwing.
     */
    public void testCacheSnapshotResponseEmptyNodesReturnsNullSnapshotId() throws IOException {
        CacheSnapshotResponse response = new CacheSnapshotResponse(ClusterName.DEFAULT, List.of(), List.of());
        assertNull(response.snapshotId());

        // Also verify the empty case survives serialization.
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            CacheSnapshotResponse copy = new CacheSnapshotResponse(out.bytes().streamInput());
            assertNull(copy.snapshotId());
        }
    }

    /**
     * A response carrying a {@link FailedNodeException} (no successful node responses) must
     * serialize/deserialize cleanly and return {@code null} for the snapshot ID.
     */
    public void testCacheSnapshotResponseWithFailureRoundTrip() throws IOException {
        DiscoveryNode node = DiscoveryNodeUtils.create("failed-node");
        FailedNodeException failure = new FailedNodeException(node.getId(), "simulated failure", new IOException("boom"));
        CacheSnapshotResponse original = new CacheSnapshotResponse(ClusterName.DEFAULT, List.of(), List.of(failure));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            CacheSnapshotResponse copy = new CacheSnapshotResponse(out.bytes().streamInput());
            assertNull(copy.snapshotId());
            assertEquals(1, copy.failures().size());
        }
    }
}
