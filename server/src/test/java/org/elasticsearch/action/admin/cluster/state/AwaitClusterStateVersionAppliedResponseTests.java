/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.transport.RemoteTransportException;

import java.util.List;

public class AwaitClusterStateVersionAppliedResponseTests extends ESTestCase {

    public void testActualFailuresFiltersActionNotFound() {
        final var node = DiscoveryNodeUtils.create("node1");
        final var failure = new FailedNodeException(node.getId(), "no handler", new ActionNotFoundTransportException("some:action"));

        final var response = new AwaitClusterStateVersionAppliedResponse(ClusterName.DEFAULT, List.of(), List.of(failure));

        assertEquals(1, response.failures().size());
        assertTrue("actualFailures should exclude ActionNotFoundTransportException", response.actualFailures().isEmpty());
        assertFalse(response.hasActualFailures());
    }

    public void testActualFailuresFiltersActionNotFoundWrappedInRemoteException() {
        final var node = DiscoveryNodeUtils.create("node1");
        final var remote = new RemoteTransportException("remote", new ActionNotFoundTransportException("some:action"));
        final var failure = new FailedNodeException(node.getId(), "no handler", remote);

        final var response = new AwaitClusterStateVersionAppliedResponse(ClusterName.DEFAULT, List.of(), List.of(failure));

        assertEquals(1, response.failures().size());
        assertTrue(
            "actualFailures should exclude RemoteTransportException-wrapped ActionNotFoundTransportException",
            response.actualFailures().isEmpty()
        );
        assertFalse(response.hasActualFailures());
    }

    public void testActualFailuresKeepsGenuineFailures() {
        final var node = DiscoveryNodeUtils.create("node1");
        final var remote = new RemoteTransportException("remote", new ElasticsearchTimeoutException("timed out"));
        final var failure = new FailedNodeException(node.getId(), "timeout", remote);

        final var response = new AwaitClusterStateVersionAppliedResponse(ClusterName.DEFAULT, List.of(), List.of(failure));

        assertEquals(1, response.failures().size());
        assertTrue(response.hasActualFailures());
        final var actual = response.actualFailures();
        assertEquals(1, actual.size());
        assertSame(failure, actual.get(0));
    }

    public void testActualFailuresMixedFailures() {
        final var node1 = DiscoveryNodeUtils.create("node1");
        final var node2 = DiscoveryNodeUtils.create("node2");

        final var actionNotFoundFailure = new FailedNodeException(
            node1.getId(),
            "no handler",
            new RemoteTransportException("remote", new ActionNotFoundTransportException("some:action"))
        );
        final var timeoutFailure = new FailedNodeException(
            node2.getId(),
            "timeout",
            new RemoteTransportException("remote", new ElasticsearchTimeoutException("timed out"))
        );

        final var response = new AwaitClusterStateVersionAppliedResponse(
            ClusterName.DEFAULT,
            List.of(),
            List.of(actionNotFoundFailure, timeoutFailure)
        );

        assertEquals(2, response.failures().size());
        assertTrue(response.hasActualFailures());
        final var actual = response.actualFailures();
        assertEquals(1, actual.size());
        assertSame(timeoutFailure, actual.get(0));
    }

    public void testActualFailuresEmptyWhenNoFailures() {
        final var response = new AwaitClusterStateVersionAppliedResponse(ClusterName.DEFAULT, List.of(), List.of());

        assertTrue(response.actualFailures().isEmpty());
        assertFalse(response.hasActualFailures());
    }
}
