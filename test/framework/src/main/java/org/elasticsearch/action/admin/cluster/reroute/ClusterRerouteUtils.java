/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.reroute;

import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommand;
import org.elasticsearch.exception.ElasticsearchException;

import static org.elasticsearch.test.ESTestCase.TEST_REQUEST_TIMEOUT;
import static org.elasticsearch.test.ESTestCase.asInstanceOf;
import static org.elasticsearch.test.ESTestCase.safeAwait;
import static org.elasticsearch.test.ESTestCase.safeGet;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Utilities for invoking {@link TransportClusterRerouteAction} in tests.
 */
public class ClusterRerouteUtils {
    private ClusterRerouteUtils() {/* no instances */}

    /**
     * Execute {@link TransportClusterRerouteAction} with the given (optional) sequence of {@link AllocationCommand} instances. Asserts that
     * this succeeds.
     */
    public static void reroute(ElasticsearchClient client, AllocationCommand... allocationCommands) {
        doReroute(client, false, allocationCommands);
    }

    /**
     * Execute {@link TransportClusterRerouteAction} to reset the allocation failure counter. Asserts that this succeeds.
     */
    public static void rerouteRetryFailed(ElasticsearchClient client) {
        doReroute(client, true);
    }

    private static void doReroute(ElasticsearchClient client, boolean retryFailed, AllocationCommand... allocationCommands) {
        assertAcked(
            safeGet(
                client.execute(
                    TransportClusterRerouteAction.TYPE,
                    new ClusterRerouteRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).setRetryFailed(retryFailed)
                        .add(allocationCommands)
                )
            )
        );
    }

    /**
     * Execute {@link TransportClusterRerouteAction} with the given (optional) sequence of {@link AllocationCommand} instances, asserts that
     * it fails, and returns the resulting (unwrapped) exception.
     */
    public static Exception expectRerouteFailure(ElasticsearchClient client, AllocationCommand... allocationCommands) {
        final Exception wrappedException = safeAwait(
            SubscribableListener.newForked(
                l -> client.execute(
                    TransportClusterRerouteAction.TYPE,
                    new ClusterRerouteRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).add(allocationCommands),
                    ActionTestUtils.assertNoSuccessListener(l::onResponse)
                )
            )
        );
        return asInstanceOf(Exception.class, wrappedException instanceof ElasticsearchException esx ? esx.unwrapCause() : wrappedException);
    }
}
