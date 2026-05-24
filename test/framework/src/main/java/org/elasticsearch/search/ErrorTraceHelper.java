/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchQueryThenFetchAsyncAction.NodeQueryResponse;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.BytesTransportResponse;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.elasticsearch.action.search.SearchQueryThenFetchAsyncAction.NODE_SEARCH_ACTION_NAME;
import static org.elasticsearch.common.Strings.format;
import static org.elasticsearch.test.ESIntegTestCase.internalCluster;
import static org.elasticsearch.test.ESTestCase.asInstanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Utilities around testing the `error_trace` message header in search.
 */
public enum ErrorTraceHelper {
    ;

    /**
     * Sets up transport interception to assert that stack traces are present in error responses for batched query requests.
     * Must be called before executing requests that are expected to generate errors.
     */
    public static void expectStackTraceObserved(InternalTestCluster internalCluster) {
        expectStackTraceObserved(internalCluster, true);
    }

    /**
     * Sets up transport interception to assert that stack traces are NOT present in error responses for batched query requests.
     * Must be called before executing requests that are expected to generate errors.
     */
    public static void expectStackTraceCleared(InternalTestCluster internalCluster) {
        expectStackTraceObserved(internalCluster, false);
    }

    private static void expectStackTraceObserved(InternalTestCluster internalCluster, boolean shouldObserveStackTrace) {
        internalCluster.getDataNodeInstances(TransportService.class)
            .forEach(
                ts -> asInstanceOf(MockTransportService.class, ts).addRequestHandlingBehavior(
                    NODE_SEARCH_ACTION_NAME,
                    (handler, request, channel, task) -> {
                        TransportChannel wrappedChannel = new TransportChannel() {
                            @Override
                            public String getProfileName() {
                                return channel.getProfileName();
                            }

                            @Override
                            public void sendResponse(TransportResponse response) {
                                var bytes = asInstanceOf(BytesTransportResponse.class, response);
                                NodeQueryResponse nodeQueryResponse = null;
                                try (StreamInput in = bytes.bytes().streamInput()) {
                                    var namedWriteableAwareInput = new NamedWriteableAwareStreamInput(
                                        in,
                                        internalCluster.getNamedWriteableRegistry()
                                    );
                                    nodeQueryResponse = new NodeQueryResponse(namedWriteableAwareInput);
                                    for (Object result : nodeQueryResponse.getResults()) {
                                        if (result instanceof Exception error) {
                                            inspectStackTraceAndAssert(error);
                                        }
                                    }
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                } finally {
                                    // Always forward to the original channel
                                    channel.sendResponse(response);
                                    if (nodeQueryResponse != null) {
                                        nodeQueryResponse.decRef();
                                    }
                                }
                            }

                            @Override
                            public void sendResponse(Exception error) {
                                try {
                                    inspectStackTraceAndAssert(error);
                                } finally {
                                    // Always forward to the original channel
                                    channel.sendResponse(error);
                                }
                            }

                            private void inspectStackTraceAndAssert(Exception error) {
                                ExceptionsHelper.unwrapCausesAndSuppressed(error, t -> {
                                    if (shouldObserveStackTrace) {
                                        assertTrue(t.getStackTrace().length > 0);
                                    } else {
                                        assertEquals(0, t.getStackTrace().length);
                                    }
                                    return true;
                                });
                            }
                        };

                        handler.messageReceived(request, wrappedChannel, task);
                    }
                )
            );
    }

    /**
     * Adds expectations for debug logging of a message and exception on each shard of the given index.
     *
     * @param numShards                 the number of shards in the index (an expectation will be added for each shard)
     * @param mockLog                   the mock log
     * @param errorTriggeringIndex      the name of the index that will trigger the error
     */
    public static void addSeenLoggingExpectations(int numShards, MockLog mockLog, String errorTriggeringIndex) {
        String nodesDisjunction = format(
            "(%s)",
            Arrays.stream(internalCluster().getNodeNames()).map(ESIntegTestCase::getNodeId).collect(Collectors.joining("|"))
        );
        for (int shard = 0; shard < numShards; shard++) {
            mockLog.addExpectation(
                new MockLog.PatternAndExceptionSeenEventExpectation(
                    format(
                        "\"[%s][%s][%d]: failed to execute search request for task [\\d+]\" and an exception logged",
                        nodesDisjunction,
                        errorTriggeringIndex,
                        shard
                    ),
                    SearchService.class.getCanonicalName(),
                    Level.DEBUG,
                    format(
                        "\\[%s\\]\\[%s\\]\\[%d\\]: failed to execute search request for task \\[\\d+\\]",
                        nodesDisjunction,
                        errorTriggeringIndex,
                        shard
                    ),
                    QueryShardException.class,
                    "failed to create query: For input string: \"foo\""
                )
            );
        }
    }
}
