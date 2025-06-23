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
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportMessageListener;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.elasticsearch.common.Strings.format;
import static org.elasticsearch.test.ESIntegTestCase.internalCluster;
import static org.elasticsearch.test.ESTestCase.asInstanceOf;

/**
 * Utilities around testing the `error_trace` message header in search.
 */
public enum ErrorTraceHelper {
    ;

    public static BooleanSupplier setupErrorTraceListener(InternalTestCluster internalCluster) {
        final AtomicBoolean transportMessageHasStackTrace = new AtomicBoolean(false);
        internalCluster.getDataNodeInstances(TransportService.class)
            .forEach(ts -> asInstanceOf(MockTransportService.class, ts).addMessageListener(new TransportMessageListener() {
                @Override
                public void onResponseSent(long requestId, String action, Exception error) {
                    TransportMessageListener.super.onResponseSent(requestId, action, error);
                    if (action.startsWith("indices:data/read/search")) {
                        Optional<Throwable> throwable = ExceptionsHelper.unwrapCausesAndSuppressed(
                            error,
                            t -> t.getStackTrace().length > 0
                        );
                        transportMessageHasStackTrace.set(throwable.isPresent());
                    }
                }
            }));
        return transportMessageHasStackTrace::get;
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
