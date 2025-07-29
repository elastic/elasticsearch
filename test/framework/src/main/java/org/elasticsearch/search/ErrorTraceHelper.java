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
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.elasticsearch.common.Strings.format;
import static org.elasticsearch.test.ESIntegTestCase.internalCluster;

/**
 * Utilities around testing the `error_trace` message header in search.
 */
public enum ErrorTraceHelper {
    ;

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
