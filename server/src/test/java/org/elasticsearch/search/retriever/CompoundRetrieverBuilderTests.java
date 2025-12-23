/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.when;

public class CompoundRetrieverBuilderTests extends ESTestCase {
    public void testShardFailureHandling() {
        SearchResponse response = Mockito.mock(SearchResponse.class);
        ShardSearchFailure[] shardFailures = new ShardSearchFailure[2];
        shardFailures[0] = new ShardSearchFailure(
            new IOException("some shard failed"), // 500
            new SearchShardTarget("1", new ShardId("1", "1", 1), "foo")
        );
        shardFailures[1] = new ShardSearchFailure(
            new IOException("some second shard failed"), // 500
            new SearchShardTarget("2", new ShardId("2", "2", 2), "bar")
        );
        when(response.getShardFailures()).thenReturn(shardFailures);

        int priorStatusCode = randomIntBetween(200, 600);
        List<Exception> failures = new ArrayList<>();
        int shardFailureStatusCode = new TestCompoundRetrieverBuilder(0).handleShardFailures(response, priorStatusCode, failures);

        assertEquals(2, failures.size());
        assertEquals("failed to retrieve data from shard [1] with message: some shard failed", failures.get(0).getMessage());
        assertEquals("failed to retrieve data from shard [2] with message: some second shard failed", failures.get(1).getMessage());
        assertEquals(Math.max(priorStatusCode, 500), shardFailureStatusCode);
    }
}
