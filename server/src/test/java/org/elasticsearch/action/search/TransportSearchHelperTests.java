/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.search;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class TransportSearchHelperTests extends ESTestCase {

    public static AtomicArray<SearchPhaseResult> generateQueryResults() {
        AtomicArray<SearchPhaseResult> array = new AtomicArray<>(3);
        DiscoveryNode node1 = DiscoveryNodeUtils.create("node_1");
        DiscoveryNode node2 = DiscoveryNodeUtils.create("node_2");
        DiscoveryNode node3 = DiscoveryNodeUtils.create("node_3");
        SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult1 = new SearchAsyncActionTests.TestSearchPhaseResult(
            new ShardSearchContextId("a", 1),
            node1
        );
        testSearchPhaseResult1.setSearchShardTarget(new SearchShardTarget("node_1", new ShardId("idx", "uuid1", 2), "cluster_x"));
        SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult2 = new SearchAsyncActionTests.TestSearchPhaseResult(
            new ShardSearchContextId("b", 12),
            node2
        );
        testSearchPhaseResult2.setSearchShardTarget(new SearchShardTarget("node_2", new ShardId("idy", "uuid2", 42), "cluster_y"));
        SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult3 = new SearchAsyncActionTests.TestSearchPhaseResult(
            new ShardSearchContextId("c", 42),
            node3
        );
        testSearchPhaseResult3.setSearchShardTarget(new SearchShardTarget("node_3", new ShardId("idy", "uuid2", 43), null));
        array.setOnce(0, testSearchPhaseResult1);
        array.setOnce(1, testSearchPhaseResult2);
        array.setOnce(2, testSearchPhaseResult3);
        return array;
    }

    public void testParseScrollId() {
        final AtomicArray<SearchPhaseResult> queryResults = generateQueryResults();
        String scrollId = TransportSearchHelper.buildScrollId(queryResults);
        ParsedScrollId parseScrollId = TransportSearchHelper.parseScrollId(scrollId);
        assertEquals(3, parseScrollId.getContext().length);
        assertEquals("node_1", parseScrollId.getContext()[0].getNode());
        assertEquals("cluster_x", parseScrollId.getContext()[0].getClusterAlias());
        assertEquals(1, parseScrollId.getContext()[0].getSearchContextId().getId());
        assertThat(parseScrollId.getContext()[0].getSearchContextId().getSessionId(), equalTo("a"));

        assertEquals("node_2", parseScrollId.getContext()[1].getNode());
        assertEquals("cluster_y", parseScrollId.getContext()[1].getClusterAlias());
        assertEquals(12, parseScrollId.getContext()[1].getSearchContextId().getId());
        assertThat(parseScrollId.getContext()[1].getSearchContextId().getSessionId(), equalTo("b"));

        assertEquals("node_3", parseScrollId.getContext()[2].getNode());
        assertNull(parseScrollId.getContext()[2].getClusterAlias());
        assertEquals(42, parseScrollId.getContext()[2].getSearchContextId().getId());
        assertThat(parseScrollId.getContext()[2].getSearchContextId().getSessionId(), equalTo("c"));
    }
}
