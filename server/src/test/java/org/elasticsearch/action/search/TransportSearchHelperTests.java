/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.search;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.SearchContextId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class TransportSearchHelperTests extends ESTestCase {

    AtomicArray<SearchPhaseResult> generateQueryResults() {
        AtomicArray<SearchPhaseResult> array = new AtomicArray<>(3);
        DiscoveryNode node1 = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode node3 = new DiscoveryNode("node_3", buildNewFakeTransportAddress(), Version.CURRENT);
        SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult1 =
            new SearchAsyncActionTests.TestSearchPhaseResult(new SearchContextId("a", 1), node1);
        testSearchPhaseResult1.setSearchShardTarget(new SearchShardTarget("node_1", new ShardId("idx", "uuid1", 2), "cluster_x", null));
        SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult2 =
            new SearchAsyncActionTests.TestSearchPhaseResult(new SearchContextId("b", 12), node2);
        testSearchPhaseResult2.setSearchShardTarget(new SearchShardTarget("node_2", new ShardId("idy", "uuid2", 42), "cluster_y", null));
        SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult3 =
            new SearchAsyncActionTests.TestSearchPhaseResult(new SearchContextId("c", 42), node3);
        testSearchPhaseResult3.setSearchShardTarget(new SearchShardTarget("node_3", new ShardId("idy", "uuid2", 43), null, null));
        array.setOnce(0, testSearchPhaseResult1);
        array.setOnce(1, testSearchPhaseResult2);
        array.setOnce(2, testSearchPhaseResult3);
        return array;
    }

    public void testParseScrollId()  {
        final Version version = VersionUtils.randomVersion(random());
        boolean includeUUID = version.onOrAfter(Version.V_7_7_0);
        final AtomicArray<SearchPhaseResult> queryResults = generateQueryResults();
        String scrollId = TransportSearchHelper.buildScrollId(queryResults, version);
        ParsedScrollId parseScrollId = TransportSearchHelper.parseScrollId(scrollId);
        assertEquals(3, parseScrollId.getContext().length);
        assertEquals("node_1", parseScrollId.getContext()[0].getNode());
        assertEquals("cluster_x", parseScrollId.getContext()[0].getClusterAlias());
        assertEquals(1, parseScrollId.getContext()[0].getContextId().getId());
        if (includeUUID) {
            assertThat(parseScrollId.getContext()[0].getContextId().getReaderId(), equalTo("a"));
        } else {
            assertThat(parseScrollId.getContext()[0].getContextId().getReaderId(), equalTo(""));
        }

        assertEquals("node_2", parseScrollId.getContext()[1].getNode());
        assertEquals("cluster_y", parseScrollId.getContext()[1].getClusterAlias());
        assertEquals(12, parseScrollId.getContext()[1].getContextId().getId());
        if (includeUUID) {
            assertThat(parseScrollId.getContext()[1].getContextId().getReaderId(), equalTo("b"));
        } else {
            assertThat(parseScrollId.getContext()[1].getContextId().getReaderId(), equalTo(""));
        }

        assertEquals("node_3", parseScrollId.getContext()[2].getNode());
        assertNull(parseScrollId.getContext()[2].getClusterAlias());
        assertEquals(42, parseScrollId.getContext()[2].getContextId().getId());
        if (includeUUID) {
            assertThat(parseScrollId.getContext()[2].getContextId().getReaderId(), equalTo("c"));
        } else {
            assertThat(parseScrollId.getContext()[2].getContextId().getReaderId(), equalTo(""));
        }
    }

    public void testEncodeDecodeReaderId() {
        final AtomicArray<SearchPhaseResult> queryResults = generateQueryResults();
        final Version version = VersionUtils.randomVersion(random());
        final String readerId = TransportSearchHelper.encodeReaderIds(queryResults, version);
        final Map<ShardId, ReaderIdForNode> contextIds = TransportSearchHelper.decodeReaderIds(readerId);
        assertThat(contextIds.keySet(), hasSize(3));

        ReaderIdForNode node1 = contextIds.get(new ShardId("idx", "uuid1", 2));
        assertThat(node1.getClusterAlias(), equalTo("cluster_x"));
        assertThat(node1.getNode(), equalTo("node_1"));
        assertThat(node1.getContextId().getId(), equalTo(1L));
        assertThat(node1.getContextId().getReaderId(), equalTo("a"));

        ReaderIdForNode node2 = contextIds.get(new ShardId("idy", "uuid2", 42));
        assertThat(node2.getClusterAlias(), equalTo("cluster_y"));
        assertThat(node2.getNode(), equalTo("node_2"));
        assertThat(node2.getContextId().getId(), equalTo(12L));
        assertThat(node2.getContextId().getReaderId(), equalTo("b"));

        ReaderIdForNode node3 = contextIds.get(new ShardId("idy", "uuid2", 43));
        assertThat(node3.getClusterAlias(), nullValue());
        assertThat(node3.getNode(), equalTo("node_3"));
        assertThat(node3.getContextId().getId(), equalTo(42L));
        assertThat(node3.getContextId().getReaderId(), equalTo("c"));
    }
}
