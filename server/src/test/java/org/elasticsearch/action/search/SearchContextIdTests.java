/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class SearchContextIdTests extends ESTestCase {

    public void testEncode() {
        final AtomicArray<SearchPhaseResult> queryResults = TransportSearchHelperTests.generateQueryResults();
        final Version version = VersionUtils.randomVersion(random());
        final Map<String, AliasFilter> aliasFilter = new HashMap<>();
        for (SearchPhaseResult result : queryResults.asList()) {
            if (randomBoolean()) {
                aliasFilter.put(result.getSearchShardTarget().getShardId().getIndex().getUUID(),  AliasFilter.EMPTY);
            }
        }
        final String id = SearchContextId.encode(queryResults.asList(), aliasFilter, version);
        final SearchContextId context = SearchContextId.decode(id);
        assertThat(context.shards().keySet(), hasSize(3));
        assertThat(context.aliasFilter(), equalTo(aliasFilter));
        SearchContextIdForNode node1 = context.shards().get(new ShardId("idx", "uuid1", 2));
        assertThat(node1.getClusterAlias(), equalTo("cluster_x"));
        assertThat(node1.getNode(), equalTo("node_1"));
        assertThat(node1.getSearchContextId().getId(), equalTo(1L));
        assertThat(node1.getSearchContextId().getReaderId(), equalTo("a"));

        SearchContextIdForNode node2 = context.shards().get(new ShardId("idy", "uuid2", 42));
        assertThat(node2.getClusterAlias(), equalTo("cluster_y"));
        assertThat(node2.getNode(), equalTo("node_2"));
        assertThat(node2.getSearchContextId().getId(), equalTo(12L));
        assertThat(node2.getSearchContextId().getReaderId(), equalTo("b"));

        SearchContextIdForNode node3 = context.shards().get(new ShardId("idy", "uuid2", 43));
        assertThat(node3.getClusterAlias(), nullValue());
        assertThat(node3.getNode(), equalTo("node_3"));
        assertThat(node3.getSearchContextId().getId(), equalTo(42L));
        assertThat(node3.getSearchContextId().getReaderId(), equalTo("c"));
    }
}
