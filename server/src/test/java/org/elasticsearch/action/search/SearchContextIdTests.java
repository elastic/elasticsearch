/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class SearchContextIdTests extends ESTestCase {

    QueryBuilder randomQueryBuilder() {
        if (randomBoolean()) {
            return new TermQueryBuilder(randomAlphaOfLength(10), randomAlphaOfLength(10));
        } else if (randomBoolean()) {
            return new MatchAllQueryBuilder();
        } else {
            return new IdsQueryBuilder().addIds(randomAlphaOfLength(10));
        }
    }

    public void testEncode() {
        final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(List.of(
            new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new),
            new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new),
            new NamedWriteableRegistry.Entry(QueryBuilder.class, IdsQueryBuilder.NAME, IdsQueryBuilder::new)
        ));
        final AtomicArray<SearchPhaseResult> queryResults = TransportSearchHelperTests.generateQueryResults();
        final Version version = Version.CURRENT;
        final Map<String, AliasFilter> aliasFilters = new HashMap<>();
        for (SearchPhaseResult result : queryResults.asList()) {
            final AliasFilter aliasFilter;
            if (randomBoolean()) {
                aliasFilter = new AliasFilter(randomQueryBuilder());
            } else if (randomBoolean()) {
                aliasFilter = new AliasFilter(randomQueryBuilder(), "alias-" + between(1, 10));
            } else {
                aliasFilter = AliasFilter.EMPTY;
            }
            if (randomBoolean()) {
                aliasFilters.put(result.getSearchShardTarget().getShardId().getIndex().getUUID(), aliasFilter);
            }
        }
        final String id = SearchContextId.encode(queryResults.asList(), aliasFilters, version);
        final SearchContextId context = SearchContextId.decode(namedWriteableRegistry, id);
        assertThat(context.shards().keySet(), hasSize(3));
        assertThat(context.aliasFilter(), equalTo(aliasFilters));
        SearchContextIdForNode node1 = context.shards().get(new ShardId("idx", "uuid1", 2));
        assertThat(node1.getClusterAlias(), equalTo("cluster_x"));
        assertThat(node1.getNode(), equalTo("node_1"));
        assertThat(node1.getSearchContextId().getId(), equalTo(1L));
        assertThat(node1.getSearchContextId().getSessionId(), equalTo("a"));

        SearchContextIdForNode node2 = context.shards().get(new ShardId("idy", "uuid2", 42));
        assertThat(node2.getClusterAlias(), equalTo("cluster_y"));
        assertThat(node2.getNode(), equalTo("node_2"));
        assertThat(node2.getSearchContextId().getId(), equalTo(12L));
        assertThat(node2.getSearchContextId().getSessionId(), equalTo("b"));

        SearchContextIdForNode node3 = context.shards().get(new ShardId("idy", "uuid2", 43));
        assertThat(node3.getClusterAlias(), nullValue());
        assertThat(node3.getNode(), equalTo("node_3"));
        assertThat(node3.getSearchContextId().getId(), equalTo(42L));
        assertThat(node3.getSearchContextId().getSessionId(), equalTo("c"));
    }
}
