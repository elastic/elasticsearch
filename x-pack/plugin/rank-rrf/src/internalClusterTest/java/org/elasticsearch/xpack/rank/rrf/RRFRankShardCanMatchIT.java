/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.builder.SubSearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(maxNumDataNodes = 3)
@ESIntegTestCase.SuiteScopeTestCase
public class RRFRankShardCanMatchIT extends ESIntegTestCase {

    public static class SkipShardPlugin extends Plugin implements SearchPlugin {

        public static class SkipShardQueryBuilder extends TermQueryBuilder {

            private final int shardA;
            private final int shardB;

            public SkipShardQueryBuilder(int shardA, int shardB, String fieldName, String value) {
                super(fieldName, value);
                this.shardA = shardA;
                this.shardB = shardB;
            }

            public SkipShardQueryBuilder(StreamInput in) throws IOException {
                super(in);
                this.shardA = in.readVInt();
                this.shardB = in.readVInt();
            }

            @Override
            public String getWriteableName() {
                return "SkipShard";
            }

            @Override
            protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
                SearchExecutionContext sec = queryRewriteContext.convertToSearchExecutionContext();
                if (sec != null) {
                    int shardId = sec.getShardId();
                    int v = Integer.parseInt(((BytesRef) value).utf8ToString());
                    if ((shardId != shardA || v < 0 || v > 9) && (shardId != shardB || v < 10 || v > 19)) {
                        return new MatchNoneQueryBuilder();
                    }
                }
                return super.doRewrite(queryRewriteContext);
            }

            @Override
            public void doWriteTo(StreamOutput out) throws IOException {
                super.doWriteTo(out);
                out.writeVInt(shardA);
                out.writeVInt(shardB);
            }

            public static SkipShardQueryBuilder fromXContent(XContentParser parser) throws IOException {
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public List<QuerySpec<?>> getQueries() {
            return List.of(new QuerySpec<>("SkipShard", SkipShardQueryBuilder::new, SkipShardQueryBuilder::fromXContent));
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(RRFRankPlugin.class, SkipShardPlugin.class);
    }

    @Override
    protected int minimumNumberOfShards() {
        return 5;
    }

    @Override
    protected int maximumNumberOfShards() {
        return 5;
    }

    @Override
    protected int minimumNumberOfReplicas() {
        return 0;
    }

    @Override
    protected int maximumNumberOfReplicas() {
        return 0;
    }

    public void testCanMatchShard() throws IOException {
        // setup the cluster
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("value")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject();

        assertAcked(prepareCreate("value_index").setMapping(builder));
        ensureGreen("value_index");

        int shardA = -1;
        int shardB = -1;

        for (int i = 0; i < 10; i++) {
            IndexResponse ir = client().prepareIndex("value_index").setSource("value", "" + i).setRouting("a").get();
            int a = ir.getShardId().id();
            assertTrue(shardA == a || shardA == -1);
            shardA = a;
        }
        for (int i = 10; i < 20; i++) {
            IndexResponse ir = client().prepareIndex("value_index").setSource("value", "" + i).setRouting("b").get();
            int b = ir.getShardId().id();
            assertTrue(shardB == b || shardB == -1);
            shardB = b;
        }

        client().admin().indices().prepareRefresh("value_index").get();

        // match 2 separate shard with no overlap in queries
        SearchResponse response = client().prepareSearch("value_index")
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setPreFilterShardSize(1)
            .setRankBuilder(new RRFRankBuilder(20, 1))
            .setTrackTotalHits(false)
            .setQueries(
                List.of(
                    new SubSearchSourceBuilder(new SkipShardPlugin.SkipShardQueryBuilder(shardA, shardB, "value", "9")),
                    new SubSearchSourceBuilder(new SkipShardPlugin.SkipShardQueryBuilder(shardA, shardB, "value", "19"))
                )
            )
            .setSize(5)
            .get();

        assertNull(response.getHits().getTotalHits());
        assertEquals(2, response.getHits().getHits().length);
        assertEquals(5, response.getSuccessfulShards());
        assertEquals(3, response.getSkippedShards());

        // match one shard with one query and do not match the other shard with one query
        response = client().prepareSearch("value_index")
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setPreFilterShardSize(1)
            .setRankBuilder(new RRFRankBuilder(20, 1))
            .setTrackTotalHits(false)
            .setQueries(
                List.of(
                    new SubSearchSourceBuilder(new SkipShardPlugin.SkipShardQueryBuilder(shardA, shardB, "value", "30")),
                    new SubSearchSourceBuilder(new SkipShardPlugin.SkipShardQueryBuilder(shardA, shardB, "value", "19"))
                )
            )
            .setSize(5)
            .get();

        assertNull(response.getHits().getTotalHits());
        assertEquals(1, response.getHits().getHits().length);
        assertEquals(5, response.getSuccessfulShards());
        assertEquals(4, response.getSkippedShards());

        // match no shards, but still use one to generate a search response
        response = client().prepareSearch("value_index")
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setPreFilterShardSize(1)
            .setRankBuilder(new RRFRankBuilder(20, 1))
            .setTrackTotalHits(false)
            .setQueries(
                List.of(
                    new SubSearchSourceBuilder(new SkipShardPlugin.SkipShardQueryBuilder(shardA, shardB, "value", "30")),
                    new SubSearchSourceBuilder(new SkipShardPlugin.SkipShardQueryBuilder(shardA, shardB, "value", "40"))
                )
            )
            .setSize(5)
            .get();

        assertNull(response.getHits().getTotalHits());
        assertEquals(0, response.getHits().getHits().length);
        assertEquals(5, response.getSuccessfulShards());
        assertEquals(4, response.getSkippedShards());

        // match the same shard for both queries
        response = client().prepareSearch("value_index")
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setPreFilterShardSize(1)
            .setRankBuilder(new RRFRankBuilder(20, 1))
            .setTrackTotalHits(false)
            .setQueries(
                List.of(
                    new SubSearchSourceBuilder(new SkipShardPlugin.SkipShardQueryBuilder(shardA, shardB, "value", "15")),
                    new SubSearchSourceBuilder(new SkipShardPlugin.SkipShardQueryBuilder(shardA, shardB, "value", "16"))
                )
            )
            .setSize(5)
            .get();

        assertNull(response.getHits().getTotalHits());
        assertEquals(2, response.getHits().getHits().length);
        assertEquals(5, response.getSuccessfulShards());
        assertEquals(4, response.getSkippedShards());

        // match one shard with the exact same query
        response = client().prepareSearch("value_index")
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setPreFilterShardSize(1)
            .setRankBuilder(new RRFRankBuilder(20, 1))
            .setTrackTotalHits(false)
            .setQueries(
                List.of(
                    new SubSearchSourceBuilder(new SkipShardPlugin.SkipShardQueryBuilder(shardA, shardB, "value", "8")),
                    new SubSearchSourceBuilder(new SkipShardPlugin.SkipShardQueryBuilder(shardA, shardB, "value", "8"))
                )
            )
            .setSize(5)
            .get();

        assertNull(response.getHits().getTotalHits());
        assertEquals(1, response.getHits().getHits().length);
        assertEquals(5, response.getSuccessfulShards());
        assertEquals(4, response.getSkippedShards());
    }
}
