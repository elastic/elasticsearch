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

package org.elasticsearch.index.query.plugin;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.instanceOf;

public class CustomQueryParserIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(DummyQueryParserPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Arrays.asList(DummyQueryParserPlugin.class);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        createIndex("test");
        ensureGreen();
        client().prepareIndex("index", "type", "1").setSource("field", "value").get();
        refresh();
    }

    @Override
    protected int numberOfShards() {
        return cluster().numDataNodes();
    }

    public void testCustomDummyQuery() {
        assertHitCount(client().prepareSearch("index").setQuery(new DummyQueryBuilder()).get(), 1L);
    }

    public void testCustomDummyQueryWithinBooleanQuery() {
        assertHitCount(client().prepareSearch("index").setQuery(new BoolQueryBuilder().must(new DummyQueryBuilder())).get(), 1L);
    }

    private static QueryShardContext queryShardContext() {
        IndicesService indicesService = internalCluster().getDataNodeInstance(IndicesService.class);
        return indicesService.indexServiceSafe(resolveIndex("index")).newQueryShardContext(
                randomInt(20), null, () -> { throw new UnsupportedOperationException(); }, null);
    }

    //see #11120
    public void testConstantScoreParsesFilter() throws Exception {
        Query q = constantScoreQuery(new DummyQueryBuilder()).toQuery(queryShardContext());
        Query inner = ((ConstantScoreQuery) q).getQuery();
        assertThat(inner, instanceOf(DummyQueryParserPlugin.DummyQuery.class));
        assertEquals(true, ((DummyQueryParserPlugin.DummyQuery) inner).isFilter);
    }

    //see #11120
    public void testBooleanParsesFilter() throws Exception {
        // single clause, serialized as inner object
        Query q = boolQuery()
                .should(new DummyQueryBuilder())
                .must(new DummyQueryBuilder())
                .filter(new DummyQueryBuilder())
                .mustNot(new DummyQueryBuilder()).toQuery(queryShardContext());
        assertThat(q, instanceOf(BooleanQuery.class));
        BooleanQuery bq = (BooleanQuery) q;
        assertEquals(4, bq.clauses().size());
        for (BooleanClause clause : bq.clauses()) {
            DummyQueryParserPlugin.DummyQuery dummy = (DummyQueryParserPlugin.DummyQuery) clause.getQuery();
            switch (clause.getOccur()) {
                case FILTER:
                case MUST_NOT:
                    assertEquals(true, dummy.isFilter);
                    break;
                case MUST:
                case SHOULD:
                    assertEquals(false, dummy.isFilter);
                    break;
                default:
                    throw new AssertionError();
            }
        }

        // multiple clauses, serialized as inner arrays
        q = boolQuery()
                .should(new DummyQueryBuilder()).should(new DummyQueryBuilder())
                .must(new DummyQueryBuilder()).must(new DummyQueryBuilder())
                .filter(new DummyQueryBuilder()).filter(new DummyQueryBuilder())
                .mustNot(new DummyQueryBuilder()).mustNot(new DummyQueryBuilder()).toQuery(queryShardContext());
        assertThat(q, instanceOf(BooleanQuery.class));
        bq = (BooleanQuery) q;
        assertEquals(8, bq.clauses().size());
        for (BooleanClause clause : bq.clauses()) {
            DummyQueryParserPlugin.DummyQuery dummy = (DummyQueryParserPlugin.DummyQuery) clause.getQuery();
            switch (clause.getOccur()) {
                case FILTER:
                case MUST_NOT:
                    assertEquals(true, dummy.isFilter);
                    break;
                case MUST:
                case SHOULD:
                    assertEquals(false, dummy.isFilter);
                    break;
                default:
                    throw new AssertionError();
            }
        }
    }
}
