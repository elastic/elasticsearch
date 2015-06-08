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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.instanceOf;

public class CustomQueryParserTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
                .put("plugin.types", DummyQueryParserPlugin.class.getName()).build();
    }

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

    @Test
    public void testCustomDummyQuery() {
        assertHitCount(client().prepareSearch("index").setQuery(new DummyQueryParserPlugin.DummyQueryBuilder()).get(), 1l);
    }

    @Test
    public void testCustomDummyQueryWithinBooleanQuery() {
        assertHitCount(client().prepareSearch("index").setQuery(new BoolQueryBuilder().must(new DummyQueryParserPlugin.DummyQueryBuilder())).get(), 1l);
    }

    private static IndexQueryParserService queryParser() {
        IndicesService indicesService = internalCluster().getDataNodeInstance(IndicesService.class);
        return indicesService.indexServiceSafe("index").queryParserService();
    }

    @Test //see #11120
    public void testConstantScoreParsesFilter() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        Query q = queryParser.parse(constantScoreQuery(new DummyQueryParserPlugin.DummyQueryBuilder())).query();
        Query inner = ((ConstantScoreQuery) q).getQuery();
        assertThat(inner, instanceOf(DummyQueryParserPlugin.DummyQuery.class));
        assertEquals(true, ((DummyQueryParserPlugin.DummyQuery) inner).isFilter);
    }

    @Test //see #11120
    public void testBooleanParsesFilter() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        // single clause, serialized as inner object
        Query q = queryParser.parse(boolQuery()
                .should(new DummyQueryParserPlugin.DummyQueryBuilder())
                .must(new DummyQueryParserPlugin.DummyQueryBuilder())
                .filter(new DummyQueryParserPlugin.DummyQueryBuilder())
                .mustNot(new DummyQueryParserPlugin.DummyQueryBuilder())).query();
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
        q = queryParser.parse(boolQuery()
                .should(new DummyQueryParserPlugin.DummyQueryBuilder()).should(new DummyQueryParserPlugin.DummyQueryBuilder())
                .must(new DummyQueryParserPlugin.DummyQueryBuilder()).must(new DummyQueryParserPlugin.DummyQueryBuilder())
                .filter(new DummyQueryParserPlugin.DummyQueryBuilder()).filter(new DummyQueryParserPlugin.DummyQueryBuilder())
                .mustNot(new DummyQueryParserPlugin.DummyQueryBuilder()).mustNot(new DummyQueryParserPlugin.DummyQueryBuilder())).query();
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
