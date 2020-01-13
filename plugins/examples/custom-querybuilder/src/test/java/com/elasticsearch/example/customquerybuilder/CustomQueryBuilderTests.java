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

package com.elasticsearch.example.customquerybuilder;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.example.customquerybuilder.CustomQueryBuilder;
import org.elasticsearch.example.customquerybuilder.CustomQueryBuilderPlugin;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.join.query.HasChildQueryBuilder;
import org.elasticsearch.percolator.PercolatorPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.startsWith;

public class CustomQueryBuilderTests extends AbstractQueryTestCase<CustomQueryBuilder> {
    private static final String TYPE = "_doc";
    private static final String PARENT_DOC = "parent";
    private static final String CHILD_DOC = "child";
    private static final String JOIN_FIELD = "join_field";
    private static final String QUERY_FIELD = "query";
    private static final String PHRASE_FIELD = "phrase";

    private String phrase;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(CustomQueryBuilderPlugin.class, ParentJoinPlugin.class, PercolatorPlugin.class);
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        XContentBuilder mapping = jsonBuilder().startObject().startObject(TYPE).startObject("properties")
            .startObject(JOIN_FIELD)
            .field("type", "join")
            .startObject("relations")
            .field(PARENT_DOC, CHILD_DOC)
            .endObject()
            .endObject()
            .startObject(QUERY_FIELD)
            .field("type", "percolator")
            .endObject()
            .startObject(PHRASE_FIELD)
            .field("type", "text")
            .endObject()
            .endObject().endObject().endObject();

        mapperService.merge(TYPE,
            new CompressedXContent(Strings.toString(mapping)), MapperService.MergeReason.MAPPING_UPDATE);
    }

    @Override
    protected CustomQueryBuilder doCreateTestQueryBuilder() {
        phrase = randomAlphaOfLength(4);
        return new CustomQueryBuilder(phrase);
    }

    @Override
    protected void doAssertLuceneQuery(CustomQueryBuilder queryBuilder, Query query, QueryShardContext context) {
        assertThat(query, instanceOf(HasChildQueryBuilder.LateParsingQuery.class));
        HasChildQueryBuilder.LateParsingQuery lateParsingQuery = (HasChildQueryBuilder.LateParsingQuery) query;
        assertEquals(ScoreMode.Max, lateParsingQuery.getScoreMode());
        assertThat(lateParsingQuery.getInnerQuery(), instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) lateParsingQuery.getInnerQuery();
        for (BooleanClause booleanClause : booleanQuery.clauses()) {
            if (BooleanClause.Occur.FILTER.equals(booleanClause.getOccur())) {
                assertThat(booleanClause.getQuery(), instanceOf(TermQuery.class));
                assertEquals(new Term(JOIN_FIELD, CHILD_DOC), ((TermQuery) booleanClause.getQuery()).getTerm());
            } else if (BooleanClause.Occur.MUST.equals(booleanClause.getOccur())) {
                // unable to check if query is instance of PercolateQuery because this class is not public
                assertThat(booleanClause.getQuery().toString(), startsWith("PercolateQuery"));
                assertThat(booleanClause.getQuery().toString(), containsString(phrase));
            } else {
                fail("Unexpected clause inside [has_child] query");
            }
        }
    }

    /**
     * As percolate query is not cacheable, this one is also not
     */
    @Override
    public void testCacheability() throws IOException {
        CustomQueryBuilder queryBuilder = createTestQueryBuilder();
        QueryShardContext context = createShardContext();
        QueryBuilder rewritten = rewriteQuery(queryBuilder, new QueryShardContext(context));
        assertNotNull(rewritten.toQuery(context));
        assertFalse("query should not be cacheable: " + queryBuilder.toString(), context.isCacheable());
    }

    @Override
    protected boolean builderGeneratesCacheableQueries() {
        return false;
    }
}
