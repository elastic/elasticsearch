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

package org.elasticsearch.index.query;

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.containsString;

public class ConstantScoreQueryBuilderTests extends AbstractQueryTestCase<ConstantScoreQueryBuilder> {
    /**
     * @return a {@link ConstantScoreQueryBuilder} with random boost between 0.1f and 2.0f
     */
    @Override
    protected ConstantScoreQueryBuilder doCreateTestQueryBuilder() {
        return new ConstantScoreQueryBuilder(RandomQueryBuilder.createQuery(random()));
    }

    @Override
    protected void doAssertLuceneQuery(ConstantScoreQueryBuilder queryBuilder, Query query, SearchContext context) throws IOException {
        Query innerQuery = queryBuilder.innerQuery().toQuery(context.getQueryShardContext());
        if (innerQuery == null) {
            assertThat(query, nullValue());
        } else {
            assertThat(query, instanceOf(ConstantScoreQuery.class));
            ConstantScoreQuery constantScoreQuery = (ConstantScoreQuery) query;
            assertThat(constantScoreQuery.getQuery(), instanceOf(innerQuery.getClass()));
        }
    }

    /**
     * test that missing "filter" element causes {@link ParsingException}
     */
    public void testFilterElement() throws IOException {
        String queryString = "{ \"" + ConstantScoreQueryBuilder.NAME + "\" : {} }";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(queryString));
        assertThat(e.getMessage(), containsString("requires a 'filter' element"));
    }

    /**
     * test that multiple "filter" elements causes {@link ParsingException}
     */
    public void testMultipleFilterElements() throws IOException {
        assumeFalse("Test only makes sense if XContent parser doesn't have strict duplicate checks enabled",
            XContent.isStrictDuplicateDetectionEnabled());
        String queryString = "{ \"" + ConstantScoreQueryBuilder.NAME + "\" : {\n" +
                                    "\"filter\" : { \"term\": { \"foo\": \"a\" } },\n" +
                                    "\"filter\" : { \"term\": { \"foo\": \"x\" } },\n" +
                            "} }";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(queryString));
        assertThat(e.getMessage(), containsString("accepts only one 'filter' element"));
    }

    /**
     * test that "filter" does not accept an array of queries, throws {@link ParsingException}
     */
    public void testNoArrayAsFilterElements() throws IOException {
        String queryString = "{ \"" + ConstantScoreQueryBuilder.NAME + "\" : {\n" +
                                    "\"filter\" : [ { \"term\": { \"foo\": \"a\" } },\n" +
                                                   "{ \"term\": { \"foo\": \"x\" } } ]\n" +
                            "} }";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(queryString));
        assertThat(e.getMessage(), containsString("unexpected token [START_ARRAY]"));
    }

    public void testIllegalArguments() {
        expectThrows(IllegalArgumentException.class, () -> new ConstantScoreQueryBuilder((QueryBuilder) null));
    }

    @Override
    public void testUnknownField() {
        assumeTrue("test doesn't apply for query filter queries", false);
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"constant_score\" : {\n" +
                "    \"filter\" : {\n" +
                "      \"terms\" : {\n" +
                "        \"user\" : [ \"kimchy\", \"elasticsearch\" ],\n" +
                "        \"boost\" : 42.0\n" +
                "      }\n" +
                "    },\n" +
                "    \"boost\" : 23.0\n" +
                "  }\n" +
                "}";

        ConstantScoreQueryBuilder parsed = (ConstantScoreQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, 23.0, parsed.boost(), 0.0001);
        assertEquals(json, 42.0, parsed.innerQuery().boost(), 0.0001);
    }

    public void testRewriteToMatchNone() throws IOException {
        ConstantScoreQueryBuilder constantScoreQueryBuilder = new ConstantScoreQueryBuilder(new MatchNoneQueryBuilder());
        QueryBuilder rewrite = constantScoreQueryBuilder.rewrite(createShardContext());
        assertEquals(rewrite, new MatchNoneQueryBuilder());
    }
}
