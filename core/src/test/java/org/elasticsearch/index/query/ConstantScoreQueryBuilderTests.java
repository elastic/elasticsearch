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
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
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
    protected void doAssertLuceneQuery(ConstantScoreQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        Query innerQuery = queryBuilder.innerQuery().toQuery(context);
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
        try {
            parseQuery(queryString);
            fail("Expected ParsingException");
        } catch (ParsingException e) {
            assertThat(e.getMessage(), containsString("requires a 'filter' element"));
        }
    }

    /**
     * test that multiple "filter" elements causes {@link ParsingException}
     */
    public void testMultipleFilterElements() throws IOException {
        String queryString = "{ \"" + ConstantScoreQueryBuilder.NAME + "\" : {\n" +
                                    "\"filter\" : { \"term\": { \"foo\": \"a\" } },\n" +
                                    "\"filter\" : { \"term\": { \"foo\": \"x\" } },\n" +
                            "} }";
        try {
            parseQuery(queryString);
            fail("Expected ParsingException");
        } catch (ParsingException e) {
            assertThat(e.getMessage(), containsString("accepts only one 'filter' element"));
        }
    }

    /**
     * test that "filter" does not accept an array of queries, throws {@link ParsingException}
     */
    public void testNoArrayAsFilterElements() throws IOException {
        String queryString = "{ \"" + ConstantScoreQueryBuilder.NAME + "\" : {\n" +
                                    "\"filter\" : [ { \"term\": { \"foo\": \"a\" } },\n" +
                                                   "{ \"term\": { \"foo\": \"x\" } } ]\n" +
                            "} }";
        try {
            parseQuery(queryString);
            fail("Expected ParsingException");
        } catch (ParsingException e) {
            assertThat(e.getMessage(), containsString("unexpected token [START_ARRAY]"));
        }
    }

    public void testIllegalArguments() {
        expectThrows(IllegalArgumentException.class, () -> new ConstantScoreQueryBuilder((QueryBuilder) null));
    }

    @Override
    public void testUnknownField() throws IOException {
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

    /**
     * we bubble up empty query bodies as an empty optional
     */
    public void testFromJsonEmptyQueryBody() throws IOException {
        String query =
                "{ \"constant_score\" : {" +
                "    \"filter\" : { }" +
                "  }" +
                "}";
        XContentParser parser = XContentFactory.xContent(query).createParser(query);
        QueryParseContext context = createParseContext(parser, ParseFieldMatcher.EMPTY);
        Optional<QueryBuilder> innerQueryBuilder = context.parseInnerQueryBuilder();
        assertTrue(innerQueryBuilder.isPresent() == false);

        parser = XContentFactory.xContent(query).createParser(query);
        QueryParseContext otherContext = createParseContext(parser, ParseFieldMatcher.STRICT);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> otherContext.parseInnerQueryBuilder());
        assertThat(ex.getMessage(), startsWith("query malformed, empty clause found at"));
    }

}
