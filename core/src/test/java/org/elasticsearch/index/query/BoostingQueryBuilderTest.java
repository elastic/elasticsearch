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

import org.apache.lucene.queries.BoostingQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;

import java.io.IOException;

public class BoostingQueryBuilderTest extends BaseQueryTestCase<BoostingQueryBuilder> {

    @Override
    protected BoostingQueryBuilder createTestQueryBuilder() {
        BoostingQueryBuilder query = new BoostingQueryBuilder();
        query.positive(RandomQueryBuilder.createQuery(random()));
        query.negative(RandomQueryBuilder.createQuery(random()));
        query.negativeBoost(2.0f / randomIntBetween(1, 20));
        if (randomBoolean()) {
            query.boost(2.0f / randomIntBetween(1, 20));
        }
        return query;
    }

    @Override
    protected Query createExpectedQuery(BoostingQueryBuilder queryBuilder, QueryParseContext context) throws IOException {
        Query positive = queryBuilder.positive().toQuery(context);
        Query negative = queryBuilder.negative().toQuery(context);
        BoostingQuery boostingQuery = new BoostingQuery(positive, negative, queryBuilder.negativeBoost());
        if (queryBuilder.boost() != 1.0f) {
            boostingQuery.setBoost(queryBuilder.boost());
        }
        return boostingQuery;
    }

    /**
     * test that setting a null negative/positive clause renders a parseable query
     */
    @Test
    public void testInnerClauseNull() throws IOException {
        BoostingQueryBuilder boostingQueryBuilder = new BoostingQueryBuilder().negativeBoost(0.1f);
        if (randomBoolean()) {
            boostingQueryBuilder.positive(new MatchAllQueryBuilder());
        } else {
            boostingQueryBuilder.negative(new MatchAllQueryBuilder());
        }
        String contentString = boostingQueryBuilder.toString();
        XContentParser parser = XContentFactory.xContent(contentString).createParser(contentString);
        QueryParseContext context = createContext();
        context.reset(parser);
        assertQueryHeader(parser, boostingQueryBuilder.getName());
        QueryBuilder parsedBuilder = context.indexQueryParserService().queryParser(boostingQueryBuilder.getName()).fromXContent(context);
        assertNotNull(parsedBuilder);
        assertNotSame(parsedBuilder, boostingQueryBuilder);
        assertEquals(parsedBuilder, boostingQueryBuilder);
    }

    /**
     * tests that we signal upstream queries to ignore this query by returning <tt>null</tt>
     * if any of the inner query builder is not set
     */
    @Test
    public void testInnerQueryBuilderNull() throws IOException {
        BoostingQueryBuilder boostingQueryBuilder = new BoostingQueryBuilder();
        if (randomBoolean()) {
            boostingQueryBuilder.positive(new MatchAllQueryBuilder()).negative(null);
        } else {
            boostingQueryBuilder.positive(null).negative(new MatchAllQueryBuilder());
        }
        assertNull(boostingQueryBuilder.toQuery(createContext()));
    }

    @Test
    public void testInnerQueryBuilderReturnsNull() throws IOException {
        QueryBuilder noOpBuilder = new AbstractQueryBuilder<QueryBuilder>() {

            @Override
            public String getName() {
                return "dummy";
            }

            @Override
            protected void doXContent(XContentBuilder builder, Params params) throws IOException {
            }

            @Override
            public Query toQuery(QueryParseContext context) {
                return null;
            }
        };
        BoostingQueryBuilder boostingQueryBuilder;
        if (randomBoolean()) {
            boostingQueryBuilder = new BoostingQueryBuilder().positive(new MatchAllQueryBuilder()).negative(noOpBuilder);
        } else {
            boostingQueryBuilder = new BoostingQueryBuilder().positive(noOpBuilder).negative(new MatchAllQueryBuilder());
        }
        assertNull(boostingQueryBuilder.toQuery(createContext()));
    }

    @Test
    public void testValidate() {
        BoostingQueryBuilder boostingQuery = new BoostingQueryBuilder();
        int totalExpectedErrors = 0;
        if (randomBoolean()) {
            boostingQuery.negative(RandomQueryBuilder.createInvalidQuery(random()));
            totalExpectedErrors++;
        } else if(rarely()) {
            boostingQuery.negative(RandomQueryBuilder.createQuery(random()));
        }
        if (randomBoolean()) {
            boostingQuery.positive(RandomQueryBuilder.createInvalidQuery(random()));
            totalExpectedErrors++;
        } else if(rarely()) {
            boostingQuery.positive(RandomQueryBuilder.createQuery(random()));
        }
        if (frequently()) {
            boostingQuery.negativeBoost(0.5f);
        } else {
            boostingQuery.negativeBoost(-0.5f);
            totalExpectedErrors++;
        }
        assertValidate(boostingQuery, totalExpectedErrors);
    }
}
