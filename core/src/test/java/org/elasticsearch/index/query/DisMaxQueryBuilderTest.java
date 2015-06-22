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

import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;

import java.io.IOException;

public class DisMaxQueryBuilderTest extends BaseQueryTestCase<DisMaxQueryBuilder> {

    @Override
    protected Query createExpectedQuery(DisMaxQueryBuilder testBuilder, QueryParseContext context) throws QueryParsingException, IOException {
        Query query = new DisjunctionMaxQuery(AbstractQueryBuilder.toQueries(testBuilder.queries(), context), testBuilder.tieBreaker());
        query.setBoost(testBuilder.boost());
        if (testBuilder.queryName() != null) {
            context.addNamedQuery(testBuilder.queryName(), query);
        }
        return query;
    }

    /**
     * @return a {@link DisMaxQueryBuilder} with random inner queries
     */
    @Override
    protected DisMaxQueryBuilder createTestQueryBuilder() {
        DisMaxQueryBuilder dismax = new DisMaxQueryBuilder();
        int clauses = randomIntBetween(1, 5);
        for (int i = 0; i < clauses; i++) {
            dismax.add(RandomQueryBuilder.create(random()));
        }
        if (randomBoolean()) {
            dismax.boost(2.0f / randomIntBetween(1, 20));
        }
        if (randomBoolean()) {
            dismax.tieBreaker(2.0f / randomIntBetween(1, 20));
        }
        if (randomBoolean()) {
            dismax.queryName(randomUnicodeOfLengthBetween(3, 15));
        }
        return dismax;
    }

    /**
     * test `null`return value for missing inner queries
     * @throws IOException
     * @throws QueryParsingException
     */
    @Test
    public void testNoInnerQueries() throws QueryParsingException, IOException {
        DisMaxQueryBuilder disMaxBuilder = new DisMaxQueryBuilder();
        assertNull(disMaxBuilder.toQuery(createContext()));
        assertNull(disMaxBuilder.validate());
    }

    /**
     * Test inner query parsing to null. Current DSL allows inner filter element to parse to <tt>null</tt>.
     * Those should be ignored upstream. To test this, we use inner {@link ConstantScoreQueryBuilder}
     * with empty inner filter.
     */
    @Test
    public void testInnerQueryReturnsNull() throws IOException {
        QueryParseContext context = createContext();
        String queryId = ConstantScoreQueryBuilder.PROTOTYPE.getName();
        String queryString = "{ \""+queryId+"\" : { \"filter\" : { } }";
        XContentParser parser = XContentFactory.xContent(queryString).createParser(queryString);
        context.reset(parser);
        assertQueryHeader(parser, queryId);
        ConstantScoreQueryBuilder innerQueryBuilder = (ConstantScoreQueryBuilder) context.indexQueryParserService()
                .queryParser(queryId).fromXContent(context);

        DisMaxQueryBuilder disMaxBuilder = new DisMaxQueryBuilder().add(innerQueryBuilder);
        assertNull(disMaxBuilder.toQuery(context));
    }
}
