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

import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class NotQueryBuilderTest extends BaseQueryTestCase<NotQueryBuilder> {

    @Override
    protected Query createExpectedQuery(NotQueryBuilder queryBuilder, QueryParseContext context) throws QueryParsingException, IOException {
        if (queryBuilder.filter() == null) {
            return null;
        }
        return Queries.not(queryBuilder.filter().toQuery(context));
    }

    /**
     * @return a NotQueryBuilder with random limit between 0 and 20
     */
    @Override
    protected NotQueryBuilder createTestQueryBuilder() {
        NotQueryBuilder query = new NotQueryBuilder(RandomQueryBuilder.createQuery(random()));
        if (randomBoolean()) {
            query.queryName(randomAsciiOfLengthBetween(1, 10));
        }
        return query;
    }

    @Override
    protected void assertLuceneQuery(NotQueryBuilder queryBuilder, Query query, QueryParseContext context) {
        if (queryBuilder.queryName() != null) {
            Query namedQuery = context.copyNamedFilters().get(queryBuilder.queryName());
            assertThat(namedQuery, equalTo(query));
        }
    }

    /**
     * test corner case where no inner query exist
     */
    @Test
    public void testNoInnerQuerry() throws QueryParsingException, IOException {
        NotQueryBuilder notQuery = new NotQueryBuilder(null);
        assertNull(notQuery.toQuery(createContext()));
    }

    /**
     * Test corner case where inner queries returns null.
     * This is legal, e.g. here {@link ConstantScoreQueryBuilder} can wrap a filter
     * element that itself parses to <tt>null</tt>, e.g.
     * '{ "constant_score" : "filter {} }'
     */
    @Test
    public void testInnerQuery() throws QueryParsingException, IOException {
        NotQueryBuilder notQuery = new NotQueryBuilder(new ConstantScoreQueryBuilder(null));
        assertNull(notQuery.toQuery(createContext()));
    }

    /**
     * @throws IOException
     */
    @Test(expected=QueryParsingException.class)
    public void testMissingFilterSection() throws IOException {
        QueryParseContext context = createContext();
        String queryString = "{ \"not\" : {}";
        XContentParser parser = XContentFactory.xContent(queryString).createParser(queryString);
        context.reset(parser);
        assertQueryHeader(parser, NotQueryBuilder.PROTOTYPE.getName());
        context.indexQueryParserService().queryParser(NotQueryBuilder.PROTOTYPE.getName()).fromXContent(context);
    }

    @Test
    public void testValidate() {
        QueryBuilder innerQuery;
        int totalExpectedErrors = 0;
        if (randomBoolean()) {
            innerQuery = RandomQueryBuilder.createInvalidQuery(random());
            totalExpectedErrors++;
        } else {
            innerQuery = RandomQueryBuilder.createQuery(random());
        }
        NotQueryBuilder notQuery = new NotQueryBuilder(innerQuery);
        assertValidate(notQuery, totalExpectedErrors);
    }
}
