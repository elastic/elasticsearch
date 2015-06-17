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
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

@SuppressWarnings("deprecation")
public class FQueryFilterBuilderTest extends BaseQueryTestCase<FQueryFilterBuilder> {

    @Override
    protected Query createExpectedQuery(FQueryFilterBuilder queryBuilder, QueryParseContext context) throws QueryParsingException, IOException {
        return new ConstantScoreQuery(queryBuilder.innerQuery().toQuery(context));
    }

    /**
     * @return a AndQueryBuilder with random limit between 0 and 20
     */
    @Override
    protected FQueryFilterBuilder createTestQueryBuilder() {
        QueryBuilder innerQuery = RandomQueryBuilder.create(random());
        FQueryFilterBuilder testQuery = new FQueryFilterBuilder(innerQuery);
        testQuery.queryName(randomAsciiOfLengthBetween(1, 10));
        return testQuery;
    }

    @Override
    protected void assertLuceneQuery(FQueryFilterBuilder queryBuilder, Query query, QueryParseContext context) {
        Query namedQuery = context.copyNamedFilters().get(queryBuilder.queryName());
        assertThat(namedQuery, equalTo(query));
    }

    /**
     * test corner case where no inner query exist
     */
    @Test
    public void testNoInnerQuery() throws QueryParsingException, IOException {
        FQueryFilterBuilder queryFilterQuery = new FQueryFilterBuilder(null);
        assertNull(queryFilterQuery.toQuery(createContext()));
    }

    /**
     * test wrapping an inner filter that returns null also returns <tt>null</null> to pass on upwards
     */
    @Test
    public void testInnerQueryReturnsNull() throws IOException {
        QueryParseContext context = createContext();

        // create inner filter
        String queryString = "{ \"constant_score\" : { \"filter\" : {} }";
        XContentParser parser = XContentFactory.xContent(queryString).createParser(queryString);
        context.reset(parser);
        assertQueryHeader(parser, ConstantScoreQueryBuilder.PROTOTYPE.getName());
        QueryBuilder innerQuery = context.indexQueryParserService().queryParser(ConstantScoreQueryBuilder.PROTOTYPE.getName()).fromXContent(context);

        // check that when wrapping this filter, toQuery() returns null
        FQueryFilterBuilder queryFilterQuery = new FQueryFilterBuilder(innerQuery);
        assertNull(queryFilterQuery.toQuery(createContext()));
    }
}
