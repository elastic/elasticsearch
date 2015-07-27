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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;

@SuppressWarnings("deprecation")
public class QueryFilterBuilderTest extends BaseQueryTestCase<QueryFilterBuilder> {

    @Override
    protected QueryFilterBuilder doCreateTestQueryBuilder() {
        QueryBuilder innerQuery = RandomQueryBuilder.createQuery(random());
        return new QueryFilterBuilder(innerQuery);
    }

    @Override
    protected void doAssertLuceneQuery(QueryFilterBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        Query innerQuery = queryBuilder.innerQuery().toQuery(context);
        if (innerQuery == null) {
            assertThat(query, nullValue());
        } else {
            assertThat(query, instanceOf(ConstantScoreQuery.class));
            ConstantScoreQuery constantScoreQuery = (ConstantScoreQuery) query;
            assertThat(constantScoreQuery.getQuery(), equalTo(innerQuery));
        }
    }

    @Override
    protected boolean supportsBoostAndQueryName() {
        return false;
    }

    /**
     * test wrapping an inner filter that returns null also returns <tt>null</null> to pass on upwards
     */
    @Test
    public void testInnerQueryReturnsNull() throws IOException {
        QueryParseContext context = createParseContext();

        // create inner filter
        String queryString = "{ \"constant_score\" : { \"filter\" : {} }";
        XContentParser parser = XContentFactory.xContent(queryString).createParser(queryString);
        context.reset(parser);
        assertQueryHeader(parser, ConstantScoreQueryBuilder.PROTOTYPE.getName());
        QueryBuilder innerQuery = context.queryParser(ConstantScoreQueryBuilder.PROTOTYPE.getName()).fromXContent(context);

        // check that when wrapping this filter, toQuery() returns null
        QueryFilterBuilder queryFilterQuery = new QueryFilterBuilder(innerQuery);
        assertNull(queryFilterQuery.toQuery(createShardContext()));
    }

    @Test
    public void testValidate() {
        QueryBuilder innerQuery = null;
        int totalExpectedErrors = 0;
        if (randomBoolean()) {
            if (randomBoolean()) {
                innerQuery = RandomQueryBuilder.createInvalidQuery(random());
            }
            totalExpectedErrors++;
        } else {
            innerQuery = RandomQueryBuilder.createQuery(random());
        }
        QueryFilterBuilder fQueryFilter = new QueryFilterBuilder(innerQuery);
        assertValidate(fQueryFilter, totalExpectedErrors);
    }
}
