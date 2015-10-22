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

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;

@SuppressWarnings("deprecation")
public class QueryFilterBuilderTests extends AbstractQueryTestCase<QueryFilterBuilder> {

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
     * test that wrapping an inner filter that returns <tt>null</tt> also returns <tt>null</tt> to pass on upwards
     */
    public void testInnerQueryReturnsNull() throws IOException {
        // create inner filter
        String queryString = "{ \"constant_score\" : { \"filter\" : {} } }";
        QueryBuilder<?> innerQuery = parseQuery(queryString);
        // check that when wrapping this filter, toQuery() returns null
        QueryFilterBuilder queryFilterQuery = new QueryFilterBuilder(innerQuery);
        assertNull(queryFilterQuery.toQuery(createShardContext()));
    }

    public void testValidate() {
        try {
            new QueryFilterBuilder(null);
            fail("cannot be null");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }
}
