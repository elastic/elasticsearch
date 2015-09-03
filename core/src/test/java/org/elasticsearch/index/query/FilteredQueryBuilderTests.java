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

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;

@SuppressWarnings("deprecation")
public class FilteredQueryBuilderTests extends BaseQueryTestCase<FilteredQueryBuilder> {

    @Override
    protected FilteredQueryBuilder doCreateTestQueryBuilder() {
        QueryBuilder queryBuilder = RandomQueryBuilder.createQuery(random());
        QueryBuilder filterBuilder = RandomQueryBuilder.createQuery(random());
        return new FilteredQueryBuilder(queryBuilder, filterBuilder);
    }

    @Override
    protected void doAssertLuceneQuery(FilteredQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        Query innerQuery = queryBuilder.innerQuery().toQuery(context);
        if (innerQuery == null) {
            assertThat(query, nullValue());
        } else {
            Query innerFilter = queryBuilder.innerFilter().toQuery(context);
            if (innerFilter == null || Queries.isConstantMatchAllQuery(innerFilter)) {
                innerQuery.setBoost(queryBuilder.boost());
                assertThat(query, equalTo(innerQuery));
            } else if (Queries.isConstantMatchAllQuery(innerQuery)) {
                assertThat(query, instanceOf(ConstantScoreQuery.class));
                assertThat(((ConstantScoreQuery)query).getQuery(), equalTo(innerFilter));
            } else {
                assertThat(query, instanceOf(BooleanQuery.class));
                BooleanQuery booleanQuery = (BooleanQuery) query;
                assertThat(booleanQuery.clauses().size(), equalTo(2));
                assertThat(booleanQuery.clauses().get(0).getOccur(), equalTo(BooleanClause.Occur.MUST));
                assertThat(booleanQuery.clauses().get(0).getQuery(), equalTo(innerQuery));
                assertThat(booleanQuery.clauses().get(1).getOccur(), equalTo(BooleanClause.Occur.FILTER));
                assertThat(booleanQuery.clauses().get(1).getQuery(), equalTo(innerFilter));
            }
        }
    }

    @Test
    public void testValidation() {
        QueryBuilder valid = RandomQueryBuilder.createQuery(random());
        QueryBuilder invalid = RandomQueryBuilder.createInvalidQuery(random());

        // invalid cases
        FilteredQueryBuilder qb = new FilteredQueryBuilder(invalid);
        QueryValidationException result = qb.validate();
        assertNotNull(result);
        assertEquals(1, result.validationErrors().size());

        qb = new FilteredQueryBuilder(valid, invalid);
        result = qb.validate();
        assertNotNull(result);
        assertEquals(1, result.validationErrors().size());

        qb = new FilteredQueryBuilder(invalid, valid);
        result = qb.validate();
        assertNotNull(result);
        assertEquals(1, result.validationErrors().size());

        qb = new FilteredQueryBuilder(invalid, invalid);
        result = qb.validate();
        assertNotNull(result);
        assertEquals(2, result.validationErrors().size());

        // valid cases
        qb = new FilteredQueryBuilder(valid);
        assertNull(qb.validate());

        qb = new FilteredQueryBuilder(null);
        assertNull(qb.validate());

        qb = new FilteredQueryBuilder(null, valid);
        assertNull(qb.validate());

        qb = new FilteredQueryBuilder(valid, null);
        assertNull(qb.validate());

        qb = new FilteredQueryBuilder(valid, valid);
        assertNull(qb.validate());
    }

}
