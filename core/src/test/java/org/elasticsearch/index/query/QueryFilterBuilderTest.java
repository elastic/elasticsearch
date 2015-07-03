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
import org.junit.Test;

import java.io.IOException;

@SuppressWarnings("deprecation")
public class QueryFilterBuilderTest extends BaseQueryTestCase<QueryFilterBuilder> {

    @Override
    protected Query doCreateExpectedQuery(QueryFilterBuilder queryBuilder, QueryParseContext context) throws QueryParsingException, IOException {
        Query query = queryBuilder.innerQuery().toQuery(context);
        return query != null ? new ConstantScoreQuery(query) : query;
    }

    /**
     * @return a QueryFilterBuilder with random inner query
     */
    @Override
    protected QueryFilterBuilder doCreateTestQueryBuilder() {
        QueryBuilder innerQuery = RandomQueryBuilder.createQuery(random());
        return new QueryFilterBuilder(innerQuery);
    }

    @Test(expected=NullPointerException.class)
    public void testQueryFilterBuilderNull() {
        new QueryFilterBuilder(null);
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
        QueryFilterBuilder fQueryFilter = new QueryFilterBuilder(innerQuery);
        assertValidate(fQueryFilter, totalExpectedErrors);
    }
}
