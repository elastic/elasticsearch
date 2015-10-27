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

import java.io.IOException;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;

public class BoostingQueryBuilderTests extends AbstractQueryTestCase<BoostingQueryBuilder> {

    @Override
    protected BoostingQueryBuilder doCreateTestQueryBuilder() {
        BoostingQueryBuilder query = new BoostingQueryBuilder(RandomQueryBuilder.createQuery(random()), RandomQueryBuilder.createQuery(random()));
        query.negativeBoost(2.0f / randomIntBetween(1, 20));
        return query;
    }

    @Override
    protected void doAssertLuceneQuery(BoostingQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        Query positive = queryBuilder.positiveQuery().toQuery(context);
        Query negative = queryBuilder.negativeQuery().toQuery(context);
        if (positive == null || negative == null) {
            assertThat(query, nullValue());
        } else {
            assertThat(query, instanceOf(BoostingQuery.class));
        }
    }

    public void testIllegalArguments() {
        try {
            new BoostingQueryBuilder(null, new MatchAllQueryBuilder());
            fail("must not be null");
        } catch (IllegalArgumentException e) {
            //
        }

        try {
            new BoostingQueryBuilder(new MatchAllQueryBuilder(), null);
            fail("must not be null");
        } catch (IllegalArgumentException e) {
            //
        }

        try {
            new BoostingQueryBuilder(new MatchAllQueryBuilder(), new MatchAllQueryBuilder()).negativeBoost(-1.0f);
            fail("must not be negative");
        } catch (IllegalArgumentException e) {
            //
        }
    }
}
