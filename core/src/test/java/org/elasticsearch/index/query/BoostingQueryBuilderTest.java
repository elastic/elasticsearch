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
import org.junit.Test;

import java.io.IOException;

public class BoostingQueryBuilderTest extends BaseQueryTestCase<BoostingQueryBuilder> {

    @Override
    protected BoostingQueryBuilder doCreateTestQueryBuilder() {
        BoostingQueryBuilder query = new BoostingQueryBuilder(RandomQueryBuilder.createQuery(random()), RandomQueryBuilder.createQuery(random()));
        query.negativeBoost(2.0f / randomIntBetween(1, 20));
        return query;
    }

    @Override
    protected Query doCreateExpectedQuery(BoostingQueryBuilder queryBuilder, QueryParseContext context) throws IOException {
        Query positive = queryBuilder.positive().toQuery(context);
        Query negative = queryBuilder.negative().toQuery(context);
        if (positive == null || negative == null) {
            return null;
        }
        return new BoostingQuery(positive, negative, queryBuilder.negativeBoost());
    }

    @Test
    public void testValidate() {
        int totalExpectedErrors = 0;
        QueryBuilder positive;
        QueryBuilder negative;
        if (frequently()) {
            negative = RandomQueryBuilder.createInvalidQuery(random());
            totalExpectedErrors++;
        } else {
            negative = RandomQueryBuilder.createQuery(random());
        }
        if (frequently()) {
            positive = RandomQueryBuilder.createInvalidQuery(random());
            totalExpectedErrors++;
        } else {
            positive = RandomQueryBuilder.createQuery(random());
        }
        BoostingQueryBuilder boostingQuery = new BoostingQueryBuilder(positive, negative);
        if (frequently()) {
            boostingQuery.negativeBoost(0.5f);
        } else {
            boostingQuery.negativeBoost(-0.5f);
            totalExpectedErrors++;
        }
        assertValidate(boostingQuery, totalExpectedErrors);
    }

    @Test(expected=NullPointerException.class)
    public void testNullConstructorArgument() {
        if (randomBoolean()) {
            new BoostingQueryBuilder(null, RandomQueryBuilder.createQuery(random()));
        } else {
            new BoostingQueryBuilder(RandomQueryBuilder.createQuery(random()), null);
        }
    }
}
