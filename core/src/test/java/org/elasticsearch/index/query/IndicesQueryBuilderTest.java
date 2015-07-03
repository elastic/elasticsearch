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
import org.junit.Test;

import java.io.IOException;

public class IndicesQueryBuilderTest extends BaseQueryTestCase<IndicesQueryBuilder> {

    @Override
    protected IndicesQueryBuilder doCreateTestQueryBuilder() {
        String[] indices;
        if (randomBoolean()) {
            indices = new String[]{getIndex().getName()};
        } else {
            indices = generateRandomStringArray(5, 10, false, false);
        }
        IndicesQueryBuilder query = new IndicesQueryBuilder(RandomQueryBuilder.createQuery(random()), indices);

        switch (randomInt(2)) {
            case 0:
                query.noMatchQuery(RandomQueryBuilder.createQuery(random()));
                break;
            case 1:
                query.noMatchQuery(randomFrom(QueryBuilders.matchAllQuery(), new MatchNoneQueryBuilder()));
                break;
            default:
                // do not set noMatchQuery
        }
        return query;
    }

    @Override
    protected void doAssertLuceneQuery(IndicesQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        Query expected;
        if (queryBuilder.indices().length == 1 && getIndex().getName().equals(queryBuilder.indices()[0])) {
            expected = queryBuilder.innerQuery().toQuery(context);
        } else {
            expected = queryBuilder.noMatchQuery().toQuery(context);
        }
        if (expected != null) {
            expected.setBoost(queryBuilder.boost());
        }
        assertEquals(query, expected);
    }

    @Test
    public void testValidate() {
        int expectedErrors = 0;

        // inner query
        QueryBuilder innerQuery;
        if (randomBoolean()) {
            // setting innerQuery to null would be caught in the builder already and make validation fail
            innerQuery = RandomQueryBuilder.createInvalidQuery(random());
            expectedErrors++;
        } else {
            innerQuery = RandomQueryBuilder.createQuery(random());
        }
        // indices
        String[] indices;
        if (randomBoolean()) {
            indices = randomBoolean() ? null : new String[0];
            expectedErrors++;
        } else {
            indices = new String[]{"index"};
        }
        // no match query
        QueryBuilder noMatchQuery;
        if (randomBoolean()) {
            noMatchQuery = RandomQueryBuilder.createInvalidQuery(random());
            expectedErrors++;
        } else {
            noMatchQuery = RandomQueryBuilder.createQuery(random());
        }

        assertValidate(new IndicesQueryBuilder(innerQuery, indices).noMatchQuery(noMatchQuery), expectedErrors);
    }
}
