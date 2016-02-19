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

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregator.KeyedFilter;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregatorBuilder;

public class FiltersTests extends BaseAggregationTestCase<FiltersAggregatorBuilder> {

    @Override
    protected FiltersAggregatorBuilder createTestAggregatorBuilder() {

        int size = randomIntBetween(1, 20);
        FiltersAggregatorBuilder factory;
        if (randomBoolean()) {
            KeyedFilter[] filters = new KeyedFilter[size];
            for (int i = 0; i < size; i++) {
                // NORELEASE make RandomQueryBuilder work outside of the
                // AbstractQueryTestCase
                // builder.query(RandomQueryBuilder.createQuery(getRandom()));
                filters[i] = new KeyedFilter(randomAsciiOfLengthBetween(1, 20),
                        QueryBuilders.termQuery(randomAsciiOfLengthBetween(5, 20), randomAsciiOfLengthBetween(5, 20)));
            }
            factory = new FiltersAggregatorBuilder(randomAsciiOfLengthBetween(1, 20), filters);
        } else {
            QueryBuilder<?>[] filters = new QueryBuilder<?>[size];
            for (int i = 0; i < size; i++) {
                // NORELEASE make RandomQueryBuilder work outside of the
                // AbstractQueryTestCase
                // builder.query(RandomQueryBuilder.createQuery(getRandom()));
                filters[i] = QueryBuilders.termQuery(randomAsciiOfLengthBetween(5, 20), randomAsciiOfLengthBetween(5, 20));
            }
            factory = new FiltersAggregatorBuilder(randomAsciiOfLengthBetween(1, 20), filters);
        }
        if (randomBoolean()) {
            factory.otherBucket(randomBoolean());
        }
        if (randomBoolean()) {
            factory.otherBucketKey(randomAsciiOfLengthBetween(1, 20));
        }
        return factory;
    }

}
