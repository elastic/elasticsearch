/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantTextAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;

import java.util.Arrays;

public class SignificantTextTests extends BaseAggregationTestCase<SignificantTextAggregationBuilder> {

    @Override
    protected SignificantTextAggregationBuilder createTestAggregatorBuilder() {
        String name = randomAlphaOfLengthBetween(3, 20);
        String field = randomAlphaOfLengthBetween(3, 20);
        SignificantTextAggregationBuilder factory = new SignificantTextAggregationBuilder(name, field);
        if (randomBoolean()) {
            factory.bucketCountThresholds().setRequiredSize(randomIntBetween(1, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            factory.sourceFieldNames(Arrays.asList(new String[] { "foo", "bar" }));
        }

        if (randomBoolean()) {
            factory.bucketCountThresholds().setShardSize(randomIntBetween(1, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            int minDocCount = randomInt(4);
            switch (minDocCount) {
                case 0:
                    break;
                case 1:
                case 2:
                case 3:
                case 4:
                    minDocCount = randomIntBetween(0, Integer.MAX_VALUE);
                    break;
            }
            factory.bucketCountThresholds().setMinDocCount(minDocCount);
        }
        if (randomBoolean()) {
            int shardMinDocCount = randomInt(4);
            switch (shardMinDocCount) {
                case 0:
                    break;
                case 1:
                case 2:
                case 3:
                case 4:
                    shardMinDocCount = randomIntBetween(0, Integer.MAX_VALUE);
                    break;
                default:
                    fail();
            }
            factory.bucketCountThresholds().setShardMinDocCount(shardMinDocCount);
        }

        factory.filterDuplicateText(randomBoolean());

        if (randomBoolean()) {
            IncludeExclude incExc = SignificantTermsTests.getIncludeExclude();
            factory.includeExclude(incExc);
        }
        if (randomBoolean()) {
            SignificanceHeuristic significanceHeuristic = SignificantTermsTests.getSignificanceHeuristic();
            factory.significanceHeuristic(significanceHeuristic);
        }
        if (randomBoolean()) {
            factory.backgroundFilter(QueryBuilders.termsQuery("foo", "bar"));
        }
        return factory;
    }

}
