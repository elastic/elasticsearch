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
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;

public class FilterTests extends BaseAggregationTestCase<FilterAggregationBuilder> {

    @Override
    protected FilterAggregationBuilder createTestAggregatorBuilder() {
        return new FilterAggregationBuilder(
            randomAlphaOfLengthBetween(1, 20),
            QueryBuilders.termQuery(randomAlphaOfLengthBetween(5, 20), randomAlphaOfLengthBetween(5, 20))
        );
    }

}
