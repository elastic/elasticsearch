/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.missing.MissingAggregationBuilder;

public class MissingTests extends BaseAggregationTestCase<MissingAggregationBuilder> {

    @Override
    protected final MissingAggregationBuilder createTestAggregatorBuilder() {
        MissingAggregationBuilder factory = new MissingAggregationBuilder(randomAlphaOfLengthBetween(3, 10));
        String field = randomNumericField();
        randomFieldOrScript(factory, field);
        return factory;
    }

}
