/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.BaseAggregationTestCase;

import java.util.List;

public class CardinalityTests extends BaseAggregationTestCase<CardinalityAggregationBuilder> {

    @Override
    protected final CardinalityAggregationBuilder createTestAggregatorBuilder() {
        CardinalityAggregationBuilder factory = new CardinalityAggregationBuilder(randomAlphaOfLengthBetween(3, 10));
        String field = randomNumericField();
        randomFieldOrScript(factory, field);
        if (randomBoolean()) {
            factory.missing("MISSING");
        }
        if (randomBoolean()) {
            factory.executionHint(
                randomFrom(List.of("segment_ordinals", "global_ordinals", "direct", "save_time_heuristic", "save_memory_heuristic"))
            );
        }
        return factory;
    }

}
