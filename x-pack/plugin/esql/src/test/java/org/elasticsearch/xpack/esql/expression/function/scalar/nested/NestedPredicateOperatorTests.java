/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import java.util.List;

/** Evaluates the predicate by reading each parent's children. */
public class NestedPredicateOperatorTests extends NestedPredicateOperatorTestCase {
    @Override
    NestedPredicate predicate() {
        return nested(priceAboveThreshold(), List.of(), List.of(), null);
    }

    @Override
    boolean matches(int parent) {
        return highestPrice(parent) > THRESHOLD;
    }
}
