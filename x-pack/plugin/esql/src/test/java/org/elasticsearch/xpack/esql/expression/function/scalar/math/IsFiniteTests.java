/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.hamcrest.Matcher;

import static org.hamcrest.Matchers.equalTo;

public class IsFiniteTests extends AbstractRationalUnaryPredicateTests {
    @Override
    protected RationalUnaryPredicate build(Source source, Expression value) {
        return new IsFinite(source, value);
    }

    @Override
    protected Matcher<Object> resultMatcher(double d) {
        return equalTo(Double.isNaN(d) == false && Double.isInfinite(d) == false);
    }

    @Override
    protected String expectedEvaluatorSimpleToString() {
        return "IsFiniteEvaluator[val=Attribute[channel=0]]";
    }
}
