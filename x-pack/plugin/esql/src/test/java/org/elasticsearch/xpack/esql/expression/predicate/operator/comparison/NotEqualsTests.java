/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.ql.tree.Source;
import org.hamcrest.Matcher;

import java.time.ZoneOffset;

import static org.hamcrest.Matchers.equalTo;

public class NotEqualsTests extends AbstractBinaryComparisonTestCase {
    @Override
    protected <T extends Comparable<T>> Matcher<Boolean> resultMatcher(T lhs, T rhs) {
        return equalTo(false == lhs.equals(rhs));
    }

    @Override
    protected String expectedEvaluatorSimpleToString() {
        return "NotEqualsIntsEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]";
    }

    @Override
    protected BinaryComparison build(Source source, Expression lhs, Expression rhs) {
        return new NotEquals(source, lhs, rhs, ZoneOffset.UTC);
    }

    @Override
    protected boolean isEquality() {
        return true;
    }
}
