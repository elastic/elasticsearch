/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.queryableexpression;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.queryableexpression.LongQueryableExpressionTests.randomInterestingLong;
import static org.hamcrest.Matchers.equalTo;

public class IntQueryableExpressionTests extends ESTestCase {
    public void testIntConstantPlusIntConstant() throws IOException {
        int lhs = randomInterestingInt();
        int rhs = randomInterestingInt();
        int result = lhs + rhs;
        logger.info("{} + {} = {}", lhs, rhs, result);

        QueryableExpressionBuilder builder = QueryableExpressionBuilder.add(
            QueryableExpressionBuilder.constant(lhs),
            QueryableExpressionBuilder.constant(rhs)
        );
        QueryableExpression expression = builder.build(null, null);
        assertThat(expression, equalTo(QueryableExpressionBuilder.constant(result).build(null, null)));
        assertThat(expression.toString(), equalTo(Integer.toString(result)));
    }

    public void testIntConstantTimesIntConstant() throws IOException {
        int lhs = randomInterestingInt();
        int rhs = randomInterestingInt();
        int result = lhs * rhs;
        logger.info("{} * {} = {}", lhs, rhs, result);

        QueryableExpressionBuilder builder = QueryableExpressionBuilder.multiply(
            QueryableExpressionBuilder.constant(lhs),
            QueryableExpressionBuilder.constant(rhs)
        );
        QueryableExpression expression = builder.build(null, null);
        assertThat(expression, equalTo(QueryableExpressionBuilder.constant(result).build(null, null)));
        assertThat(expression.toString(), equalTo(Integer.toString(result)));
    }

    public void testIntConstantDividedByIntConstant() throws IOException {
        int lhs = randomInterestingInt();
        int rhs = randomValueOtherThan(0, IntQueryableExpressionTests::randomInterestingInt);
        int result = lhs / rhs;
        logger.info("{} / {} = {}", lhs, rhs, result);

        QueryableExpressionBuilder builder = QueryableExpressionBuilder.divide(
            QueryableExpressionBuilder.constant(lhs),
            QueryableExpressionBuilder.constant(rhs)
        );
        QueryableExpression expression = builder.build(null, null);
        assertThat(expression, equalTo(QueryableExpressionBuilder.constant(result).build(null, null)));
        assertThat(expression.toString(), equalTo(Integer.toString(result)));
    }

    public void testIntConstantPlusLongConstant() throws IOException {
        int lhs = randomInterestingInt();
        long rhs = randomInterestingLong();
        long result = lhs + rhs;
        logger.info("{} + {} = {}", lhs, rhs, result);

        QueryableExpressionBuilder builder = QueryableExpressionBuilder.add(
            QueryableExpressionBuilder.constant(lhs),
            QueryableExpressionBuilder.constant(rhs)
        );
        QueryableExpression expression = builder.build(null, null);
        assertThat(expression, equalTo(QueryableExpressionBuilder.constant(result).build(null, null)));
        assertThat(expression.toString(), equalTo(Long.toString(result)));
    }

    public void testIntConstantTimesLongConstant() throws IOException {
        int lhs = randomInterestingInt();
        long rhs = randomInterestingLong();
        long result = lhs * rhs;
        logger.info("{} * {} = {}", lhs, rhs, result);

        QueryableExpressionBuilder builder = QueryableExpressionBuilder.multiply(
            QueryableExpressionBuilder.constant(lhs),
            QueryableExpressionBuilder.constant(rhs)
        );
        QueryableExpression expression = builder.build(null, null);
        assertThat(expression, equalTo(QueryableExpressionBuilder.constant(result).build(null, null)));
        assertThat(expression.toString(), equalTo(Long.toString(result)));
    }

    public void testIntConstantDividedByLongConstant() throws IOException {
        int lhs = randomInterestingInt();
        long rhs = randomValueOtherThan(0L, LongQueryableExpressionTests::randomInterestingLong);
        long result = lhs / rhs;
        logger.info("{} / {} = {}", lhs, rhs, result);

        QueryableExpressionBuilder builder = QueryableExpressionBuilder.divide(
            QueryableExpressionBuilder.constant(lhs),
            QueryableExpressionBuilder.constant(rhs)
        );
        QueryableExpression expression = builder.build(null, null);
        assertThat(expression, equalTo(QueryableExpressionBuilder.constant(result).build(null, null)));
        assertThat(expression.toString(), equalTo(Long.toString(result)));
    }

    static int randomInterestingInt() {
        switch (between(0, 4)) {
            case 0:
                return 0;
            case 1:
                return randomBoolean() ? 1 : -1;
            case 2:
                return randomShort(); // Even multiplication won't overflow
            case 3:
                return randomInt();
            case 4:
                return randomBoolean() ? Integer.MIN_VALUE : Integer.MAX_VALUE;
            default:
                throw new IllegalArgumentException("Unsupported case");
        }
    }
}
