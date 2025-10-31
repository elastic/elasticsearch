/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser.promql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.time.Duration;

import static org.elasticsearch.xpack.esql.parser.promql.ArithmeticOperation.ADD;
import static org.elasticsearch.xpack.esql.parser.promql.ArithmeticOperation.DIV;
import static org.elasticsearch.xpack.esql.parser.promql.ArithmeticOperation.MOD;
import static org.elasticsearch.xpack.esql.parser.promql.ArithmeticOperation.MUL;
import static org.elasticsearch.xpack.esql.parser.promql.ArithmeticOperation.POW;
import static org.elasticsearch.xpack.esql.parser.promql.ArithmeticOperation.SUB;
import static org.hamcrest.Matchers.containsString;

public class PromqlArithmeticUtilsTests extends ESTestCase {

    private static final Source SOURCE = new Source(0, 0, "test");

    private static Duration sec(int seconds) {
        return Duration.ofSeconds(seconds);
    }

    // Utility method for compact one-liner tests
    private void evaluate(Object left, ArithmeticOperation op, Object right, Object expected) {
        Object result = PromqlArithmeticUtils.evaluate(SOURCE, left, right, op);
        assertEquals(expected, result);
    }

    private void error(Object left, ArithmeticOperation op, Object right, String errorMessage) {
        ParsingException exception = expectThrows(ParsingException.class, () -> PromqlArithmeticUtils.evaluate(SOURCE, left, right, op));
        assertThat(exception.getErrorMessage(), containsString(errorMessage));
    }

    // Number op Number tests
    public void testNumberAddition() {
        evaluate(5, ADD, 3, 8);
        evaluate(5L, ADD, 3L, 8L);
        evaluate(5.5, ADD, 3.5, 9.0);
    }

    public void testNumberSubtraction() {
        evaluate(5, SUB, 3, 2);
        evaluate(5L, SUB, 3L, 2L);
        evaluate(5.5, SUB, 3.5, 2.0);
    }

    public void testNumberMultiplication() {
        evaluate(5, MUL, 3, 15);
        evaluate(5L, MUL, 3L, 15L);
        evaluate(5.5, MUL, 2.0, 11.0);
    }

    public void testNumberDivision() {
        evaluate(10, DIV, 2, 5);
        evaluate(10L, DIV, 2L, 5L);
        evaluate(10.0, DIV, 2.0, 5.0);
    }

    public void testNumberModulo() {
        evaluate(10, MOD, 3, 1);
        evaluate(10L, MOD, 3L, 1L);
    }

    public void testNumberPower() {
        evaluate(2, POW, 3, 8);  // integer result
        evaluate(2.5, POW, 2.0, 6.25);  // double result
    }

    // Duration op Duration tests
    public void testDurationAddition() {
        evaluate(sec(60), ADD, sec(30), sec(90));
    }

    public void testDurationSubtraction() {
        evaluate(sec(60), SUB, sec(30), sec(30));
    }

    public void testDurationInvalidOperations() {
        error(sec(60), MUL, sec(30), "not supported between two durations");
        error(sec(60), DIV, sec(30), "not supported between two durations");
    }

    // Duration op Number tests (Number interpreted as seconds for ADD/SUB, dimensionless for MUL/DIV/MOD/POW)
    public void testDurationAddNumber() {
        evaluate(sec(60), ADD, 30, sec(90));
        evaluate(sec(60), ADD, 30.0, sec(90));
    }

    public void testDurationSubNumber() {
        evaluate(sec(60), SUB, 30, sec(30));
        evaluate(sec(60), SUB, 30.0, sec(30));
    }

    public void testDurationMulNumber() {
        evaluate(sec(60), MUL, 2, sec(120));
        evaluate(sec(60), MUL, 2.5, sec(150));
    }

    public void testDurationDivNumber() {
        evaluate(sec(60), DIV, 2, sec(30));
        evaluate(sec(60), DIV, 2.0, sec(30));
    }

    public void testDurationModNumber() {
        evaluate(sec(65), MOD, 60, sec(5));
    }

    public void testDurationPowNumber() {
        evaluate(sec(2), POW, 3, sec(8));
    }

    public void testDurationDivByZero() {
        error(sec(60), DIV, 0, "Cannot divide duration by zero");
    }

    public void testDurationModByZero() {
        error(sec(60), MOD, 0, "Cannot compute modulo with zero");
    }

    public void testNumberMulDuration() {
        evaluate(2, MUL, sec(60), sec(120));
        evaluate(2.5, MUL, sec(60), sec(150));
        evaluate(2, ADD, sec(60), sec(62));
        evaluate(60, SUB, sec(2), sec(58));
    }

    public void testNumberInvalidDurationOperations() {
        error(2, DIV, sec(60), "not supported with scalar on left");
    }

    // Validation tests
    public void testNegativeDuration() {
        evaluate(sec(30), SUB, sec(60), sec(-30));
        evaluate(sec(60), SUB, 90, sec(-30));
    }
}
