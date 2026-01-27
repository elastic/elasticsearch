/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression.FieldValue;

import java.math.BigInteger;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.is;

public class ExpressionModelPredicateTests extends ESTestCase {

    public void testNullValue() throws Exception {
        final Predicate<FieldValue> predicate = ExpressionModel.buildPredicate(null);
        assertThat(predicate.test(new FieldValue(null)), is(true));
        assertThat(predicate.test(new FieldValue("")), is(false));
        assertThat(predicate.test(new FieldValue(1)), is(false));
        assertThat(predicate.test(new FieldValue(true)), is(false));
    }

    public void testBooleanValue() throws Exception {
        final boolean matchValue = randomBoolean();
        final Predicate<FieldValue> predicate = ExpressionModel.buildPredicate(matchValue);
        assertThat(predicate.test(new FieldValue(matchValue)), is(true));
        Object value = matchValue == false;
        assertThat(predicate.test(new FieldValue(value)), is(false));
        assertThat(predicate.test(new FieldValue(String.valueOf(matchValue))), is(false));
        assertThat(predicate.test(new FieldValue("")), is(false));
        assertThat(predicate.test(new FieldValue(1)), is(false));
        assertThat(predicate.test(new FieldValue(null)), is(false));
    }

    public void testLongValue() throws Exception {
        final int intValue = randomInt();
        final long longValue = intValue;
        final Predicate<FieldValue> predicate = ExpressionModel.buildPredicate(longValue);

        assertThat(predicate.test(new FieldValue(longValue)), is(true));
        assertThat(predicate.test(new FieldValue(intValue)), is(true));
        assertThat(predicate.test(new FieldValue(new BigInteger(String.valueOf(longValue)))), is(true));

        assertThat(predicate.test(new FieldValue(longValue - 1)), is(false));
        assertThat(predicate.test(new FieldValue(intValue + 1)), is(false));
        assertThat(predicate.test(new FieldValue(String.valueOf(longValue))), is(false));
        assertThat(predicate.test(new FieldValue("")), is(false));
        assertThat(predicate.test(new FieldValue(true)), is(false));
        assertThat(predicate.test(new FieldValue(null)), is(false));
    }

    public void testSimpleAutomatonValue() throws Exception {
        final String prefix = randomAlphaOfLength(3);
        FieldValue fieldValue = new FieldValue(prefix + "*");

        assertThat(ExpressionModel.buildPredicate(prefix).test(fieldValue), is(true));
        assertThat(ExpressionModel.buildPredicate(prefix + randomAlphaOfLengthBetween(1, 5)).test(fieldValue), is(true));

        assertThat(ExpressionModel.buildPredicate("_" + prefix).test(fieldValue), is(false));
        assertThat(ExpressionModel.buildPredicate(prefix.substring(0, 1)).test(fieldValue), is(false));

        assertThat(ExpressionModel.buildPredicate("").test(fieldValue), is(false));
        assertThat(ExpressionModel.buildPredicate(1).test(fieldValue), is(false));
        assertThat(ExpressionModel.buildPredicate(true).test(fieldValue), is(false));
        assertThat(ExpressionModel.buildPredicate(null).test(fieldValue), is(false));
    }

    public void testEmptyStringValue() throws Exception {
        final Predicate<FieldValue> predicate = ExpressionModel.buildPredicate("");

        assertThat(predicate.test(new FieldValue("")), is(true));

        assertThat(predicate.test(new FieldValue(randomAlphaOfLengthBetween(1, 3))), is(false));
        assertThat(predicate.test(new FieldValue(1)), is(false));
        assertThat(predicate.test(new FieldValue(true)), is(false));
        assertThat(predicate.test(new FieldValue(null)), is(false));
    }

    public void testRegexAutomatonValue() throws Exception {
        final String substring = randomAlphaOfLength(5);
        final FieldValue fieldValue = new FieldValue("/.*" + substring + ".*/");

        assertThat(ExpressionModel.buildPredicate(substring).test(fieldValue), is(true));
        assertThat(
            ExpressionModel.buildPredicate(randomAlphaOfLengthBetween(2, 4) + substring + randomAlphaOfLengthBetween(1, 5))
                .test(fieldValue),
            is(true)
        );

        assertThat(ExpressionModel.buildPredicate(substring.substring(1, 3)).test(fieldValue), is(false));

        assertThat(ExpressionModel.buildPredicate("").test(fieldValue), is(false));
        assertThat(ExpressionModel.buildPredicate(1).test(fieldValue), is(false));
        assertThat(ExpressionModel.buildPredicate(true).test(fieldValue), is(false));
        assertThat(ExpressionModel.buildPredicate(null).test(fieldValue), is(false));
    }

}
