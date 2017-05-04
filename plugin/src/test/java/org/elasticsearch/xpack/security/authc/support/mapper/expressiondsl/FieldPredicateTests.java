/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support.mapper.expressiondsl;

import java.math.BigInteger;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.authc.support.mapper.expressiondsl.FieldExpression.FieldPredicate;

import static org.hamcrest.Matchers.is;

public class FieldPredicateTests extends ESTestCase {

    public void testNullValue() throws Exception {
        final FieldPredicate predicate = FieldPredicate.create(null);
        assertThat(predicate.test(null), is(true));
        assertThat(predicate.test(""), is(false));
        assertThat(predicate.test(1), is(false));
        assertThat(predicate.test(true), is(false));
    }

    public void testBooleanValue() throws Exception {
        final boolean matchValue = randomBoolean();
        final FieldPredicate predicate = FieldPredicate.create(matchValue);
        assertThat(predicate.test(matchValue), is(true));
        assertThat(predicate.test(!matchValue), is(false));
        assertThat(predicate.test(String.valueOf(matchValue)), is(false));
        assertThat(predicate.test(""), is(false));
        assertThat(predicate.test(1), is(false));
        assertThat(predicate.test(null), is(false));
    }

    public void testLongValue() throws Exception {
        final int intValue = randomInt();
        final long longValue = intValue;
        final FieldPredicate predicate = FieldPredicate.create(longValue);

        assertThat(predicate.test(longValue), is(true));
        assertThat(predicate.test(intValue), is(true));
        assertThat(predicate.test(new BigInteger(String.valueOf(longValue))), is(true));

        assertThat(predicate.test(longValue - 1), is(false));
        assertThat(predicate.test(intValue + 1), is(false));
        assertThat(predicate.test(String.valueOf(longValue)), is(false));
        assertThat(predicate.test(""), is(false));
        assertThat(predicate.test(true), is(false));
        assertThat(predicate.test(null), is(false));
    }

    public void testSimpleAutomatonValue() throws Exception {
        final String prefix = randomAlphaOfLength(3);
        final FieldPredicate predicate = FieldPredicate.create(prefix + "*");

        assertThat(predicate.test(prefix), is(true));
        assertThat(predicate.test(prefix + randomAlphaOfLengthBetween(1, 5)), is(true));

        assertThat(predicate.test("_" + prefix), is(false));
        assertThat(predicate.test(prefix.substring(0, 1)), is(false));

        assertThat(predicate.test(""), is(false));
        assertThat(predicate.test(1), is(false));
        assertThat(predicate.test(true), is(false));
        assertThat(predicate.test(null), is(false));
    }

    public void testEmptyStringValue() throws Exception {
        final FieldPredicate predicate = FieldPredicate.create("");

        assertThat(predicate.test(""), is(true));

        assertThat(predicate.test(randomAlphaOfLengthBetween(1, 3)), is(false));
        assertThat(predicate.test(1), is(false));
        assertThat(predicate.test(true), is(false));
        assertThat(predicate.test(null), is(false));
    }

    public void testRegexAutomatonValue() throws Exception {
        final String substring = randomAlphaOfLength(5);
        final FieldPredicate predicate = FieldPredicate.create("/.*" + substring + ".*/");

        assertThat(predicate.test(substring), is(true));
        assertThat(predicate.test(
                randomAlphaOfLengthBetween(2, 4) + substring + randomAlphaOfLengthBetween(1, 5)),
                is(true));

        assertThat(predicate.test(substring.substring(1, 3)), is(false));

        assertThat(predicate.test(""), is(false));
        assertThat(predicate.test(1), is(false));
        assertThat(predicate.test(true), is(false));
        assertThat(predicate.test(null), is(false));
    }
}