/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.esql.expression.function.DocsV3Support.FUNCTIONS;
import static org.hamcrest.Matchers.equalTo;

public class DocsV3SupportTests extends ESTestCase {

    public void testFunctionLink() {
        String text = "The value that is greater than half of all values and less than half of all values, "
            + "also known as the 50% <<esql-percentile>>.";
        String expected = "The value that is greater than half of all values and less than half of all values, "
            + "also known as the 50% [`PERCENTILE`](../../../esql-functions-operators.md#esql-percentile).";
        assertThat(FUNCTIONS.replaceLinks(text), equalTo(expected));
    }

    public void testFunctionAndHeaderLinks() {
        String text = "Like <<esql-percentile>>, `MEDIAN` is <<esql-percentile-approximate,usually approximate>>.";
        String expected = "Like [`PERCENTILE`](../../../esql-functions-operators.md#esql-percentile), "
            + "`MEDIAN` is [usually approximate](../../../esql-functions-operators.md#esql-percentile-approximate).";
        assertThat(FUNCTIONS.replaceLinks(text), equalTo(expected));
    }

    public void testWikipediaMacro() {
        String text = "Returns the {wikipedia}/Inverse_trigonometric_functions[arccosine] of `n` as an angle, expressed in radians.";
        String expected = "Returns the [arccosine](https://en.wikipedia.org/wiki/Inverse_trigonometric_functions) "
            + "of `n` as an angle, expressed in radians.";
        assertThat(FUNCTIONS.replaceLinks(text), equalTo(expected));
    }

    public void testJavadocMacro() {
        String text = "This is a noop for `long` (including unsigned) and `integer`.\n"
            + "For `double` this picks the closest `double` value to the integer similar to\n"
            + "{javadoc}/java.base/java/lang/Math.html#ceil(double)[Math.ceil].";
        String expected = "This is a noop for `long` (including unsigned) and `integer`.\n"
            + "For `double` this picks the closest `double` value to the integer similar to\n"
            + "[Math.ceil](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Math.html#ceil(double)).";
        assertThat(FUNCTIONS.replaceLinks(text), equalTo(expected));
    }
}
