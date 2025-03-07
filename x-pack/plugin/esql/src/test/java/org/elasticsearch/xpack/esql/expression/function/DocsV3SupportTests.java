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
            + "also known as the 50% [`PERCENTILE`](/reference/query-languages/esql/esql-functions-operators.md#esql-percentile).";
        assertThat(FUNCTIONS.replaceLinks(text), equalTo(expected));
    }

    public void testFunctionAndHeaderLinks() {
        String text = "Like <<esql-percentile>>, `MEDIAN` is <<esql-percentile-approximate,usually approximate>>.";
        String expected = "Like [`PERCENTILE`](/reference/query-languages/esql/esql-functions-operators.md#esql-percentile), "
            + "`MEDIAN` is [usually approximate](/reference/query-languages/esql/esql-functions-operators.md#esql-percentile-approximate).";
        assertThat(FUNCTIONS.replaceLinks(text), equalTo(expected));
    }

    public void testWikipediaMacro() {
        String text = "Returns the {wikipedia}/Inverse_trigonometric_functions[arccosine] of `n` as an angle, expressed in radians.";
        String expected = "Returns the [arccosine](https://en.wikipedia.org/wiki/Inverse_trigonometric_functions) "
            + "of `n` as an angle, expressed in radians.";
        assertThat(FUNCTIONS.replaceLinks(text), equalTo(expected));
    }

    public void testWikipediaMacro2() {
        String text = "This builds on the three-valued logic ({wikipedia}/Three-valued_logic[3VL]) of the language.";
        String expected =
            "This builds on the three-valued logic ([3VL](https://en.wikipedia.org/wiki/Three-valued_logic)) of the language.";
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

    public void testJavadoc8Macro() {
        String text = "Refer to {javadoc8}/java/time/temporal/ChronoField.html[java.time.temporal.ChronoField]";
        String expected =
            "Refer to [java.time.temporal.ChronoField](https://docs.oracle.com/javase/8/docs/api/java/time/temporal/ChronoField.html)";
        assertThat(FUNCTIONS.replaceLinks(text), equalTo(expected));
    }

    public void testJavadoc8MacroLongText() {
        String text = """
            Part of the date to extract.\n
            Can be: `aligned_day_of_week_in_month`, `aligned_day_of_week_in_year`, `aligned_week_of_month`, `aligned_week_of_year`,
            `ampm_of_day`, `clock_hour_of_ampm`, `clock_hour_of_day`, `day_of_month`, `day_of_week`, `day_of_year`, `epoch_day`,
            `era`, `hour_of_ampm`, `hour_of_day`, `instant_seconds`, `micro_of_day`, `micro_of_second`, `milli_of_day`,
            `milli_of_second`, `minute_of_day`, `minute_of_hour`, `month_of_year`, `nano_of_day`, `nano_of_second`,
            `offset_seconds`, `proleptic_month`, `second_of_day`, `second_of_minute`, `year`, or `year_of_era`.
            Refer to {javadoc8}/java/time/temporal/ChronoField.html[java.time.temporal.ChronoField]
            for a description of these values.\n
            If `null`, the function returns `null`.""";
        String expected = """
            Part of the date to extract.\n
            Can be: `aligned_day_of_week_in_month`, `aligned_day_of_week_in_year`, `aligned_week_of_month`, `aligned_week_of_year`,
            `ampm_of_day`, `clock_hour_of_ampm`, `clock_hour_of_day`, `day_of_month`, `day_of_week`, `day_of_year`, `epoch_day`,
            `era`, `hour_of_ampm`, `hour_of_day`, `instant_seconds`, `micro_of_day`, `micro_of_second`, `milli_of_day`,
            `milli_of_second`, `minute_of_day`, `minute_of_hour`, `month_of_year`, `nano_of_day`, `nano_of_second`,
            `offset_seconds`, `proleptic_month`, `second_of_day`, `second_of_minute`, `year`, or `year_of_era`.
            Refer to [java.time.temporal.ChronoField](https://docs.oracle.com/javase/8/docs/api/java/time/temporal/ChronoField.html)
            for a description of these values.\n
            If `null`, the function returns `null`.""";
        assertThat(FUNCTIONS.replaceLinks(text), equalTo(expected));
    }

    public void testKnownRootEsqlFiles() {
        String text = """
            The order that <<esql-multivalued-fields, multivalued fields>>
            are read from underlying storage is not guaranteed""";
        String expected = """
            The order that [multivalued fields](/reference/query-languages/esql/esql-multivalued-fields.md)
            are read from underlying storage is not guaranteed""";
        assertThat(FUNCTIONS.replaceLinks(text), equalTo(expected));
    }

    public void testKnownRootWithTag() {
        String text = """
            Match can use <<esql-function-named-params,function named parameters>>
            to specify additional options for the match query.""";
        String expected = """
            Match can use [function named parameters](/reference/query-languages/esql/esql-syntax.md#esql-function-named-params)
            to specify additional options for the match query.""";
        assertThat(FUNCTIONS.replaceLinks(text), equalTo(expected));
    }
}
