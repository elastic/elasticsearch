/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser.promql;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.PromqlFeatures;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.junit.BeforeClass;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

import static java.time.Duration.ofDays;
import static java.time.Duration.ofHours;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assume.assumeTrue;

public class PromqlParserUtilsTests extends ESTestCase {

    @BeforeClass
    public static void checkPromqlEnabled() {
        assumeTrue("requires snapshot build with promql feature enabled", PromqlFeatures.isEnabled());
    }

    private Duration parseTimeValue(String value) {
        return PromqlParserUtils.parseDuration(new Source(0, 0, value), value);
    }

    private void invalidTimeValue(String value, String errorMessage) {
        ParsingException exception = expectThrows(ParsingException.class, () -> parseTimeValue(value));
        assertThat(exception.getErrorMessage(), containsString(errorMessage));
    }

    public void testTimeValuePerUnit() throws Exception {
        assertEquals(ofDays(365), parseTimeValue("1y"));
        assertEquals(ofDays(7), parseTimeValue("1w"));
        assertEquals(ofDays(1), parseTimeValue("1d"));
        assertEquals(ofHours(1), parseTimeValue("1h"));
        assertEquals(ofMinutes(1), parseTimeValue("1m"));
        assertEquals(ofSeconds(1), parseTimeValue("1s"));
        assertEquals(ofMillis(1), parseTimeValue("1ms"));
    }

    public void testTimeValueCombined() throws Exception {
        assertEquals(ofDays(365).plus(ofDays(14)), parseTimeValue("1y2w"));
        assertEquals(ofDays(365).plus(ofDays(3)), parseTimeValue("1y3d"));
        assertEquals(ofDays(365).plus(ofHours(4)), parseTimeValue("1y4h"));
        assertEquals(ofDays(365).plus(ofMinutes(5)), parseTimeValue("1y5m"));
        assertEquals(ofDays(365).plus(ofSeconds(6)), parseTimeValue("1y6s"));
        assertEquals(ofDays(365).plus(ofMillis(7)), parseTimeValue("1y7ms"));

        assertEquals(ofDays(365).plus(ofDays(14)).plus(ofDays(3)), parseTimeValue("1y2w3d"));

        assertEquals(ofDays(365).plus(ofDays(3)).plus(ofHours(4)), parseTimeValue("1y3d4h"));

        assertEquals(ofDays(365).plus(ofMinutes(5)).plus(ofSeconds(6)), parseTimeValue("1y5m6s"));

        assertEquals(
            ofDays(365).plus(ofDays(7)).plus(ofDays(1)).plus(ofHours(1)).plus(ofMinutes(1)).plus(ofSeconds(1)).plus(ofMillis(1)),
            parseTimeValue("1y1w1d1h1m1s1ms")
        );

    }

    public void testTimeValueRandomCombination() throws Exception {
        // lambdas generating duration for each time unit (identified by its list index)
        List<Tuple<String, Function<Integer, Duration>>> generators = asList(
            new Tuple<>("y", n -> ofDays(365 * n)),
            new Tuple<>("w", n -> ofDays(7 * n)),
            new Tuple<>("d", Duration::ofDays),
            new Tuple<>("h", Duration::ofHours),
            new Tuple<>("m", Duration::ofMinutes),
            new Tuple<>("s", Duration::ofSeconds),
            new Tuple<>("ms", Duration::ofMillis)
        );
        // iterate using a random step through list pick a random number of units from the list by iterating with a random step
        int maximum = generators.size() - 1;
        StringBuilder timeString = new StringBuilder();
        long millis = 0;

        for (int position = -1; position < maximum;) {
            int step = randomIntBetween(1, maximum - position);
            position += step;
            Tuple<String, Function<Integer, Duration>> tuple = generators.get(position);
            int number = randomInt(128);
            timeString.append(number);
            timeString.append(tuple.v1());
            millis += tuple.v2().apply(number).toMillis();
        }

        assertEquals(ofMillis(millis), parseTimeValue(timeString.toString()));
    }

    public void testTimeValueErrorNoNumber() throws Exception {
        String error = "no number specified at";
        List<String> values = asList("d", " y");
        for (String value : values) {
            invalidTimeValue(value, error);
        }
    }

    public void testTimeValueErrorNoUnit() throws Exception {
        String error = "no unit specified at";
        List<String> values = asList("234/", "12-", "1y1");
        for (String value : values) {
            invalidTimeValue(value, error);
        }
    }

    public void testTimeValueErrorInvalidUnit() throws Exception {
        String error = "unrecognized time unit";
        List<String> values = asList("1o", "234sd", "1dd");
        for (String value : values) {
            invalidTimeValue(value, error);
        }
    }

    public void testTimeValueErrorUnitOrder() throws Exception {
        String error = "units must be ordered from the longest to the shortest";
        List<String> values = asList("1w1y", "1y1w1h1m1h", "1y1w1y");
        for (String value : values) {
            invalidTimeValue(value, error);
        }
    }

    public void testTimeValueErrorUnitDuplicated() throws Exception {
        String error = "a given unit must only appear once";
        List<String> values = asList("1w1w", "1y1w1d1d");
        for (String value : values) {
            invalidTimeValue(value, error);
        }
    }

    //
    // Go escaping rules
    //
    private String unquote(String value) {
        return PromqlParserUtils.unquote(new Source(0, 0, value));
    }

    private void invalidString(String value, String errorMessage) {
        ParsingException exception = expectThrows(ParsingException.class, () -> unquote(value));
        assertThat(exception.getErrorMessage(), containsString(errorMessage));
    }

    private String quote(String unquote) {
        char c = randomBoolean() ? '"' : '\'';
        return c + unquote + c;
    }

    public void testGoRuneLiteralsHex() throws Exception {
        assertEquals("#", unquote(quote("\\x23")));
        assertEquals("#4", unquote(quote("\\x234")));
        assertEquals("a#4", unquote(quote("a\\x234")));

        assertEquals("#", unquote(quote("\\u0023")));
        assertEquals("#4", unquote(quote("\\u00234")));

        assertEquals("#", unquote(quote("\\U00000023")));
        assertEquals("#4", unquote(quote("\\U000000234")));
    }

    public void testGoRuneLiteralsErrorHex() throws Exception {
        String error = "Invalid unicode character code";
        List<String> values = asList("\\xAG", "a\\xXA", "\\u123G", "u\\uX23G", "\\U1234567X", "AB\\U1234567X", "\\xff");
        for (String value : values) {
            invalidString(quote(value), error);
        }
    }

    public void testGoRuneLiteralsOct() throws Exception {
        assertEquals("#", unquote(quote("\\043")));
        assertEquals("#4", unquote(quote("\\0434")));
        assertEquals("a#4", unquote(quote("a\\0434")));
    }

    public void testGoRuneLiteralsErrorOct() throws Exception {
        String error = "Invalid unicode character code";
        List<String> values = asList("901", "0909");
        for (String value : values) {
            invalidString(quote("\\" + value), error);
        }
    }
}
