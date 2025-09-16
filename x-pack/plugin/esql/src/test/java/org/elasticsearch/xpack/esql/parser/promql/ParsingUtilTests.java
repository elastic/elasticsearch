/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser.promql;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static org.elasticsearch.core.TimeValue.timeValueDays;
import static org.elasticsearch.core.TimeValue.timeValueHours;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.core.TimeValue.timeValueMinutes;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.hamcrest.Matchers.containsString;

public class ParsingUtilTests extends ESTestCase {

    private TimeValue parseTimeValue(String value) {
        return ParsingUtils.parseTimeValue(new Source(0, 0, value), value);
    }

    private void invalidTimeValue(String value, String errorMessage) {
        ParsingException exception = expectThrows(ParsingException.class, () -> parseTimeValue(value));
        assertThat(exception.getErrorMessage(), containsString(errorMessage));
    }

    public void testTimeValuePerUnit() throws Exception {
        assertEquals(timeValueDays(365), parseTimeValue("1y"));
        assertEquals(timeValueDays(7), parseTimeValue("1w"));
        assertEquals(timeValueDays(1), parseTimeValue("1d"));
        assertEquals(timeValueHours(1), parseTimeValue("1h"));
        assertEquals(timeValueMinutes(1), parseTimeValue("1m"));
        assertEquals(timeValueSeconds(1), parseTimeValue("1s"));
        assertEquals(timeValueMillis(1), parseTimeValue("1ms"));
    }

    public void testTimeValueCombined() throws Exception {
        assertEquals(new TimeValue(timeValueDays(365).millis() + timeValueDays(2 * 7).millis()), parseTimeValue("1y2w"));
        assertEquals(new TimeValue(timeValueDays(365).millis() + timeValueDays(3).millis()), parseTimeValue("1y3d"));
        assertEquals(new TimeValue(timeValueDays(365).millis() + timeValueHours(4).millis()), parseTimeValue("1y4h"));
        assertEquals(new TimeValue(timeValueDays(365).millis() + timeValueMinutes(5).millis()), parseTimeValue("1y5m"));
        assertEquals(new TimeValue(timeValueDays(365).millis() + timeValueSeconds(6).millis()), parseTimeValue("1y6s"));
        assertEquals(new TimeValue(timeValueDays(365).millis() + timeValueMillis(7).millis()), parseTimeValue("1y7ms"));

        assertEquals(
            new TimeValue(timeValueDays(365).millis() + timeValueDays(2 * 7).millis() + timeValueDays(3).millis()),
            parseTimeValue("1y2w3d")
        );

        assertEquals(
            new TimeValue(timeValueDays(365).millis() + timeValueDays(3).millis() + timeValueHours(4).millis()),
            parseTimeValue("1y3d4h")
        );

        assertEquals(
            new TimeValue(timeValueDays(365).millis() + timeValueMinutes(5).millis() + timeValueSeconds(6).millis()),
            parseTimeValue("1y5m6s")
        );

        assertEquals(
            new TimeValue(
                timeValueDays(365).millis() + timeValueDays(7).millis() + timeValueDays(1).millis() + timeValueHours(1).millis()
                    + timeValueMinutes(1).millis() + timeValueSeconds(1).millis() + timeValueMillis(1).millis()
            ),
            parseTimeValue("1y1w1d1h1m1s1ms")
        );

    }

    public void testTimeValueRandomCombination() throws Exception {
        // lambdas generating time value for each time unit (identified by its list index)
        List<Tuple<String, Function<Integer, TimeValue>>> generators = asList(
            new Tuple<>("y", n -> timeValueDays(365 * n)),
            new Tuple<>("w", n -> timeValueDays(7 * n)),
            new Tuple<>("d", TimeValue::timeValueDays),
            new Tuple<>("h", TimeValue::timeValueHours),
            new Tuple<>("m", TimeValue::timeValueMinutes),
            new Tuple<>("s", TimeValue::timeValueSeconds),
            new Tuple<>("ms", TimeValue::timeValueMillis)
        );
        // iterate using a random step through list pick a random number of units from the list by iterating with a random step
        int maximum = generators.size() - 1;
        StringBuilder timeString = new StringBuilder();
        long millis = 0;

        for (int position = -1; position < maximum;) {
            int step = randomIntBetween(1, maximum - position);
            position += step;
            Tuple<String, Function<Integer, TimeValue>> tuple = generators.get(position);
            int number = randomInt(128);
            timeString.append(number);
            timeString.append(tuple.v1());
            millis += tuple.v2().apply(number).millis();
        }

        assertEquals(new TimeValue(millis), parseTimeValue(timeString.toString()));
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
        return ParsingUtils.unquote(new Source(0, 0, value));
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
