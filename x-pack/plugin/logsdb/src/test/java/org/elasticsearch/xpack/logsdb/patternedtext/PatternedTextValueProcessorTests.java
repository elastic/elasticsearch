/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext;

import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.core.IsEqual.equalTo;

public class PatternedTextValueProcessorTests extends ESTestCase {

    private static final List<String> MILLI_RES_DATES = List.of(
        // 1 token
        "2020-09-06T08:29:04.123456",
        "2020-09-06T08:29:04.123Z",
        "2020-09-06T08:29:04,123",
        "2020-09-06T08:29:04.123+00:00",
        "2020-09-06T08:29:04.123+0000",

        // 2 token
        "2020-09-06 08:29:04,123",
        "2020-09-06 08:29:04.123",

        // 4 token
        "06 Sep 2020 08:29:04.123"
    );

    private static final List<String> SEC_RES_DATES = List.of(
        // 1 token
        "2020-09-06T08:29:04Z",
        "2020-09-06T08:29:04+0000",

        // 2 token
        "2020-09-06 08:29:04",
        "2020/09/06 08:29:04",
        "06/Sep/2020:08:29:04 +0000",

        // 3 token
        "2020-09-06 08:29:04 +0000",
        "2020-09-06 08:29:04 UTC",

        // 5 token
        "Sep 6, 2020 08:29:04 AM"
    );

    private static final List<String> ALL_DATES = new ArrayList<>();
    static {
        ALL_DATES.addAll(MILLI_RES_DATES);
        ALL_DATES.addAll(SEC_RES_DATES);
    }

    private static final String NORMALIZED_MILLI_RES = "2020-09-06T08:29:04.123Z";
    private static final String NORMALIZED_SEC_RES = "2020-09-06T08:29:04.000Z";

    public void testEmpty() {
        String text = "";
        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(text);
        assertEquals(text, parts.template());
        assertTrue(parts.args().isEmpty());
        assertEquals(text, PatternedTextValueProcessor.merge(parts));
    }

    public void testWhitespace() {
        String text = " ";
        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(text);
        assertEquals(text, parts.template());
        assertTrue(parts.args().isEmpty());
        assertEquals(text, PatternedTextValueProcessor.merge(parts));
    }

    public void testWithoutTimestamp() {
        String text = " some text with arg1 and 2arg2 and 333 ";
        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(text);
        assertEquals(" some text with %W and %W and %W ", parts.template());
        assertThat(parts.args(), Matchers.contains("arg1", "2arg2", "333"));
        assertEquals(text, PatternedTextValueProcessor.merge(parts));
    }

    public void testTimestampBeginning() {
        String testTemplate = "% some text with arg1 and arg2 and arg3";
        for (String ts : ALL_DATES) {
            String text = testTemplate.replace("%", ts);
            String expectedText = testTemplate.replace("%", normalized(ts));
            PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(text);
            assertEquals("%T some text with %W and %W and %W", parts.template());
            assertThat(parts.args(), Matchers.contains("arg1", "arg2", "arg3"));
            assertThat(parts.timestamp(), equalTo(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(normalized(ts))));
            assertEquals(expectedText, PatternedTextValueProcessor.merge(parts));
        }
    }

    public void testTimestampMiddle() {
        String testTemplate = "Using namespace: kubernetes-dashboard' % | HTTP status: 400, message: [1:395]";
        for (var ts : ALL_DATES) {
            String text = testTemplate.replace("%", ts);
            String expectedText = testTemplate.replace("%", normalized(ts));
            PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(text);
            assertEquals("Using namespace: kubernetes-dashboard' %T | HTTP status: %W message: [%W]", parts.template());
            assertThat(parts.args(), Matchers.contains("400,", "1:395"));
            assertThat(parts.timestamp(), equalTo(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(normalized(ts))));
            assertEquals(expectedText, PatternedTextValueProcessor.merge(parts));
        }
    }

    public void testTimestampEnd() {
        String testTemplate = "Using namespace: kubernetes-dashboard' | HTTP status: 400, message: [1:395] %";
        for (var ts : ALL_DATES) {
            String text = testTemplate.replace("%", ts);
            String expectedText = testTemplate.replace("%", normalized(ts));
            PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(text);
            assertEquals("Using namespace: kubernetes-dashboard' | HTTP status: %W message: [%W] %T", parts.template());
            assertThat(parts.args(), Matchers.contains("400,", "1:395"));
            assertThat(parts.timestamp(), equalTo(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(normalized(ts))));
            assertEquals(expectedText, PatternedTextValueProcessor.merge(parts));
        }
    }

    public void testUUID() {
        String testTemplate = "[%][15][2354][action_controller][INFO]: [18be2355-6306-4a00-9db9-f0696aa1a225] "
            + "some text with arg1 and arg2";
        String ts = randomFrom(ALL_DATES);
        String text = testTemplate.replace("%", ts);
        String expectedText = testTemplate.replace("%", normalized(ts));
        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(text);
        assertEquals("[%T][%W][%W][action_controller][INFO]: [%W] some text with %W and %W", parts.template());
        assertThat(parts.args(), Matchers.contains("15", "2354", "18be2355-6306-4a00-9db9-f0696aa1a225", "arg1", "arg2"));
        assertThat(parts.timestamp(), equalTo(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(normalized(ts))));
        assertEquals(expectedText, PatternedTextValueProcessor.merge(parts));
    }

    public void testIP() {
        String testTemplate = "[%][15][2354][action_controller][INFO]: from 94.168.152.150 and arg1";
        String ts = randomFrom(ALL_DATES);
        String text = testTemplate.replace("%", ts);
        String expectedText = testTemplate.replace("%", normalized(ts));
        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(text);
        assertEquals("[%T][%W][%W][action_controller][INFO]: from %W and %W", parts.template());
        assertThat(parts.args(), Matchers.contains("15", "2354", "94.168.152.150", "arg1"));
        assertThat(parts.timestamp(), equalTo(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(normalized(ts))));
        assertEquals(expectedText, PatternedTextValueProcessor.merge(parts));
    }

    public void testSecondDate() {
        String ts1 = randomFrom(ALL_DATES);
        String ts2 = randomFrom(ALL_DATES);
        String testTemplate = "[%1][15][2354][action_controller][INFO]: at %2 and arg1";
        String text = testTemplate.replace("%1", ts1).replace("%2", ts2);
        String expectedText = testTemplate.replace("%1", normalized(ts1)).replace("%2", ts2);

        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(text);

        var secondDateExpectedPlaceholders = Arrays.stream(ts2.split(" "))
            .map(t -> PatternedTextValueProcessor.containsDigit(t) ? "%W" : t)
            .collect(Collectors.joining(" "));
        assertEquals("[%T][%W][%W][action_controller][INFO]: at " + secondDateExpectedPlaceholders + " and %W", parts.template());
        assertThat(parts.timestamp(), equalTo(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(normalized(ts1))));
        assertEquals(expectedText, PatternedTextValueProcessor.merge(parts));
    }

    public void testWithTimestampRandomParseSuccess() {
        String template = "%";
        if (randomBoolean()) {
            template = "[" + template + "]";
        }
        template = randomFrom(" ", "") + template + randomFrom(" ", "");

        var ts = randomFrom(ALL_DATES);
        String text = template.replace("%", ts);
        String expected = template.replace("%", normalized(ts));

        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(text);
        assertEquals(expected, PatternedTextValueProcessor.merge(parts));
    }

    public void testWithTimestampRandomNoParseSuccess() {
        String template = "%";
        template = randomFrom("cat", "123") + template + randomFrom("cat", "123");

        var ts = randomFrom(ALL_DATES);
        String text = template.replace("%", ts);

        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(text);
        assertEquals(text, PatternedTextValueProcessor.merge(parts));
    }

    public void testTemplateIdIsExpectedShape() {
        String text = "[2020-08-18T00:58:56] Found 123 errors for service [cheddar1]";
        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(text);
        assertEquals("TZwL6Ju2bIg", parts.templateId());
    }

    public void testTemplateIdHasVeryFewCollisions() {
        Set<String> templates = new HashSet<>();
        Set<String> ids = new HashSet<>();

        for (int i = 0; i < 1000; i++) {
            var template = randomTemplate();
            var parts = new PatternedTextValueProcessor.Parts(template, null, List.of());
            templates.add(template);
            ids.add(parts.templateId());
        }
        // This can technically fail due to hash collision, but it should happen quite rarely.
        assertEquals(templates.size(), ids.size());
    }

    private static String randomTemplate() {
        StringBuilder sb = new StringBuilder();
        int numTokens = randomIntBetween(1, 20);
        for (int i = 0; i < numTokens; i++) {
            var token = randomBoolean() ? randomAlphaOfLength(between(1, 10)) : randomPlaceholder();
            sb.append(token);
            sb.append(randomDelimiter());
        }
        return sb.toString();
    }

    public void testParseTimestamps() {
        for (var ts : ALL_DATES) {
            String[] split = ts.split(" ");
            var res = PatternedTextValueProcessor.parse(split, 0);
            var millis = res.v1();
            String parsed = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(millis);
            logger.info("parsed: " + ts + " as " + parsed);
            assertEquals(normalized(ts), parsed);
        }
    }

    public void testNotTimestamp() {
        List<String> notTimestamp = List.of("2020-12-01", "2020-12", "2020");
        for (var ts : notTimestamp) {
            String[] split = ts.split(" ");
            var res = PatternedTextValueProcessor.parse(split, 0);
            assertNull(res);
        }
    }

    public void testOtherTimezones() {
        {
            String ts = "2020-09-06T08:29:04-05:00";
            String[] split = ts.split(" ");
            var res = PatternedTextValueProcessor.parse(split, 0);
            assertEquals("2020-09-06T13:29:04.000Z", DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(res.v1()));
        }
        {
            String ts = "2020-09-06 08:29:04 CDT";
            String[] split = ts.split(" ");
            var res = PatternedTextValueProcessor.parse(split, 0);
            assertEquals("2020-09-06T13:29:04.000Z", DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(res.v1()));
        }
    }

    public void testDontParseMultipleOffsetParts() {
        String ts = "2020-09-06T08:29:04+0000" + " UTC";
        String[] split = ts.split(" ");
        var res = PatternedTextValueProcessor.parse(split, 0);

        String result = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(res.v1());
        assertEquals("2020-09-06T08:29:04.000Z", result);
    }

    private static String randomPlaceholder() {
        return randomFrom(List.of("%W", "%D", "%I", "%U", "%T"));
    }

    private static String randomDelimiter() {
        return randomFrom(List.of(" ", "\n", "\t", "[", "]"));
    }

    private static String normalized(String date) {
        return SEC_RES_DATES.contains(date) ? NORMALIZED_SEC_RES : NORMALIZED_MILLI_RES;
    }
}
