/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.patternedtext;

import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

public class PatternedTextValueProcessorTests extends ESTestCase {

    public void testEmpty() {
        String text = "";
        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(text);
        assertEquals(text, parts.template());
        assertNull(parts.timestamp());
        assertTrue(parts.args().isEmpty());
        assertEquals(text, PatternedTextValueProcessor.merge(parts));
    }

    public void testWhitespace() {
        String text = " ";
        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(text);
        assertEquals(text, parts.template());
        assertNull(parts.timestamp());
        assertTrue(parts.args().isEmpty());
        assertEquals(text, PatternedTextValueProcessor.merge(parts));
    }

    public void testWithoutTimestamp() {
        String text = " some text with arg1 and 2arg2 and 333 ";
        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(text);
        assertEquals(" some text with %W and %W and %W ", parts.template());
        assertNull(parts.timestamp());
        assertThat(parts.args(), Matchers.contains("arg1", "2arg2", "333"));
        assertEquals(text, PatternedTextValueProcessor.merge(parts));
    }

    public void testWithTimestamp() {
        String text = " 2021-04-13T13:51:38.000Z some text with arg1 and arg2 and arg3";
        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(text);
        assertEquals(" %T some text with %W and %W and %W", parts.template());
        assertEquals(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2021-04-13T13:51:38.000Z"), (long) parts.timestamp());
        assertThat(parts.args(), Matchers.contains("arg1", "arg2", "arg3"));
        assertEquals(text, PatternedTextValueProcessor.merge(parts));
    }

    public void testWithDateSpaceTime() {
        String text = " 2021-04-13 13:51:38 some text with arg1 and arg2 and arg3";
        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(text);
        assertEquals(" %T some text with %W and %W and %W", parts.template());
        assertEquals(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2021-04-13T13:51:38.000Z"), (long) parts.timestamp());
        assertThat(parts.args(), Matchers.contains("arg1", "arg2", "arg3"));
        assertEquals(text.replace("2021-04-13 13:51:38", "2021-04-13T13:51:38.000Z"), PatternedTextValueProcessor.merge(parts));
    }

    public void testMalformedDate() {
        String text = "2020/09/06 10:11:38 Using namespace: kubernetes-dashboard' | HTTP status: 400, message: [1:395]";
        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(text);
        assertEquals("%T Using namespace: kubernetes-dashboard' | HTTP status: %W message: [%W]", parts.template());
        assertEquals(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-09-06T10:11:38"), (long) parts.timestamp());
        assertThat(parts.args(), Matchers.contains("400,", "1:395"));
        assertEquals(text.replace("2020/09/06 10:11:38", "2020-09-06T10:11:38.000Z"), PatternedTextValueProcessor.merge(parts));
    }

    public void testUUID() {
        String text = "[2020-08-18T00:58:56.751+00:00][15][2354][action_controller][INFO]: [18be2355-6306-4a00-9db9-f0696aa1a225] "
            + "some text with arg1 and arg2";
        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(text);
        assertEquals("[%T][%W][%W][action_controller][INFO]: [%U] some text with %W and %W", parts.template());
        assertEquals(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-08-18T00:58:56.751+00:00"), (long) parts.timestamp());
        assertThat(parts.args(), Matchers.contains("15", "2354", "AEoGY1UjvhgloqFqafC5nQ", "arg1", "arg2"));
        assertEquals(text.replace("+00:00", "Z"), PatternedTextValueProcessor.merge(parts));
    }

    public void testIP() {
        String text = "[2020-08-18T00:58:56.751+00:00][15][2354][action_controller][INFO]: from 94.168.152.150 and arg1";
        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(text);
        assertEquals("[%T][%W][%W][action_controller][INFO]: from %I and %W", parts.template());
        assertEquals(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-08-18T00:58:56.751+00:00"), (long) parts.timestamp());
        assertThat(parts.args(), Matchers.contains("15", "2354", "XqiYlg", "arg1"));
        assertEquals(text.replace("+00:00", "Z"), PatternedTextValueProcessor.merge(parts));
    }

    public void testSecondDate() {
        String text = "[2020-08-18T00:58:56.751+00:00][15][2354][action_controller][INFO]: at 2020-08-18 00:58:56 +0000 and arg1";
        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(text);
        assertEquals("[%T][%W][%W][action_controller][INFO]: at %D and %W", parts.template());
        assertEquals(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-08-18T00:58:56.751+00:00"), (long) parts.timestamp());
        assertThat(parts.args(), Matchers.contains("15", "2354", "gIQT/3MBAAA", "arg1"));
        assertEquals(
            text.replace("2020-08-18 00:58:56 +0000", "2020-08-18T00:58:56.000Z").replace("+00:00", "Z"),
            PatternedTextValueProcessor.merge(parts)
        );
    }
}
