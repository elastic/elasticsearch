/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.patternedtext;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

public class PatternedTextValueProcessorTests extends ESTestCase {

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

    public void testWithTimestamp() {
        String text = " 2021-04-13T13:51:38.000Z some text with arg1 and arg2 and arg3";
        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(text);
        assertEquals(" %W some text with %W and %W and %W", parts.template());
        assertThat(parts.args(), Matchers.contains("2021-04-13T13:51:38.000Z", "arg1", "arg2", "arg3"));
        assertEquals(text, PatternedTextValueProcessor.merge(parts));
    }

    public void testWithDateSpaceTime() {
        String text = " 2021-04-13 13:51:38 some text with arg1 and arg2 and arg3";
        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(text);
        assertEquals(" %W %W some text with %W and %W and %W", parts.template());
        assertThat(parts.args(), Matchers.contains("2021-04-13", "13:51:38", "arg1", "arg2", "arg3"));
        assertEquals(text, PatternedTextValueProcessor.merge(parts));
    }

    public void testMalformedDate() {
        String text = "2020/09/06 10:11:38 Using namespace: kubernetes-dashboard' | HTTP status: 400, message: [1:395]";
        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(text);
        assertEquals("%W %W Using namespace: kubernetes-dashboard' | HTTP status: %W message: [%W]", parts.template());
        assertThat(parts.args(), Matchers.contains("2020/09/06", "10:11:38", "400,", "1:395"));
        assertEquals(text, PatternedTextValueProcessor.merge(parts));
    }

    public void testUUID() {
        String text = "[2020-08-18T00:58:56.751+00:00][15][2354][action_controller][INFO]: [18be2355-6306-4a00-9db9-f0696aa1a225] "
            + "some text with arg1 and arg2";
        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(text);
        assertEquals("[%W][%W][%W][action_controller][INFO]: [%W] some text with %W and %W", parts.template());
        assertThat(
            parts.args(),
            Matchers.contains("2020-08-18T00:58:56.751+00:00", "15", "2354", "18be2355-6306-4a00-9db9-f0696aa1a225", "arg1", "arg2")
        );
        assertEquals(text, PatternedTextValueProcessor.merge(parts));
    }

    public void testIP() {
        String text = "[2020-08-18T00:58:56.751+00:00][15][2354][action_controller][INFO]: from 94.168.152.150 and arg1";
        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(text);
        assertEquals("[%W][%W][%W][action_controller][INFO]: from %W and %W", parts.template());
        assertThat(parts.args(), Matchers.contains("2020-08-18T00:58:56.751+00:00", "15", "2354", "94.168.152.150", "arg1"));
        assertEquals(text, PatternedTextValueProcessor.merge(parts));
    }

    public void testSecondDate() {
        String text = "[2020-08-18T00:58:56.751+00:00][15][2354][action_controller][INFO]: at 2020-08-18 00:58:56 +0000 and arg1";
        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(text);
        assertEquals("[%W][%W][%W][action_controller][INFO]: at %W %W %W and %W", parts.template());
        assertThat(
            parts.args(),
            Matchers.contains("2020-08-18T00:58:56.751+00:00", "15", "2354", "2020-08-18", "00:58:56", "+0000", "arg1")
        );
        assertEquals(text, PatternedTextValueProcessor.merge(parts));
    }
}
