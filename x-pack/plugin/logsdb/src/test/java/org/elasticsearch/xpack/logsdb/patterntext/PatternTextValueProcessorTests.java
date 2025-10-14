/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class PatternTextValueProcessorTests extends ESTestCase {

    public void testEmpty() throws IOException {
        String text = "";
        PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(text);
        assertEquals(text, parts.template());
        assertTrue(parts.args().isEmpty());
        assertEquals(text, PatternTextValueProcessor.merge(parts));
    }

    public void testWhitespace() throws IOException {
        String text = " ";
        PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(text);
        assertEquals(text, parts.template());
        assertTrue(parts.args().isEmpty());
        assertEquals(text, PatternTextValueProcessor.merge(parts));
    }

    public void testWithoutTimestamp() throws IOException {
        String text = " some text with arg1 and 2arg2 and 333 ";
        PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(text);
        assertEquals(" some text with  and  and  ", parts.template());
        assertThat(parts.args(), Matchers.contains("arg1", "2arg2", "333"));
        assertThat(parts.argsInfo(), equalTo(info(16, 21, 26)));
        assertEquals(text, PatternTextValueProcessor.merge(parts));
    }

    public void testWithTimestamp() throws IOException {
        String text = " 2021-04-13T13:51:38.000Z some text with arg1 and arg2 and arg3";
        PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(text);
        assertEquals("  some text with  and  and ", parts.template());
        assertThat(parts.args(), Matchers.contains("2021-04-13T13:51:38.000Z", "arg1", "arg2", "arg3"));
        assertThat(parts.argsInfo(), equalTo(info(1, 17, 22, 27)));
        assertEquals(text, PatternTextValueProcessor.merge(parts));
    }

    public void testWithDateSpaceTime() throws IOException {
        String text = " 2021-04-13 13:51:38 some text with arg1 and arg2 and arg3";
        PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(text);
        assertEquals("   some text with  and  and ", parts.template());
        assertThat(parts.argsInfo(), equalTo(info(1, 2, 18, 23, 28)));
        assertThat(parts.args(), Matchers.contains("2021-04-13", "13:51:38", "arg1", "arg2", "arg3"));
        assertEquals(text, PatternTextValueProcessor.merge(parts));
    }

    public void testMalformedDate() throws IOException {
        String text = "2020/09/06 10:11:38 Using namespace: kubernetes-dashboard' | HTTP status: 400, message: [1:395]";
        PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(text);
        assertEquals("  Using namespace: kubernetes-dashboard' | HTTP status:  message: []", parts.template());
        assertThat(parts.argsInfo(), equalTo(info(0, 1, 56, 67)));
        assertThat(parts.args(), Matchers.contains("2020/09/06", "10:11:38", "400,", "1:395"));
        assertEquals(text, PatternTextValueProcessor.merge(parts));
    }

    public void testUUID() throws IOException {
        String text = "[2020-08-18T00:58:56.751+00:00][15][2354][action_controller][INFO]: [18be2355-6306-4a00-9db9-f0696aa1a225] "
            + "some text with arg1 and arg2";
        PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(text);
        assertEquals("[][][][action_controller][INFO]: [] some text with  and ", parts.template());
        assertThat(parts.argsInfo(), equalTo(info(1, 3, 5, 34, 51, 56)));
        assertThat(
            parts.args(),
            Matchers.contains("2020-08-18T00:58:56.751+00:00", "15", "2354", "18be2355-6306-4a00-9db9-f0696aa1a225", "arg1", "arg2")
        );
        assertEquals(text, PatternTextValueProcessor.merge(parts));
    }

    public void testIP() throws IOException {
        String text = "[2020-08-18T00:58:56.751+00:00][15][2354][action_controller][INFO]: from 94.168.152.150 and arg1";
        PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(text);
        assertEquals("[][][][action_controller][INFO]: from  and ", parts.template());
        assertThat(parts.argsInfo(), equalTo(info(1, 3, 5, 38, 43)));
        assertThat(parts.args(), Matchers.contains("2020-08-18T00:58:56.751+00:00", "15", "2354", "94.168.152.150", "arg1"));
        assertEquals(text, PatternTextValueProcessor.merge(parts));
    }

    public void testSecondDate() throws IOException {
        String text = "[2020-08-18T00:58:56.751+00:00][15][2354][action_controller][INFO]: at 2020-08-18 00:58:56 +0000 and arg1";
        PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(text);
        assertEquals("[][][][action_controller][INFO]: at    and ", parts.template());
        assertThat(parts.argsInfo(), equalTo(info(1, 3, 5, 36, 37, 38, 43)));
        assertThat(
            parts.args(),
            Matchers.contains("2020-08-18T00:58:56.751+00:00", "15", "2354", "2020-08-18", "00:58:56", "+0000", "arg1")
        );
        assertEquals(text, PatternTextValueProcessor.merge(parts));
    }

    public void testWithTimestampStartBrackets() throws IOException {
        String text = "[2020-08-18T00:58:56] Found 123 errors for service [cheddar1]";
        PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(text);
        assertEquals("[] Found  errors for service []", parts.template());
        assertThat(parts.argsInfo(), equalTo(info(1, 9, 30)));
        assertThat(parts.args(), Matchers.contains("2020-08-18T00:58:56", "123", "cheddar1"));
        assertEquals(text, PatternTextValueProcessor.merge(parts));
    }

    public void testTemplateIdIsExpectedShape() throws IOException {
        String text = "[2020-08-18T00:58:56] Found 123 errors for service [cheddar1]";
        PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(text);
        assertEquals("1l_PtCLQ5xY", parts.templateId());
    }

    public void testTemplateIdHasVeryFewCollisions() throws IOException {
        Set<String> templates = new HashSet<>();
        Set<String> ids = new HashSet<>();

        for (int i = 0; i < 1000; i++) {
            var template = randomTemplate();
            var parts = new PatternTextValueProcessor.Parts(template, List.of(), List.of());
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
            var token = randomBoolean() ? randomAlphaOfLength(between(1, 10)) : "";
            sb.append(token);
            sb.append(randomDelimiter());
        }
        return sb.toString();
    }

    private static String randomDelimiter() {
        return randomFrom(List.of(" ", "\n", "\t", "[", "]"));
    }

    private static List<Arg.Info> info(int... offsets) throws IOException {
        return Arrays.stream(offsets).mapToObj(o -> new Arg.Info(Arg.Type.GENERIC, o)).toList();
    }
}
