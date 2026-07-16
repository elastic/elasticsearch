/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cli.terminal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.terminal.Terminal.JsonTerminal;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class JsonTerminalTests extends ESTestCase {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private MockTerminal mockTerminal;
    private Terminal jsonTerminal;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockTerminal = MockTerminal.create();
        jsonTerminal = new JsonTerminal(mockTerminal);
    }

    private static JsonNode parseJson(String raw) {
        try {
            return MAPPER.readTree(raw);
        } catch (JsonProcessingException e) {
            throw new AssertionError("produced invalid JSON: " + raw, e);
        }
    }

    private static JsonNode parseSingleJsonLine(String str) {
        assertThat("JSON must be single line", str.indexOf('\n'), is(str.length() - 1));
        return parseJson(str);
    }

    public void testPrintlnFormatsAsJson() {
        jsonTerminal.println("Starting Serverless Elasticsearch...");

        JsonNode json = parseSingleJsonLine(mockTerminal.getOutput());
        assertEquals("INFO", json.get("log.level").asText());
        assertEquals("Starting Serverless Elasticsearch...", json.get("message").asText());
        assertEquals("stdout", json.get("log.logger").asText());
    }

    public void testErrorPrintlnFormatsAsJsonWithErrorLevel() {
        jsonTerminal.errorPrintln("something went wrong");

        JsonNode json = parseSingleJsonLine(mockTerminal.getErrorOutput());
        assertEquals("WARN", json.get("log.level").asText());
        assertEquals("something went wrong", json.get("message").asText());
        assertEquals("stderr", json.get("log.logger").asText());
    }

    public void testErrorPrintlnThrowableProducesSingleLine() {
        RuntimeException ex = new RuntimeException("test failure", new IllegalArgumentException("bad input"));

        jsonTerminal.errorPrintln(ex);

        JsonNode json = parseSingleJsonLine(mockTerminal.getErrorOutput());
        assertEquals("java.lang.RuntimeException", json.get("error.type").asText());
        assertTrue(json.has("error.stack_trace"));
        assertTrue("stack trace should mention cause", json.get("error.stack_trace").asText().contains("bad input"));
    }

    public void testErrorPrintlnThrowableWithVerbosity() {
        RuntimeException ex = new RuntimeException("verbose failure");

        jsonTerminal.setVerbosity(Terminal.Verbosity.SILENT);
        jsonTerminal.errorPrintln(Terminal.Verbosity.NORMAL, ex);
        assertEquals("should not print at SILENT verbosity", "", mockTerminal.getErrorOutput());

        jsonTerminal.setVerbosity(Terminal.Verbosity.NORMAL);
        jsonTerminal.errorPrintln(Terminal.Verbosity.NORMAL, ex);
        JsonNode json = parseSingleJsonLine(mockTerminal.getErrorOutput());
        assertEquals("java.lang.RuntimeException", json.get("error.type").asText());
    }

    public void testPrintlnWithVerbosity() {
        jsonTerminal.setVerbosity(Terminal.Verbosity.SILENT);
        jsonTerminal.println(Terminal.Verbosity.NORMAL, "should not appear");
        assertEquals("", mockTerminal.getOutput());

        jsonTerminal.setVerbosity(Terminal.Verbosity.NORMAL);
        jsonTerminal.println(Terminal.Verbosity.NORMAL, "should appear");
        JsonNode json = parseSingleJsonLine(mockTerminal.getOutput());
        assertEquals("should appear", json.get("message").asText());
    }

    public void testOutputIsAlwaysSingleLine() {
        jsonTerminal.println("line with\nnewlines\nembedded");

        JsonNode json = parseSingleJsonLine(mockTerminal.getOutput());
        assertEquals("line with\nnewlines\nembedded", json.get("message").asText());
    }

    public void testBinaryOutputDelegatesUnchanged() {
        mockTerminal.setSupportsBinary(true);
        assertNotNull("getOutputStream should delegate", jsonTerminal.getOutputStream());
    }

    public void testMultipleMessagesProduceMultipleJsonLines() {
        String[] messages = new String[] { "first", "second", "third" };
        for (var msg : messages) {
            jsonTerminal.println(msg);
        }

        String[] lines = mockTerminal.getOutput().split("\n");
        assertThat(lines.length, is(messages.length));

        for (var i = 0; i < lines.length; i++) {
            JsonNode json = parseJson(lines[i]);
            assertThat(json.get("message").asText(), is(messages[i]));
        }
    }
}
