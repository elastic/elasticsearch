/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.launcher;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for {@link StderrTaggingOutputStream}.
 */
public class StderrTaggingOutputStreamTests extends ESTestCase {

    private ByteArrayOutputStream rawOutput;
    private StderrTaggingOutputStream tagging;

    private void createStream() {
        rawOutput = new ByteArrayOutputStream();
        tagging = new StderrTaggingOutputStream(rawOutput);
    }

    private String output() {
        return rawOutput.toString(StandardCharsets.UTF_8);
    }

    private void writeString(String s) throws IOException {
        tagging.write(s.getBytes(StandardCharsets.UTF_8));
    }

    public void testSingleLineTagged() throws IOException {
        createStream();
        writeString("hello\n");
        assertThat(output(), equalTo("\u0001hello\n"));
    }

    public void testMultipleLinesEachTagged() throws IOException {
        createStream();
        writeString("line1\nline2\nline3\n");
        assertThat(output(), equalTo("\u0001line1\n\u0001line2\n\u0001line3\n"));
    }

    public void testPartialLineWithoutNewline() throws IOException {
        createStream();
        writeString("partial");
        assertThat(output(), equalTo("\u0001partial"));
    }

    public void testPartialLineThenNewline() throws IOException {
        createStream();
        writeString("partial");
        writeString(" more\n");
        assertThat(output(), equalTo("\u0001partial more\n"));
    }

    public void testMultipleWritesSameLine() throws IOException {
        createStream();
        writeString("hello ");
        writeString("world\n");
        assertThat(output(), equalTo("\u0001hello world\n"));
    }

    public void testByteAtATime() throws IOException {
        createStream();
        for (byte b : "hi\n".getBytes(StandardCharsets.UTF_8)) {
            tagging.write(b);
        }
        assertThat(output(), equalTo("\u0001hi\n"));
    }

    public void testEmptyWrite() throws IOException {
        createStream();
        tagging.write(new byte[0], 0, 0);
        assertThat(output(), equalTo(""));
    }

    public void testNewlineOnlyLine() throws IOException {
        createStream();
        writeString("\n");
        assertThat(output(), equalTo("\u0001\n"));
    }

    public void testConsecutiveNewlines() throws IOException {
        createStream();
        writeString("\n\n");
        assertThat(output(), equalTo("\u0001\n\u0001\n"));
    }

    public void testTagAfterNewlineInSeparateWrite() throws IOException {
        createStream();
        writeString("first\n");
        writeString("second\n");
        assertThat(output(), equalTo("\u0001first\n\u0001second\n"));
    }

    public void testUtf8MultiByteCharacters() throws IOException {
        createStream();
        writeString("café\n");
        assertThat(output(), equalTo("\u0001café\n"));
    }
}
