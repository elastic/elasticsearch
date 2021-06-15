/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.categorization;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.StringReader;

import static org.hamcrest.Matchers.equalTo;

public class FirstNonBlankLineCharFilterTests extends ESTestCase {

    public void testEmpty() throws IOException {

        String input = "";
        FirstNonBlankLineCharFilter filter = new FirstNonBlankLineCharFilter(new StringReader(input));

        assertThat(filter.read(), equalTo(-1));
    }

    public void testAllBlankOneLine() throws IOException {

        String input = "\t";
        if (randomBoolean()) {
            input = " " + input;
        }
        if (randomBoolean()) {
            input = input + " ";
        }
        FirstNonBlankLineCharFilter filter = new FirstNonBlankLineCharFilter(new StringReader(input));

        assertThat(filter.read(), equalTo(-1));
    }

    public void testNonBlankNoNewlines() throws IOException {

        String input = "the quick brown fox jumped over the lazy dog";
        if (randomBoolean()) {
            input = " " + input;
        }
        if (randomBoolean()) {
            input = input + " ";
        }
        FirstNonBlankLineCharFilter filter = new FirstNonBlankLineCharFilter(new StringReader(input));

        char[] output = new char[input.length()];
        assertThat(filter.read(output, 0, output.length), equalTo(input.length()));
        assertThat(filter.read(), equalTo(-1));
        assertThat(new String(output), equalTo(input));
    }

    public void testAllBlankMultiline() throws IOException {

        StringBuilder input = new StringBuilder();
        String lineEnding = randomBoolean() ? "\n" : "\r\n";
        for (int lineNum = randomIntBetween(2, 5); lineNum > 0; --lineNum) {
            for (int charNum = randomIntBetween(0, 5); charNum > 0; --charNum) {
                input.append(randomBoolean() ? " " : "\t");
            }
            if (lineNum > 1 || randomBoolean()) {
                input.append(lineEnding);
            }
        }

        FirstNonBlankLineCharFilter filter = new FirstNonBlankLineCharFilter(new StringReader(input.toString()));

        assertThat(filter.read(), equalTo(-1));
    }

    public void testNonBlankMultiline() throws IOException {

        StringBuilder input = new StringBuilder();
        String lineEnding = randomBoolean() ? "\n" : "\r\n";
        for (int lineBeforeNum = randomIntBetween(2, 5); lineBeforeNum > 0; --lineBeforeNum) {
            for (int charNum = randomIntBetween(0, 5); charNum > 0; --charNum) {
                input.append(randomBoolean() ? " " : "\t");
            }
            input.append(lineEnding);
        }
        String lineToKeep = "the quick brown fox jumped over the lazy dog";
        if (randomBoolean()) {
            lineToKeep = " " + lineToKeep;
        }
        if (randomBoolean()) {
            lineToKeep = lineToKeep + " ";
        }
        input.append(lineToKeep).append(lineEnding);
        for (int lineAfterNum = randomIntBetween(2, 5); lineAfterNum > 0; --lineAfterNum) {
            for (int charNum = randomIntBetween(0, 5); charNum > 0; --charNum) {
                input.append(randomBoolean() ? " " : "more");
            }
            if (lineAfterNum > 1 || randomBoolean()) {
                input.append(lineEnding);
            }
        }

        FirstNonBlankLineCharFilter filter = new FirstNonBlankLineCharFilter(new StringReader(input.toString()));

        char[] output = new char[lineToKeep.length()];
        assertThat(filter.read(output, 0, output.length), equalTo(lineToKeep.length()));
        assertThat(filter.read(), equalTo(-1));
        assertThat(new String(output), equalTo(lineToKeep));
    }

    public void testCorrect() throws IOException {

        String input = "   \nfirst line\nsecond line";
        FirstNonBlankLineCharFilter filter = new FirstNonBlankLineCharFilter(new StringReader(input));

        String expectedOutput = "first line";

        char[] output = new char[expectedOutput.length()];
        assertThat(filter.read(output, 0, output.length), equalTo(expectedOutput.length()));
        assertThat(filter.read(), equalTo(-1));
        assertThat(new String(output), equalTo(expectedOutput));

        int expectedOutputIndex = input.indexOf(expectedOutput);
        for (int i = 0; i < expectedOutput.length(); ++i) {
            assertThat(filter.correctOffset(i), equalTo(expectedOutputIndex + i));
        }
        // When the input gets chopped by a char filter immediately after a token, that token must be reported as
        // ending at the very end of the original input, otherwise multi-message analysis will have incorrect offsets
        assertThat(filter.correctOffset(expectedOutput.length()), equalTo(input.length()));
    }
}
