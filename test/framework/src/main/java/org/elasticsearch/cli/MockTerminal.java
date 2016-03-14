/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cli;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * A terminal for tests which captures all output, and
 * can be plugged with fake input.
 */
public class MockTerminal extends Terminal {

    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    private final PrintWriter writer = new PrintWriter(new OutputStreamWriter(buffer, StandardCharsets.UTF_8));
    private final Deque<String> textInput = new ArrayDeque<>();
    private final Deque<String> secretInput = new ArrayDeque<>();

    public MockTerminal() {
        super("\n"); // always *nix newlines for tests
    }

    @Override
    public String readText(String prompt) {
        if (textInput.isEmpty()) {
            throw new IllegalStateException("No text input configured for prompt [" + prompt + "]");
        }
        return textInput.removeFirst();
    }

    @Override
    public char[] readSecret(String prompt) {
        if (secretInput.isEmpty()) {
            throw new IllegalStateException("No secret input configured for prompt [" + prompt + "]");
        }
        return secretInput.removeFirst().toCharArray();
    }

    @Override
    public PrintWriter getWriter() {
        return writer;
    }

    /** Adds an an input that will be return from {@link #readText(String)}. Values are read in FIFO order. */
    public void addTextInput(String input) {
        textInput.addLast(input);
    }

    /** Adds an an input that will be return from {@link #readText(String)}. Values are read in FIFO order. */
    public void addSecretInput(String input) {
        secretInput.addLast(input);
    }

    /** Returns all output written to this terminal. */
    public String getOutput() throws UnsupportedEncodingException {
        return buffer.toString("UTF-8");
    }

    /** Wipes the input and output. */
    public void reset() {
        buffer.reset();
        textInput.clear();
        secretInput.clear();
    }
}
