/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.predicate.regex;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.re2.RE2;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * Checks {@link Re2RegExp} against the RE2 test data Go's {@code regexp} package tests itself with, read as Go's
 * {@code exec_test.go} reads it: each expression, compiled with Go's {@code Perl} flags, on each string of its block,
 * where the first of the four results is whether the expression matches the whole string, which is what the automaton
 * accepts. As in Go, an expression with {@code \C}, which Go doesn't support, is skipped, and so is {@code \B} on a
 * string past ASCII, as RE2 sees a boundary between the bytes of a rune.
 */
public class Re2RegExpGoTestdataTests extends ESTestCase {

    public void testSearch() throws IOException {
        assertTestdata("re2-search.txt");
    }

    public void testExhaustive() throws IOException {
        assertTestdata("re2-exhaustive.txt.bz2");
    }

    private void assertTestdata(String file) throws IOException {
        List<String> failures = new ArrayList<>();
        int cases = 0;
        int checked = 0;
        int matches = 0;
        try (InputStream raw = getClass().getResourceAsStream(file)) {
            assertNotNull(file, raw);
            InputStream in = file.endsWith(".bz2") ? new BZip2CompressorInputStream(raw) : raw;
            Reader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            List<String> strings = new ArrayList<>();
            ArrayDeque<String> input = new ArrayDeque<>();
            boolean inStrings = false;
            String regex = null;
            CharacterRunAutomaton run = null;
            String line;
            for (int lineno = 1; (line = readLine(reader)) != null; lineno++) {
                String at = file + ":" + lineno + ": ";
                if (line.isEmpty()) {
                    fail(at + "unexpected blank line");
                } else if (line.charAt(0) == '#' || (line.charAt(0) >= 'A' && line.charAt(0) <= 'Z')) {
                    // a comment or the name of a test
                } else if (line.equals("strings")) {
                    strings = new ArrayList<>();
                    inStrings = true;
                } else if (line.equals("regexps")) {
                    inStrings = false;
                } else if (line.charAt(0) == '"') {
                    String quoted = goString(unquote(line));
                    if (inStrings) {
                        strings.add(quoted);
                        continue;
                    }
                    assertTrue(at + "out of sync: " + input.size() + " strings left before " + line, input.isEmpty());
                    regex = quoted;
                    run = null;
                    try {
                        run = new CharacterRunAutomaton(
                            Operations.determinize(new Re2RegExp(regex, RE2.PERL).toAutomaton(), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT)
                        );
                    } catch (IllegalArgumentException e) {
                        if (e.getMessage().equals("error parsing regexp: invalid escape sequence: `\\C`") == false) {
                            failures.add(at + "compile [" + regex + "]: " + e.getMessage());
                        }
                    }
                    input = new ArrayDeque<>(strings);
                } else if (line.charAt(0) == '-' || (line.charAt(0) >= '0' && line.charAt(0) <= '9')) {
                    cases++;
                    if (run == null) {
                        // the expression didn't compile
                        input.poll();
                        continue;
                    }
                    assertFalse(at + "out of sync: no input remaining", input.isEmpty());
                    String text = input.poll();
                    if (isAscii(text) == false && regex.contains("\\B")) {
                        continue;
                    }
                    String[] results = line.split(";");
                    assertEquals(at + "results", 4, results.length);
                    boolean expected = results[0].equals("-") == false;
                    checked++;
                    matches += expected ? 1 : 0;
                    if (run.run(text) != expected) {
                        failures.add(at + "[" + regex + "] on [" + text + "]: " + (expected ? "should match" : "should not match"));
                    }
                } else {
                    fail(at + "out of sync: " + line);
                }
            }
            assertTrue(file + ": out of sync: " + input.size() + " strings left at EOF", input.isEmpty());
        }
        logger.info("{}: {} cases, {} checked, of which {} match", file, cases, checked, matches);
        assertTrue(failures.size() + " failures, the first: " + failures.subList(0, Math.min(failures.size(), 20)), failures.isEmpty());
    }

    /**
     * A line as Go's {@code bufio.ScanLines} reads it: up to a newline, without a carriage return right before it. A
     * carriage return anywhere else is part of the line, as the data has some in its strings.
     */
    private static String readLine(Reader reader) throws IOException {
        StringBuilder line = new StringBuilder();
        int c;
        while ((c = reader.read()) != -1 && c != '\n') {
            line.append((char) c);
        }
        if (c == -1 && line.isEmpty()) {
            return null;
        }
        if (line.isEmpty() == false && line.charAt(line.length() - 1) == '\r') {
            line.setLength(line.length() - 1);
        }
        return line.toString();
    }

    private static boolean isAscii(String text) {
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) >= 0x80) {
                return false;
            }
        }
        return true;
    }

    /** The bytes of a string Go's {@code strconv.Quote} quoted. */
    private static byte[] unquote(String quoted) {
        assertTrue(quoted, quoted.length() >= 2 && quoted.charAt(0) == '"' && quoted.charAt(quoted.length() - 1) == '"');
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        for (int i = 1; i < quoted.length() - 1;) {
            int c = quoted.codePointAt(i);
            if (c != '\\') {
                bytes.writeBytes(Character.toString(c).getBytes(StandardCharsets.UTF_8));
                i += Character.charCount(c);
                continue;
            }
            char e = quoted.charAt(i + 1);
            i += 2;
            switch (e) {
                case 'a' -> bytes.write(7);
                case 'b' -> bytes.write('\b');
                case 'f' -> bytes.write('\f');
                case 'n' -> bytes.write('\n');
                case 'r' -> bytes.write('\r');
                case 't' -> bytes.write('\t');
                case 'v' -> bytes.write(11);
                case '\\', '"', '\'' -> bytes.write(e);
                case 'x' -> {
                    bytes.write(Integer.parseInt(quoted.substring(i, i + 2), 16));
                    i += 2;
                }
                case 'u' -> {
                    bytes.writeBytes(Character.toString(Integer.parseInt(quoted.substring(i, i + 4), 16)).getBytes(StandardCharsets.UTF_8));
                    i += 4;
                }
                case 'U' -> {
                    bytes.writeBytes(Character.toString(Integer.parseInt(quoted.substring(i, i + 8), 16)).getBytes(StandardCharsets.UTF_8));
                    i += 8;
                }
                case '0', '1', '2', '3', '4', '5', '6', '7' -> {
                    bytes.write(Integer.parseInt(quoted.substring(i - 1, i + 2), 8));
                    i += 2;
                }
                default -> throw new AssertionError("unknown escape [\\" + e + "] in " + quoted);
            }
        }
        return bytes.toByteArray();
    }

    /**
     * The runes of {@code bytes} as Go's {@code regexp} reads them: a byte that doesn't start a valid UTF-8 encoding is
     * {@code U+FFFD} on its own, as {@code utf8.DecodeRune} has it.
     */
    private static String goString(byte[] bytes) {
        StringBuilder runes = new StringBuilder();
        for (int i = 0; i < bytes.length;) {
            int b0 = bytes[i] & 0xFF;
            int length;
            int min;
            int max;
            if (b0 < 0x80) {
                runes.append((char) b0);
                i++;
                continue;
            } else if (b0 >= 0xC2 && b0 <= 0xDF) {
                length = 2;
                min = 0x80;
                max = 0xBF;
            } else if (b0 >= 0xE0 && b0 <= 0xEF) {
                length = 3;
                min = b0 == 0xE0 ? 0xA0 : 0x80;
                max = b0 == 0xED ? 0x9F : 0xBF;
            } else if (b0 >= 0xF0 && b0 <= 0xF4) {
                length = 4;
                min = b0 == 0xF0 ? 0x90 : 0x80;
                max = b0 == 0xF4 ? 0x8F : 0xBF;
            } else {
                runes.append('�');
                i++;
                continue;
            }
            boolean valid = i + length <= bytes.length;
            for (int k = 1; valid && k < length; k++) {
                int b = bytes[i + k] & 0xFF;
                valid = k == 1 ? b >= min && b <= max : b >= 0x80 && b <= 0xBF;
            }
            if (valid == false) {
                runes.append('�');
                i++;
                continue;
            }
            runes.append(new String(bytes, i, length, StandardCharsets.UTF_8));
            i += length;
        }
        return runes.toString();
    }
}
