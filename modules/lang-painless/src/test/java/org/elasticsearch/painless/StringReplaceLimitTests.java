/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.common.settings.Settings;

import java.util.Collections;
import java.util.Map;

/**
 * Tests for the bounded {@code String.replace(CharSequence, CharSequence)} augmentation, which guards against scripts that grow strings
 * inside uninterruptible JDK code (the failure mode that took down a search node when a script repeatedly applied
 * {@code s.replace('A', 'AB')} in a loop).
 */
public class StringReplaceLimitTests extends ScriptTestCase {

    private static final String LIMIT_MESSAGE = "[scripting] String.replace would produce up to";

    private void setMaxStringChars(int max) {
        Settings settings = Settings.builder().put(CompilerSettings.MAX_STRING_CHARS.getKey(), max).build();
        scriptEngine = new PainlessScriptEngine(settings, scriptContexts());
    }

    public void testReplaceWithinLimit() {
        assertEquals("xByByB", exec("'xAyAyA'.replace('A', 'B')"));
    }

    public void testReplaceWithGrowthWithinLimit() {
        // 'AAA'.replace('A','AB') -> 'ABABAB' (worst case 6, well under default 1 MiB)
        assertEquals("ABABAB", exec("'AAA'.replace('A', 'AB')"));
    }

    public void testSingleReplaceOverLimitThrows() {
        // Tight cap; one replace call whose worst case (1024*2 = 2048 chars) exceeds it.
        setMaxStringChars(1024);
        // Build a 1024-char receiver in Painless via 10 doublings of "A".
        String script = "String s = 'A'; for (int i = 0; i < 10; i++) { s += s; } return s.replace('A', 'AB');";
        ErrorCauseWrapper err = expectScriptThrows(ErrorCauseWrapper.class, () -> exec(script));
        assertEquals(PainlessError.class, err.realCause.getClass());
        assertTrue("expected limit message, got: " + err.getMessage(), err.getMessage().contains(LIMIT_MESSAGE));
    }

    public void testIncidentReproducerAborts() {
        // The original incident's script: a loop of s = s.replace('A', 'AB'). Each iter adds (number of A's) chars; with a
        // tight cap the script must throw cleanly within a few iterations rather than churning for hours.
        setMaxStringChars(1024);
        String script = "String s = 'AAAAAAA'; for (int i = 0; i < 999999; i++) { s = s.replace('A', 'AB'); } return s.length();";
        ErrorCauseWrapper err = expectScriptThrows(ErrorCauseWrapper.class, () -> exec(script));
        assertEquals(PainlessError.class, err.realCause.getClass());
        assertTrue("expected limit message, got: " + err.getMessage(), err.getMessage().contains(LIMIT_MESSAGE));
    }

    public void testEmptyTargetUnderLimit() {
        // 'abc'.replace('','X') -> 'XaXbXcX'. Worst case = (3+1)*1 + 3 = 7 chars, well under default limit.
        assertEquals("XaXbXcX", exec("'abc'.replace('', 'X')"));
    }

    public void testEmptyTargetOverLimit() {
        // Tight cap forces empty-target formula to trip: receiver length 100, replacement length 100,
        // worstCase = 101*100 + 100 = 10200 > 1024.
        setMaxStringChars(1024);
        String receiver = "a".repeat(100);
        String replacement = "X".repeat(100);
        String script = "'" + receiver + "'.replace('', '" + replacement + "')";
        ErrorCauseWrapper err = expectScriptThrows(ErrorCauseWrapper.class, () -> exec(script));
        assertEquals(PainlessError.class, err.realCause.getClass());
        assertTrue("expected limit message, got: " + err.getMessage(), err.getMessage().contains(LIMIT_MESSAGE));
    }

    public void testNodeSettingTightensLimit() {
        // Lower the node-level cap; a replacement that previously fit must now fail.
        setMaxStringChars(1024);
        String receiver = "A".repeat(600);
        String script = "'" + receiver + "'.replace('A', 'AB')"; // worst case = 600*2 = 1200 > 1024
        ErrorCauseWrapper err = expectScriptThrows(ErrorCauseWrapper.class, () -> exec(script));
        assertEquals(PainlessError.class, err.realCause.getClass());
        assertTrue("expected limit message, got: " + err.getMessage(), err.getMessage().contains(LIMIT_MESSAGE));
    }

    public void testPerExecutionOverrideTightens() {
        // Default (1 MiB) would let this pass; per-execution override drops it to 1024 chars.
        String receiver = "A".repeat(600);
        String script = "'" + receiver + "'.replace('A', 'AB')";
        Map<String, String> compileParams = Map.of(
            CompilerSettings.MAX_STRING_CHARS_KEY,
            "1024",
            CompilerSettings.INITIAL_CALL_SITE_DEPTH,
            "0"
        );
        ErrorCauseWrapper err = expectScriptThrows(
            ErrorCauseWrapper.class,
            () -> exec(script, Collections.emptyMap(), compileParams, true)
        );
        assertEquals(PainlessError.class, err.realCause.getClass());
        assertTrue("expected limit message, got: " + err.getMessage(), err.getMessage().contains(LIMIT_MESSAGE));
    }

    public void testPerExecutionOverrideRelaxes() {
        // Tight node cap, but per-execution override loosens it for this one call.
        setMaxStringChars(1024);
        String receiver = "A".repeat(600);
        String script = "'" + receiver + "'.replace('A', 'AB')";
        Map<String, String> compileParams = Map.of(
            CompilerSettings.MAX_STRING_CHARS_KEY,
            "65536",
            CompilerSettings.INITIAL_CALL_SITE_DEPTH,
            "0"
        );
        Object result = exec(script, Collections.emptyMap(), compileParams, true);
        assertEquals("AB".repeat(600), result);
    }
}
