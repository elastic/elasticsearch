/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;

public class RegexLimitTests extends ScriptTestCase {
    // This regex has backtracking due to .*?
    private final String pattern = "/abc.*?def/";
    private final String charSequence = "'abcdodef'";
    private final String splitCharSequence = "'0-abc-1-def-X-abc-2-def-Y-abc-3-def-Z-abc'";
    private final String regexCircuitMessage = "[scripting] Regular expression considered too many characters";

    public void testRegexInject_Matcher() {
        String[] scripts = new String[] {
            pattern + ".matcher(" + charSequence + ").matches()",
            "Matcher m = " + pattern + ".matcher(" + charSequence + "); m.matches()" };
        for (String script : scripts) {
            setRegexLimitFactor(2);
            assertEquals(Boolean.TRUE, exec(script));

            // Backtracking means the regular expression will fail with limit factor 1 (don't consider more than each char once)
            setRegexLimitFactor(1);
            CircuitBreakingException cbe = expectScriptThrows(CircuitBreakingException.class, () -> exec(script));
            assertTrue(cbe.getMessage().contains(regexCircuitMessage));
        }
    }

    public void testRegexInjectUnlimited_Matcher() {
        String[] scripts = new String[] {
            pattern + ".matcher(" + charSequence + ").matches()",
            "Matcher m = " + pattern + ".matcher(" + charSequence + "); m.matches()" };
        for (String script : scripts) {
            setRegexEnabled();
            assertEquals(Boolean.TRUE, exec(script));
        }
    }

    public void testRegexInject_Def_Matcher() {
        String[] scripts = new String[] {
            "def p = " + pattern + "; p.matcher(" + charSequence + ").matches()",
            "def p = " + pattern + "; def m = p.matcher(" + charSequence + "); m.matches()" };
        for (String script : scripts) {
            setRegexLimitFactor(2);
            assertEquals(Boolean.TRUE, exec(script));

            setRegexLimitFactor(1);
            CircuitBreakingException cbe = expectScriptThrows(CircuitBreakingException.class, () -> exec(script));
            assertTrue(cbe.getMessage().contains(regexCircuitMessage));
        }
    }

    public void testMethodRegexInject_Ref_Matcher() {
        String script = "boolean isMatch(Function func) { func.apply("
            + charSequence
            + ").matches(); } "
            + "Pattern pattern = "
            + pattern
            + ";"
            + "isMatch(pattern::matcher)";
        setRegexLimitFactor(2);
        assertEquals(Boolean.TRUE, exec(script));

        setRegexLimitFactor(1);
        CircuitBreakingException cbe = expectScriptThrows(CircuitBreakingException.class, () -> exec(script));
        assertTrue(cbe.getMessage().contains(regexCircuitMessage));
    }

    public void testRegexInject_DefMethodRef_Matcher() {
        String script = "boolean isMatch(Function func) { func.apply("
            + charSequence
            + ").matches(); } "
            + "def pattern = "
            + pattern
            + ";"
            + "isMatch(pattern::matcher)";
        setRegexLimitFactor(2);
        assertEquals(Boolean.TRUE, exec(script));

        setRegexLimitFactor(1);
        CircuitBreakingException cbe = expectScriptThrows(CircuitBreakingException.class, () -> exec(script));
        assertTrue(cbe.getMessage().contains(regexCircuitMessage));
    }

    public void testRegexInject_SplitLimit() {
        String[] scripts = new String[] {
            pattern + ".split(" + splitCharSequence + ", 2)",
            "Pattern p = " + pattern + "; p.split(" + splitCharSequence + ", 2)" };
        for (String script : scripts) {
            setRegexLimitFactor(2);
            assertArrayEquals(new String[] { "0-", "-X-abc-2-def-Y-abc-3-def-Z-abc" }, (String[]) exec(script));

            setRegexLimitFactor(1);
            CircuitBreakingException cbe = expectScriptThrows(CircuitBreakingException.class, () -> exec(script));
            assertTrue(cbe.getMessage().contains(regexCircuitMessage));
        }
    }

    public void testRegexInjectUnlimited_SplitLimit() {
        String[] scripts = new String[] {
            pattern + ".split(" + splitCharSequence + ", 2)",
            "Pattern p = " + pattern + "; p.split(" + splitCharSequence + ", 2)" };
        for (String script : scripts) {
            setRegexEnabled();
            assertArrayEquals(new String[] { "0-", "-X-abc-2-def-Y-abc-3-def-Z-abc" }, (String[]) exec(script));
        }
    }

    public void testRegexInject_Def_SplitLimit() {
        String script = "def p = " + pattern + "; p.split(" + splitCharSequence + ", 2)";
        setRegexLimitFactor(2);
        assertArrayEquals(new String[] { "0-", "-X-abc-2-def-Y-abc-3-def-Z-abc" }, (String[]) exec(script));

        setRegexLimitFactor(1);
        CircuitBreakingException cbe = expectScriptThrows(CircuitBreakingException.class, () -> exec(script));
        assertTrue(cbe.getMessage().contains(regexCircuitMessage));
    }

    public void testRegexInject_Ref_SplitLimit() {
        String script = "String[] splitLimit(BiFunction func) { func.apply("
            + splitCharSequence
            + ", 2); } "
            + "Pattern pattern = "
            + pattern
            + ";"
            + "splitLimit(pattern::split)";
        setRegexLimitFactor(2);
        assertArrayEquals(new String[] { "0-", "-X-abc-2-def-Y-abc-3-def-Z-abc" }, (String[]) exec(script));

        setRegexLimitFactor(1);
        CircuitBreakingException cbe = expectScriptThrows(CircuitBreakingException.class, () -> exec(script));
        assertTrue(cbe.getMessage().contains(regexCircuitMessage));
    }

    public void testRegexInject_DefMethodRef_SplitLimit() {
        String script = "String[] splitLimit(BiFunction func) { func.apply("
            + splitCharSequence
            + ", 2); } "
            + "def pattern = "
            + pattern
            + ";"
            + "splitLimit(pattern::split)";
        setRegexLimitFactor(2);
        assertArrayEquals(new String[] { "0-", "-X-abc-2-def-Y-abc-3-def-Z-abc" }, (String[]) exec(script));

        setRegexLimitFactor(1);
        CircuitBreakingException cbe = expectScriptThrows(CircuitBreakingException.class, () -> exec(script));
        assertTrue(cbe.getMessage().contains(regexCircuitMessage));
    }

    public void testRegexInject_Split() {
        String[] scripts = new String[] {
            pattern + ".split(" + splitCharSequence + ")",
            "Pattern p = " + pattern + "; p.split(" + splitCharSequence + ")" };
        for (String script : scripts) {
            setRegexLimitFactor(2);
            assertArrayEquals(new String[] { "0-", "-X-", "-Y-", "-Z-abc" }, (String[]) exec(script));

            setRegexLimitFactor(1);
            CircuitBreakingException cbe = expectScriptThrows(CircuitBreakingException.class, () -> exec(script));
            assertTrue(cbe.getMessage().contains(regexCircuitMessage));
        }
    }

    public void testRegexInjectUnlimited_Split() {
        String[] scripts = new String[] {
            pattern + ".split(" + splitCharSequence + ")",
            "Pattern p = " + pattern + "; p.split(" + splitCharSequence + ")" };
        for (String script : scripts) {
            setRegexEnabled();
            assertArrayEquals(new String[] { "0-", "-X-", "-Y-", "-Z-abc" }, (String[]) exec(script));
        }
    }

    public void testRegexInject_Def_Split() {
        String script = "def p = " + pattern + "; p.split(" + splitCharSequence + ")";
        setRegexLimitFactor(2);
        assertArrayEquals(new String[] { "0-", "-X-", "-Y-", "-Z-abc" }, (String[]) exec(script));

        setRegexLimitFactor(1);
        CircuitBreakingException cbe = expectScriptThrows(CircuitBreakingException.class, () -> exec(script));
        assertTrue(cbe.getMessage().contains(regexCircuitMessage));
    }

    public void testRegexInject_Ref_Split() {
        String script = "String[] split(Function func) { func.apply("
            + splitCharSequence
            + "); } "
            + "Pattern pattern = "
            + pattern
            + ";"
            + "split(pattern::split)";
        setRegexLimitFactor(2);
        assertArrayEquals(new String[] { "0-", "-X-", "-Y-", "-Z-abc" }, (String[]) exec(script));

        setRegexLimitFactor(1);
        CircuitBreakingException cbe = expectScriptThrows(CircuitBreakingException.class, () -> exec(script));
        assertTrue(cbe.getMessage().contains(regexCircuitMessage));
    }

    public void testRegexInject_DefMethodRef_Split() {
        String script = "String[] split(Function func) { func.apply("
            + splitCharSequence
            + "); } "
            + "def pattern = "
            + pattern
            + ";"
            + "split(pattern::split)";
        setRegexLimitFactor(2);
        assertArrayEquals(new String[] { "0-", "-X-", "-Y-", "-Z-abc" }, (String[]) exec(script));

        setRegexLimitFactor(1);
        CircuitBreakingException cbe = expectScriptThrows(CircuitBreakingException.class, () -> exec(script));
        assertTrue(cbe.getMessage().contains(regexCircuitMessage));
    }

    public void testRegexInject_SplitAsStream() {
        String[] scripts = new String[] {
            pattern + ".splitAsStream(" + splitCharSequence + ").toArray(String[]::new)",
            "Pattern p = " + pattern + "; p.splitAsStream(" + splitCharSequence + ").toArray(String[]::new)" };
        for (String script : scripts) {
            setRegexLimitFactor(2);
            assertArrayEquals(new String[] { "0-", "-X-", "-Y-", "-Z-abc" }, (String[]) exec(script));

            setRegexLimitFactor(1);
            CircuitBreakingException cbe = expectScriptThrows(CircuitBreakingException.class, () -> exec(script));
            assertTrue(cbe.getMessage().contains(regexCircuitMessage));
        }
    }

    public void testRegexInjectUnlimited_SplitAsStream() {
        String[] scripts = new String[] {
            pattern + ".splitAsStream(" + splitCharSequence + ").toArray(String[]::new)",
            "Pattern p = " + pattern + "; p.splitAsStream(" + splitCharSequence + ").toArray(String[]::new)" };
        for (String script : scripts) {
            setRegexEnabled();
            assertArrayEquals(new String[] { "0-", "-X-", "-Y-", "-Z-abc" }, (String[]) exec(script));
        }
    }

    public void testRegexInject_Def_SplitAsStream() {
        String script = "def p = " + pattern + "; p.splitAsStream(" + splitCharSequence + ").toArray(String[]::new)";
        setRegexLimitFactor(2);
        assertArrayEquals(new String[] { "0-", "-X-", "-Y-", "-Z-abc" }, (String[]) exec(script));

        setRegexLimitFactor(1);
        CircuitBreakingException cbe = expectScriptThrows(CircuitBreakingException.class, () -> exec(script));
        assertTrue(cbe.getMessage().contains(regexCircuitMessage));
    }

    public void testRegexInject_Ref_SplitAsStream() {
        String script = "Stream splitStream(Function func) { func.apply("
            + splitCharSequence
            + "); } "
            + "Pattern pattern = "
            + pattern
            + ";"
            + "splitStream(pattern::splitAsStream).toArray(String[]::new)";
        setRegexLimitFactor(2);
        assertArrayEquals(new String[] { "0-", "-X-", "-Y-", "-Z-abc" }, (String[]) exec(script));

        setRegexLimitFactor(1);
        CircuitBreakingException cbe = expectScriptThrows(CircuitBreakingException.class, () -> exec(script));
        assertTrue(cbe.getMessage().contains(regexCircuitMessage));
    }

    public void testRegexInject_DefMethodRef_SplitAsStream() {
        String script = "Stream splitStream(Function func) { func.apply("
            + splitCharSequence
            + "); } "
            + "def pattern = "
            + pattern
            + ";"
            + "splitStream(pattern::splitAsStream).toArray(String[]::new)";
        setRegexLimitFactor(2);
        assertArrayEquals(new String[] { "0-", "-X-", "-Y-", "-Z-abc" }, (String[]) exec(script));

        setRegexLimitFactor(1);
        CircuitBreakingException cbe = expectScriptThrows(CircuitBreakingException.class, () -> exec(script));
        assertTrue(cbe.getMessage().contains(regexCircuitMessage));
    }

    public void testRegexInjectFindOperator() {
        String script = "if (" + charSequence + " =~ " + pattern + ") { return 100; } return 200";
        setRegexLimitFactor(2);
        assertEquals(Integer.valueOf(100), (Integer) exec(script));

        setRegexLimitFactor(1);
        CircuitBreakingException cbe = expectScriptThrows(CircuitBreakingException.class, () -> exec(script));
        assertTrue(cbe.getMessage().contains(regexCircuitMessage));
    }

    public void testRegexInjectMatchOperator() {
        String script = "if (" + charSequence + " ==~ " + pattern + ") { return 100; } return 200";
        setRegexLimitFactor(2);
        assertEquals(Integer.valueOf(100), (Integer) exec(script));

        setRegexLimitFactor(1);
        CircuitBreakingException cbe = expectScriptThrows(CircuitBreakingException.class, () -> exec(script));
        assertTrue(cbe.getMessage().contains(regexCircuitMessage));
    }

    public void testSnippetRegex() {
        String longCharSequence = "abcdef123456".repeat(100);
        String script = "if ('" + longCharSequence + "' ==~ " + pattern + ") { return 100; } return 200";

        setRegexLimitFactor(1);
        CircuitBreakingException cbe = expectScriptThrows(CircuitBreakingException.class, () -> exec(script));
        assertTrue(cbe.getMessage().contains(regexCircuitMessage));
        assertTrue(cbe.getMessage().contains(longCharSequence.subSequence(0, 61) + "..."));
    }

    private void setRegexLimitFactor(int factor) {
        Settings settings = Settings.builder().put(CompilerSettings.REGEX_LIMIT_FACTOR.getKey(), factor).build();
        scriptEngine = new PainlessScriptEngine(settings, scriptContexts());
    }

    private void setRegexEnabled() {
        Settings settings = Settings.builder().put(CompilerSettings.REGEX_ENABLED.getKey(), "true").build();
        scriptEngine = new PainlessScriptEngine(settings, scriptContexts());
    }
}
