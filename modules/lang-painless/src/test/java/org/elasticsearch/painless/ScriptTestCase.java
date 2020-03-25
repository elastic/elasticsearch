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

package org.elasticsearch.painless;

import junit.framework.AssertionFailedError;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.painless.antlr.Walker;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.painless.action.PainlessExecuteAction.PainlessTestScript;
import static org.hamcrest.Matchers.hasSize;

/**
 * Base test case for scripting unit tests.
 * <p>
 * Typically just asserts the output of {@code exec()}
 */
public abstract class ScriptTestCase extends ESTestCase {
    protected PainlessScriptEngine scriptEngine;

    @Before
    public void setup() {
        scriptEngine = new PainlessScriptEngine(scriptEngineSettings(), scriptContexts());
    }

    /**
     * Settings used to build the script engine. Override to customize settings like {@link RegexTests} does to enable regexes.
     */
    protected Settings scriptEngineSettings() {
        return Settings.EMPTY;
    }

    /**
     * Script contexts used to build the script engine. Override to customize which script contexts are available.
     */
    protected Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = new HashMap<>();
        List<Whitelist> whitelists = new ArrayList<>(Whitelist.BASE_WHITELISTS);
        whitelists.add(WhitelistLoader.loadFromResourceFiles(Whitelist.class, "org.elasticsearch.painless.test"));
        contexts.put(PainlessTestScript.CONTEXT, whitelists);
        return contexts;
    }

    /** Compiles and returns the result of {@code script} */
    public Object exec(String script) {
        return exec(script, null, true);
    }

    /** Compiles and returns the result of {@code script} with access to {@code picky} */
    public Object exec(String script, boolean picky) {
        return exec(script, null, picky);
    }

    /** Compiles and returns the result of {@code script} with access to {@code vars} */
    public Object exec(String script, Map<String, Object> vars, boolean picky) {
        Map<String,String> compilerSettings = new HashMap<>();
        compilerSettings.put(CompilerSettings.INITIAL_CALL_SITE_DEPTH, random().nextBoolean() ? "0" : "10");
        return exec(script, vars, compilerSettings, picky);
    }

    /** Compiles and returns the result of {@code script} with access to {@code vars} and compile-time parameters */
    public Object exec(String script, Map<String, Object> vars, Map<String,String> compileParams, boolean picky) {
        // test for ambiguity errors before running the actual script if picky is true
        if (picky) {
            ScriptClassInfo scriptClassInfo =
                    new ScriptClassInfo(scriptEngine.getContextsToLookups().get(PainlessTestScript.CONTEXT), PainlessTestScript.class);
            CompilerSettings pickySettings = new CompilerSettings();
            pickySettings.setPicky(true);
            pickySettings.setRegexesEnabled(CompilerSettings.REGEX_ENABLED.get(scriptEngineSettings()));
            Walker.buildPainlessTree(scriptClassInfo, getTestName(), script, pickySettings,
                    scriptEngine.getContextsToLookups().get(PainlessTestScript.CONTEXT), null);
        }
        // test actual script execution
        PainlessTestScript.Factory factory = scriptEngine.compile(null, script, PainlessTestScript.CONTEXT, compileParams);
        PainlessTestScript testScript = factory.newInstance(vars == null ? Collections.emptyMap() : vars);
        return testScript.execute();
    }

    /**
     * Uses the {@link Debugger} to get the bytecode output for a script and compare
     * it against an expected bytecode passed in as a String.
     */
    public void assertBytecodeExists(String script, String bytecode) {
        final String asm = Debugger.toString(script);
        assertTrue("bytecode not found, got: \n" + asm , asm.contains(bytecode));
    }

    /**
     * Uses the {@link Debugger} to get the bytecode output for a script and compare
     * it against an expected bytecode pattern as a regular expression (please try to avoid!)
     */
    public void assertBytecodeHasPattern(String script, String pattern) {
        final String asm = Debugger.toString(script);
        assertTrue("bytecode not found, got: \n" + asm , asm.matches(pattern));
    }

    /** Checks a specific exception class is thrown (boxed inside ScriptException) and returns it. */
    public static <T extends Throwable> T expectScriptThrows(Class<T> expectedType, ThrowingRunnable runnable) {
        return expectScriptThrows(expectedType, true, runnable);
    }

    /** Checks a specific exception class is thrown (boxed inside ScriptException) and returns it. */
    public static <T extends Throwable> T expectScriptThrows(Class<T> expectedType, boolean shouldHaveScriptStack,
            ThrowingRunnable runnable) {
        try {
            runnable.run();
        } catch (Throwable e) {
            if (e instanceof ScriptException) {
                boolean hasEmptyScriptStack = ((ScriptException) e).getScriptStack().isEmpty();
                if (shouldHaveScriptStack && hasEmptyScriptStack) {
                    /* If this fails you *might* be missing -XX:-OmitStackTraceInFastThrow in the test jvm
                     * In Eclipse you can add this by default by going to Preference->Java->Installed JREs,
                     * clicking on the default JRE, clicking edit, and adding the flag to the
                     * "Default VM Arguments". */
                    AssertionFailedError assertion = new AssertionFailedError("ScriptException should have a scriptStack");
                    assertion.initCause(e);
                    throw assertion;
                } else if (false == shouldHaveScriptStack && false == hasEmptyScriptStack) {
                    AssertionFailedError assertion = new AssertionFailedError("ScriptException shouldn't have a scriptStack");
                    assertion.initCause(e);
                    throw assertion;
                }
                e = e.getCause();
                if (expectedType.isInstance(e)) {
                    return expectedType.cast(e);
                }
            } else {
                AssertionFailedError assertion = new AssertionFailedError("Expected boxed ScriptException");
                assertion.initCause(e);
                throw assertion;
            }
            AssertionFailedError assertion = new AssertionFailedError("Unexpected exception type, expected "
                                                                      + expectedType.getSimpleName());
            assertion.initCause(e);
            throw assertion;
        }
        throw new AssertionFailedError("Expected exception " + expectedType.getSimpleName());
    }

    /**
     * Asserts that the script_stack looks right.
     */
    public static void assertScriptStack(ScriptException e, String... stack) {
        // This particular incantation of assertions makes the error messages more useful
        try {
            assertThat(e.getScriptStack(), hasSize(stack.length));
            for (int i = 0; i < stack.length; i++) {
                assertEquals(stack[i], e.getScriptStack().get(i));
            }
        } catch (AssertionError assertion) {
            assertion.initCause(e);
            throw assertion;
        }
    }

}
