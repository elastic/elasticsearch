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
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.lucene.ScorerAware;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.painless.antlr.Walker;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.painless.node.SSource.MainMethodReserved;
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
    protected Collection<ScriptContext<?>> scriptContexts() {
        Collection<ScriptContext<?>> contexts = new ArrayList<>();
        contexts.add(SearchScript.CONTEXT);
        contexts.add(ExecutableScript.CONTEXT);

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
        return exec(script, vars, compilerSettings, null, picky);
    }

    /** Compiles and returns the result of {@code script} with access to {@code vars} and compile-time parameters */
    public Object exec(String script, Map<String, Object> vars, Map<String,String> compileParams, Scorer scorer, boolean picky) {
        // test for ambiguity errors before running the actual script if picky is true
        if (picky) {
            Definition definition = Definition.DEFINITION;
            ScriptClassInfo scriptClassInfo = new ScriptClassInfo(definition, GenericElasticsearchScript.class);
            CompilerSettings pickySettings = new CompilerSettings();
            pickySettings.setPicky(true);
            pickySettings.setRegexesEnabled(CompilerSettings.REGEX_ENABLED.get(scriptEngineSettings()));
            Walker.buildPainlessTree(scriptClassInfo, new MainMethodReserved(), getTestName(), script, pickySettings, definition, null);
        }
        // test actual script execution
        ExecutableScript.Factory factory = scriptEngine.compile(null, script, ExecutableScript.CONTEXT, compileParams);
        ExecutableScript executableScript = factory.newInstance(vars);
        if (scorer != null) {
            ((ScorerAware)executableScript).setScorer(scorer);
        }
        return executableScript.run();
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
