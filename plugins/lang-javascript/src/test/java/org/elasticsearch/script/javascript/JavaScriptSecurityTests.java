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

package org.elasticsearch.script.javascript;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.mozilla.javascript.EcmaError;
import org.mozilla.javascript.WrappedException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for the Javascript security permissions
 */
public class JavaScriptSecurityTests extends ESTestCase {

    private JavaScriptScriptEngineService se;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        se = new JavaScriptScriptEngineService(Settings.Builder.EMPTY_SETTINGS);
        // otherwise will exit your VM and other bad stuff
        assumeTrue("test requires security manager to be enabled", System.getSecurityManager() != null);
    }

    @Override
    public void tearDown() throws Exception {
        se.close();
        super.tearDown();
    }

    /** runs a script */
    private void doTest(String script) {
        Map<String, Object> vars = new HashMap<String, Object>();
        se.executable(new CompiledScript(ScriptService.ScriptType.INLINE, "test", "js", se.compile(null, script, Collections.emptyMap())), vars).run();
    }

    /** asserts that a script runs without exception */
    private void assertSuccess(String script) {
        doTest(script);
    }

    /** assert that a security exception is hit */
    private void assertFailure(String script, Class<? extends Throwable> exceptionClass) {
        try {
            doTest(script);
            fail("did not get expected exception");
        } catch (WrappedException expected) {
            Throwable cause = expected.getCause();
            assertNotNull(cause);
            if (exceptionClass.isAssignableFrom(cause.getClass()) == false) {
                throw new AssertionError("unexpected exception: " + expected, expected);
            }
        } catch (EcmaError expected) {
            if (exceptionClass.isAssignableFrom(expected.getClass()) == false) {
                throw new AssertionError("unexpected exception: " + expected, expected);
            }
        }
    }

    /** Test some javascripts that are ok */
    public void testOK() {
        assertSuccess("1 + 2");
        assertSuccess("Math.cos(Math.PI)");
        assertSuccess("Array.apply(null, Array(100)).map(function (_, i) {return i;}).map(function (i) {return i+1;})");
    }

    /** Test some javascripts that should hit security exception */
    public void testNotOK() throws Exception {
        // sanity check :)
        assertFailure("java.lang.Runtime.getRuntime().halt(0)", EcmaError.class);
        // check a few things more restrictive than the ordinary policy
        // no network
        assertFailure("new java.net.Socket(\"localhost\", 1024)", EcmaError.class);
        // no files
        assertFailure("java.io.File.createTempFile(\"test\", \"tmp\")", EcmaError.class);
    }

    public void testDefinitelyNotOK() {
        // no mucking with security controller
        assertFailure("var ctx = org.mozilla.javascript.Context.getCurrentContext(); " +
                      "ctx.setSecurityController(new org.mozilla.javascript.PolicySecurityController());", EcmaError.class);
        // no compiling scripts from scripts
        assertFailure("var ctx = org.mozilla.javascript.Context.getCurrentContext(); " +
                      "ctx.compileString(\"1 + 1\", \"foobar\", 1, null); ", EcmaError.class);
    }
}
