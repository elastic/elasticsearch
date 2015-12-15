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

package org.elasticsearch.script.python;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.python.core.PyException;

import java.text.DecimalFormatSymbols;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Tests for Python security permissions
 */
public class PythonSecurityTests extends ESTestCase {
    
    private PythonScriptEngineService se;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        se = new PythonScriptEngineService(Settings.Builder.EMPTY_SETTINGS);
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
        se.executable(new CompiledScript(ScriptService.ScriptType.INLINE, "test", "python", se.compile(script)), vars).run();
    }
    
    /** asserts that a script runs without exception */
    private void assertSuccess(String script) {
        doTest(script);
    }
    
    /** assert that a security exception is hit */
    private void assertFailure(String script) {
        try {
            doTest(script);
            fail("did not get expected exception");
        } catch (PyException expected) {
            // TODO: fix jython localization bugs: https://github.com/elastic/elasticsearch/issues/13967
            // we do a gross hack for now
            DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance(Locale.getDefault());
            if (symbols.getZeroDigit() == '0') {
                assertTrue(expected.toString().contains("cannot import"));
            }
        }
    }
    
    /** Test some py scripts that are ok */
    public void testOK() {
        assertSuccess("1 + 2");
        assertSuccess("from java.lang import Math\nMath.cos(0)");
    }
    
    /** Test some py scripts that should hit security exception */
    public void testNotOK() {
        // sanity check :)
        assertFailure("from java.lang import Runtime\nRuntime.getRuntime().halt(0)");
        // check a few things more restrictive than the ordinary policy
        // no network
        assertFailure("from java.net import Socket\nSocket(\"localhost\", 1024)");
        // no files
        assertFailure("from java.io import File\nFile.createTempFile(\"test\", \"tmp\")");
    }
    
    /** Test again from a new thread, python has complex threadlocal configuration */
    public void testNotOKFromSeparateThread() throws Exception {
        Thread t = new Thread() {
            @Override
            public void run() {
                assertFailure("from java.lang import Runtime\nRuntime.availableProcessors()");
            }
        };
        t.start();
        t.join();
    }
}
