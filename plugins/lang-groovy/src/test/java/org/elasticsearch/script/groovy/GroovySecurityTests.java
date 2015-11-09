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

package org.elasticsearch.script.groovy;

import org.apache.lucene.util.Constants;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for the Groovy security permissions
 */
public class GroovySecurityTests extends ESTestCase {

    private GroovyScriptEngineService se;

    static {
        // ensure we load all the timezones in the parent classloader with all permissions
        // relates to https://github.com/elastic/elasticsearch/issues/14524
        org.joda.time.DateTimeZone.getDefault();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        se = new GroovyScriptEngineService(Settings.Builder.EMPTY_SETTINGS);
        // otherwise will exit your VM and other bad stuff
        assumeTrue("test requires security manager to be enabled", System.getSecurityManager() != null);
    }

    @Override
    public void tearDown() throws Exception {
        se.close();
        super.tearDown();
    }

    public void testEvilGroovyScripts() throws Exception {
        // Plain test
        assertSuccess("");
        // field access
        assertSuccess("def foo = doc['foo'].value; if (foo == null) { return 5; }");
        // List
        assertSuccess("def list = [doc['foo'].value, 3, 4]; def v = list.get(1); list.add(10)");
        // Ranges
        assertSuccess("def range = 1..doc['foo'].value; def v = range.get(0)");
        // Maps
        assertSuccess("def v = doc['foo'].value; def m = [:]; m.put(\"value\", v)");
        // serialization to json (this is best effort considering the unsafe etc at play)
        assertSuccess("def x = 5; groovy.json.JsonOutput.toJson(x)");
        // Times
        assertSuccess("def t = Instant.now().getMillis()");
        // GroovyCollections
        assertSuccess("def n = [1,2,3]; GroovyCollections.max(n)");

        // Fail cases:
        // AccessControlException[access denied ("java.io.FilePermission" "<<ALL FILES>>" "execute")]
        assertFailure("pr = Runtime.getRuntime().exec(\"touch /tmp/gotcha\"); pr.waitFor()");

        // AccessControlException[access denied ("java.lang.RuntimePermission" "accessClassInPackage.sun.reflect")]
        assertFailure("d = new DateTime(); d.getClass().getDeclaredMethod(\"year\").setAccessible(true)");
        assertFailure("d = new DateTime(); d.\"${'get' + 'Class'}\"()." +
                        "\"${'getDeclared' + 'Method'}\"(\"year\").\"${'set' + 'Accessible'}\"(false)");
        assertFailure("Class.forName(\"org.joda.time.DateTime\").getDeclaredMethod(\"year\").setAccessible(true)");

        // AccessControlException[access denied ("groovy.security.GroovyCodeSourcePermission" "/groovy/shell")]
        assertFailure("Eval.me('2 + 2')");
        assertFailure("Eval.x(5, 'x + 2')");

        // AccessControlException[access denied ("java.lang.RuntimePermission" "accessDeclaredMembers")]
        assertFailure("d = new Date(); java.lang.reflect.Field f = Date.class.getDeclaredField(\"fastTime\");" +
                " f.setAccessible(true); f.get(\"fastTime\")");

        // AccessControlException[access denied ("java.io.FilePermission" "<<ALL FILES>>" "execute")]
        assertFailure("def methodName = 'ex'; Runtime.\"${'get' + 'Runtime'}\"().\"${methodName}ec\"(\"touch /tmp/gotcha2\")");

        // AccessControlException[access denied ("java.lang.RuntimePermission" "modifyThreadGroup")]
        assertFailure("t = new Thread({ println 3 });");

        // test a directory we normally have access to, but the groovy script does not.
        Path dir = createTempDir();
        // TODO: figure out the necessary escaping for windows paths here :)
        if (!Constants.WINDOWS) {
            // access denied ("java.io.FilePermission" ".../tempDir-00N" "read")
            assertFailure("new File(\"" + dir + "\").exists()");
        }
    }

    /** runs a script */
    private void doTest(String script) {
        Map<String, Object> vars = new HashMap<String, Object>();
        // we add a "mock document" containing a single field "foo" that returns 4 (abusing a jdk class with a getValue() method)
        vars.put("doc", Collections.singletonMap("foo", new AbstractMap.SimpleEntry<Object,Integer>(null, 4)));
        se.executable(new CompiledScript(ScriptService.ScriptType.INLINE, "test", "js", se.compile(script)), vars).run();
    }

    /** asserts that a script runs without exception */
    private void assertSuccess(String script) {
        doTest(script);
    }

    /** asserts that a script triggers securityexception */
    private void assertFailure(String script) {
        try {
            doTest(script);
            fail("did not get expected exception");
        } catch (ScriptException expected) {
            Throwable cause = expected.getCause();
            assertNotNull(cause);
            assertTrue("unexpected exception: " + cause, cause instanceof SecurityException);
        }
    }
}
