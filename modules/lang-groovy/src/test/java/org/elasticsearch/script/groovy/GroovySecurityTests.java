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
import org.codehaus.groovy.control.MultipleCompilationErrorsException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;

import groovy.lang.MissingPropertyException;

import java.nio.file.Path;
import java.security.PrivilegedActionException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
        se = new GroovyScriptEngineService(Settings.EMPTY);
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
        // field access (via map)
        assertSuccess("def foo = doc['foo'].value; if (foo == null) { return 5; }");
        // field access (via list)
        assertSuccess("def foo = mylist[0]; if (foo == null) { return 5; }");
        // field access (via array)
        assertSuccess("def foo = myarray[0]; if (foo == null) { return 5; }");
        // field access (via object)
        assertSuccess("def foo = myobject.primitive.toString(); if (foo == null) { return 5; }");
        assertSuccess("def foo = myobject.object.toString(); if (foo == null) { return 5; }");
        assertSuccess("def foo = myobject.list[0].primitive.toString(); if (foo == null) { return 5; }");
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
        assertFailure("pr = Runtime.getRuntime().exec(\"touch /tmp/gotcha\"); pr.waitFor()", MissingPropertyException.class);

        // infamous:
        assertFailure("java.lang.Math.class.forName(\"java.lang.Runtime\")", PrivilegedActionException.class);
        // filtered directly by our classloader
        assertFailure("getClass().getClassLoader().loadClass(\"java.lang.Runtime\").availableProcessors()", PrivilegedActionException.class);
        // unfortunately, we have access to other classloaders (due to indy mechanism needing getClassLoader permission)
        // but we can't do much with them directly at least. 
        assertFailure("myobject.getClass().getClassLoader().loadClass(\"java.lang.Runtime\").availableProcessors()", SecurityException.class);
        assertFailure("d = new DateTime(); d.getClass().getDeclaredMethod(\"year\").setAccessible(true)", SecurityException.class);
        assertFailure("d = new DateTime(); d.\"${'get' + 'Class'}\"()." +
                        "\"${'getDeclared' + 'Method'}\"(\"year\").\"${'set' + 'Accessible'}\"(false)", SecurityException.class);
        assertFailure("Class.forName(\"org.joda.time.DateTime\").getDeclaredMethod(\"year\").setAccessible(true)", MissingPropertyException.class);

        assertFailure("Eval.me('2 + 2')", MissingPropertyException.class);
        assertFailure("Eval.x(5, 'x + 2')", MissingPropertyException.class);

        assertFailure("d = new Date(); java.lang.reflect.Field f = Date.class.getDeclaredField(\"fastTime\");" +
                " f.setAccessible(true); f.get(\"fastTime\")", MultipleCompilationErrorsException.class);

        assertFailure("def methodName = 'ex'; Runtime.\"${'get' + 'Runtime'}\"().\"${methodName}ec\"(\"touch /tmp/gotcha2\")", MissingPropertyException.class);

        assertFailure("t = new Thread({ println 3 });", MultipleCompilationErrorsException.class);

        // test a directory we normally have access to, but the groovy script does not.
        Path dir = createTempDir();
        // TODO: figure out the necessary escaping for windows paths here :)
        if (!Constants.WINDOWS) {
            assertFailure("new File(\"" + dir + "\").exists()", MultipleCompilationErrorsException.class);
        }
    }

    /** runs a script */
    private void doTest(String script) {
        Map<String, Object> vars = new HashMap<String, Object>();
        // we add a "mock document" containing a single field "foo" that returns 4 (abusing a jdk class with a getValue() method)
        vars.put("doc", Collections.singletonMap("foo", new AbstractMap.SimpleEntry<Object,Integer>(null, 4)));
        vars.put("mylist", Arrays.asList("foo"));
        vars.put("myarray", Arrays.asList("foo"));
        vars.put("myobject", new MyObject());

        se.executable(new CompiledScript(ScriptService.ScriptType.INLINE, "test", "js", se.compile(script)), vars).run();
    }
    
    public static class MyObject {
        public int getPrimitive() { return 0; }
        public Object getObject() { return "value"; }
        public List<Object> getList() { return Arrays.asList(new MyObject()); }
    }

    /** asserts that a script runs without exception */
    private void assertSuccess(String script) {
        doTest(script);
    }

    /** asserts that a script triggers securityexception */
    private void assertFailure(String script, Class<? extends Throwable> exceptionClass) {
        try {
            doTest(script);
            fail("did not get expected exception");
        } catch (ScriptException expected) {
            Throwable cause = expected.getCause();
            assertNotNull(cause);
            if (exceptionClass.isAssignableFrom(cause.getClass()) == false) {
                throw new AssertionError("unexpected exception: " + cause, expected);
            }
        }
    }
}
