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

package org.elasticsearch.script;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.CoreMatchers.equalTo;

/**
 * Tests for the Groovy scripting sandbox
 */
public class GroovySandboxScriptTests extends ElasticsearchIntegrationTest {

    @Test
    public void testSandboxedGroovyScript() {
        client().prepareIndex("test", "doc", "1").setSource("foo", 5).setRefresh(true).get();

        // Plain test
        testSuccess("");
        // List
        testSuccess("def list = [doc['foo'].value, 3, 4]; def v = list.get(1); list.add(10)");
        // Ranges
        testSuccess("def range = 1..doc['foo'].value; def v = range.get(0)");
        // Maps
        testSuccess("def v = doc['foo'].value; def m = [:]; m.put(\\\"value\\\", v)");
        // Times
        testSuccess("def t = Instant.now().getMillis()");
        // GroovyCollections
        testSuccess("def n = [1,2,3]; GroovyCollections.max(n)");

        // Fail cases
        testFailure("pr = Runtime.getRuntime().exec(\\\"touch /tmp/gotcha\\\"); pr.waitFor()",
                "Method calls not allowed on [java.lang.Runtime]");

        testFailure("d = new DateTime(); d.getClass().getDeclaredMethod(\\\"plus\\\").setAccessible(true)",
                "Expression [MethodCallExpression] is not allowed: d.getClass()");

        testFailure("d = new DateTime(); d.\\\"${'get' + 'Class'}\\\"()." +
                        "\\\"${'getDeclared' + 'Method'}\\\"(\\\"now\\\").\\\"${'set' + 'Accessible'}\\\"(false)",
                "Expression [MethodCallExpression] is not allowed: d.$(get + Class)().$(getDeclared + Method)(now).$(set + Accessible)(false)");

        testFailure("Class.forName(\\\"DateTime\\\").getDeclaredMethod(\\\"plus\\\").setAccessible(true)",
                "Method calls not allowed on [java.lang.Class]");

        testFailure("Eval.me('2 + 2')", "Method calls not allowed on [groovy.util.Eval]");

        testFailure("Eval.x(5, 'x + 2')", "Method calls not allowed on [groovy.util.Eval]");

        testFailure("t = new java.util.concurrent.ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS, " +
                "new java.util.concurrent.LinkedBlockingQueue<Runnable>()); t.execute({ println 5 })",
                "Expression [ConstructorCallExpression] is not allowed: new java.util.concurrent.ThreadPoolExecutor");

        testFailure("d = new Date(); java.lang.reflect.Field f = Date.class.getDeclaredField(\\\"fastTime\\\");" +
                " f.setAccessible(true); f.get(\\\"fastTime\\\")",
                "Method calls not allowed on [java.lang.reflect.Field]");

        testFailure("t = new Thread({ println 3 }); t.start(); t.join()",
                "Expression [ConstructorCallExpression] is not allowed: new java.lang.Thread");

        testFailure("Thread.start({ println 4 })", "Method calls not allowed on [java.lang.Thread]");

        testFailure("import java.util.concurrent.ThreadPoolExecutor;",
                "Importing [java.util.concurrent.ThreadPoolExecutor] is not allowed");

        testFailure("s = new java.net.URL();", "Expression [ConstructorCallExpression] is not allowed: new java.net.URL()");

        testFailure("def methodName = 'ex'; Runtime.\\\"${'get' + 'Runtime'}\\\"().\\\"${methodName}ec\\\"(\\\"touch /tmp/gotcha2\\\")",
                "Expression [MethodCallExpression] is not allowed: java.lang.Runtime.$(get + Runtime)().$methodNameec(touch /tmp/gotcha2)");
    }

    public void testSuccess(String script) {
        logger.info("--> script: " + script);
        SearchResponse resp = client().prepareSearch("test")
                .setSource("{\"query\": {\"match_all\": {}}," +
                        "\"sort\":{\"_script\": {\"script\": \""+ script +
                        "; doc['foo'].value + 2\", \"type\": \"number\", \"lang\": \"groovy\"}}}").get();
        assertNoFailures(resp);
        assertThat(resp.getHits().getAt(0).getSortValues(), equalTo(new Object[]{7.0}));
    }

    public void testFailure(String script, String failMessage) {
        logger.info("--> script: " + script);
        try {
            client().prepareSearch("test")
                    .setSource("{\"query\": {\"match_all\": {}}," +
                            "\"sort\":{\"_script\": {\"script\": \""+ script +
                            "; doc['foo'].value + 2\", \"type\": \"number\", \"lang\": \"groovy\"}}}").get();
            fail("script: " + script + " failed to be caught be the sandbox!");
        } catch (SearchPhaseExecutionException e) {
            String msg = ExceptionsHelper.detailedMessage(ExceptionsHelper.unwrapCause(e));
            assertThat("script failed, but with incorrect message: " + msg, msg.contains(failMessage), equalTo(true));
        }
    }
}
