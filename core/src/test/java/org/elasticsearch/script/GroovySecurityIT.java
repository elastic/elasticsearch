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

import org.apache.lucene.util.Constants;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.nio.file.Path;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.CoreMatchers.equalTo;

/**
 * Tests for the Groovy security permissions
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class GroovySecurityIT extends ESIntegTestCase {
    
    @Override
    public void setUp() throws Exception {
        super.setUp();
        assumeTrue("test requires security manager to be enabled", System.getSecurityManager() != null);
    }

    @Test
    public void testEvilGroovyScripts() throws Exception {
        int nodes = randomIntBetween(1, 3);
        Settings nodeSettings = Settings.builder()
                .put("script.inline", true)
                .put("script.indexed", true)
                .build();
        internalCluster().startNodesAsync(nodes, nodeSettings).get();
        client().admin().cluster().prepareHealth().setWaitForNodes(nodes + "").get();

        client().prepareIndex("test", "doc", "1").setSource("foo", 5, "bar", "baz").setRefresh(true).get();

        // Plain test
        assertSuccess("");
        // numeric field access
        assertSuccess("def foo = doc['foo'].value; if (foo == null) { return 5; }");
        // string field access
        assertSuccess("def bar = doc['bar'].value; if (bar == null) { return 5; }");
        // List
        assertSuccess("def list = [doc['foo'].value, 3, 4]; def v = list.get(1); list.add(10)");
        // Ranges
        assertSuccess("def range = 1..doc['foo'].value; def v = range.get(0)");
        // Maps
        assertSuccess("def v = doc['foo'].value; def m = [:]; m.put(\\\"value\\\", v)");
        // Times
        assertSuccess("def t = Instant.now().getMillis()");
        // GroovyCollections
        assertSuccess("def n = [1,2,3]; GroovyCollections.max(n)");

        // Fail cases:
        // AccessControlException[access denied ("java.io.FilePermission" "<<ALL FILES>>" "execute")]
        assertFailure("pr = Runtime.getRuntime().exec(\\\"touch /tmp/gotcha\\\"); pr.waitFor()");

        // AccessControlException[access denied ("java.lang.RuntimePermission" "accessClassInPackage.sun.reflect")]
        assertFailure("d = new DateTime(); d.getClass().getDeclaredMethod(\\\"year\\\").setAccessible(true)");
        assertFailure("d = new DateTime(); d.\\\"${'get' + 'Class'}\\\"()." +
                        "\\\"${'getDeclared' + 'Method'}\\\"(\\\"year\\\").\\\"${'set' + 'Accessible'}\\\"(false)");
        assertFailure("Class.forName(\\\"org.joda.time.DateTime\\\").getDeclaredMethod(\\\"year\\\").setAccessible(true)");

        // AccessControlException[access denied ("groovy.security.GroovyCodeSourcePermission" "/groovy/shell")]
        assertFailure("Eval.me('2 + 2')");
        assertFailure("Eval.x(5, 'x + 2')");

        // AccessControlException[access denied ("java.lang.RuntimePermission" "accessDeclaredMembers")]
        assertFailure("d = new Date(); java.lang.reflect.Field f = Date.class.getDeclaredField(\\\"fastTime\\\");" +
                " f.setAccessible(true); f.get(\\\"fastTime\\\")");

        // AccessControlException[access denied ("java.io.FilePermission" "<<ALL FILES>>" "execute")]
        assertFailure("def methodName = 'ex'; Runtime.\\\"${'get' + 'Runtime'}\\\"().\\\"${methodName}ec\\\"(\\\"touch /tmp/gotcha2\\\")");
        
        // test a directory we normally have access to, but the groovy script does not.
        Path dir = createTempDir();
        // TODO: figure out the necessary escaping for windows paths here :)
        if (!Constants.WINDOWS) {
            // access denied ("java.io.FilePermission" ".../tempDir-00N" "read")
            assertFailure("new File(\\\"" + dir + "\\\").exists()");
        }
    }

    private void assertSuccess(String script) {
        logger.info("--> script: " + script);
        SearchResponse resp = client().prepareSearch("test")
                .setSource(new BytesArray("{\"query\": {\"match_all\": {}}," +
                        "\"sort\":{\"_script\": {\"script\": \"" + script +
                        "; doc['foo'].value + 2\", \"type\": \"number\", \"lang\": \"groovy\"}}}")).get();
        assertNoFailures(resp);
        assertEquals(1, resp.getHits().getTotalHits());
        assertThat(resp.getHits().getAt(0).getSortValues(), equalTo(new Object[]{7.0}));
    }

    private void assertFailure(String script) {
        logger.info("--> script: " + script);
        SearchResponse resp = client().prepareSearch("test")
                 .setSource(new BytesArray("{\"query\": {\"match_all\": {}}," +
                         "\"sort\":{\"_script\": {\"script\": \"" + script +
                         "; doc['foo'].value + 2\", \"type\": \"number\", \"lang\": \"groovy\"}}}")).get();
        assertEquals(0, resp.getHits().getTotalHits());
        ShardSearchFailure fails[] = resp.getShardFailures();
        // TODO: GroovyScriptExecutionException needs work:
        // fix it to preserve cause so we don't do this flaky string-check stuff
        for (ShardSearchFailure fail : fails) {
            assertTrue("unexpected exception" + fail.getCause(),
                       // different casing, depending on jvm impl...
                       fail.getCause().toString().toLowerCase(Locale.ROOT).contains("accesscontrolexception[access denied"));
        }
    }
}
