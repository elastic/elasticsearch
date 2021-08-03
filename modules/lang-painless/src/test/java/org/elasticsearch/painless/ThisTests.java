/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ThisTests extends ScriptTestCase {

    public abstract static class TestThisScript {

        protected String testString;

        public TestThisScript(String testString) {
            this.testString = testString;
        }

        public String getTestString() {
            return testString;
        }

        public void setTestString(String testString) {
            this.testString = testString;
        }

        public int getTestLength() {
            return testString.length();
        }

        public abstract Object execute();

        public interface Factory {

            TestThisScript newInstance(String testString);
        }

        public static final String[] PARAMETERS = {};
        public static final ScriptContext<TestThisScript.Factory> CONTEXT =
                new ScriptContext<>("this_test", TestThisScript.Factory.class);
    }

    @Override
    protected Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = new HashMap<>();
        List<Whitelist> whitelists = new ArrayList<>(Whitelist.BASE_WHITELISTS);
        whitelists.add(WhitelistLoader.loadFromResourceFiles(Whitelist.class, "org.elasticsearch.painless.this"));
        contexts.put(TestThisScript.CONTEXT, whitelists);
        return contexts;
    }

    public Object exec(String script, String testString) {
        TestThisScript.Factory factory = scriptEngine.compile(null, script, TestThisScript.CONTEXT, new HashMap<>());
        TestThisScript testThisScript = factory.newInstance(testString);
        return testThisScript.execute();
    }

    public void testThisMethods() {
        assertEquals("test string", exec("List x = []; x.add(getTestString()); x[0];", "test string"));
    }
}
