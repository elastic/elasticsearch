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

    public abstract static class ThisBaseScript {

        protected String baseString;

        public ThisBaseScript(String baseString) {
            this.baseString = baseString;
        }

        public String getBaseString() {
            return baseString;
        }

        public void setBaseString(String testString) {
            this.baseString = testString;
        }

        public int getBaseLength() {
            return baseString.length();
        }
    }

    public abstract static class ThisScript extends ThisBaseScript {

        protected String thisString;

        public ThisScript(String baseString, String thisString) {
            super(baseString);

            this.thisString = thisString;
        }

        public String thisString() {
            return thisString;
        }

        public void thisString(String testString) {
            this.thisString = testString;
        }

        public int thisLength() {
            return thisString.length();
        }

        public abstract Object execute();

        public interface Factory {

            ThisScript newInstance(String baseString, String testString);
        }

        public static final String[] PARAMETERS = {};
        public static final ScriptContext<ThisScript.Factory> CONTEXT =
                new ScriptContext<>("this_test", ThisScript.Factory.class);
    }

    @Override
    protected Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = new HashMap<>();
        List<Whitelist> whitelists = new ArrayList<>(PainlessPlugin.BASE_WHITELISTS);
        whitelists.add(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.painless.this"));
        contexts.put(ThisScript.CONTEXT, whitelists);
        return contexts;
    }

    public Object exec(String script, String baseString, String testString) {
        ThisScript.Factory factory = scriptEngine.compile(null, script, ThisScript.CONTEXT, new HashMap<>());
        ThisScript testThisScript = factory.newInstance(baseString, testString);
        return testThisScript.execute();
    }

    public void testThisMethods() {
        assertEquals("basethis", exec("getBaseString() + thisString()", "base", "this"));
        assertEquals(8, exec("getBaseLength() + thisLength()", "yyy", "xxxxx"));

        List<String> result = new ArrayList<>();
        result.add("this");
        result.add("base");
        assertEquals(result, exec("List result = []; " +
                "thisString('this');" +
                "setBaseString('base');" +
                "result.add(thisString()); " +
                "result.add(getBaseString());" +
                "result;", "", ""));
    }
}
