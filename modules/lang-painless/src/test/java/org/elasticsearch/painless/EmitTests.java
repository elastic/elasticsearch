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

public class EmitTests extends ScriptTestCase {
    @Override
    protected Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = new HashMap<>();
        List<Whitelist> whitelists = new ArrayList<>(PainlessPlugin.BASE_WHITELISTS);
        whitelists.add(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.painless.test"));
        contexts.put(TestFieldScript.CONTEXT, whitelists);
        return contexts;
    }

    @Override
    public TestFieldScript exec(String script) {
        TestFieldScript.Factory factory = scriptEngine.compile(null, script, TestFieldScript.CONTEXT, new HashMap<>());
        TestFieldScript testScript = factory.newInstance();
        testScript.execute();
        return testScript;
    }

    public void testEmit() {
        TestFieldScript script = exec("emit(1L)");
        assertNotNull(script);
        assertArrayEquals(new long[] { 1L }, script.fetchValues());
    }

    public void testEmitFromUserFunction() {
        TestFieldScript script = exec("void doEmit(long l) { emit(l) } doEmit(1L); doEmit(100L)");
        assertNotNull(script);
        assertArrayEquals(new long[] { 1L, 100L }, script.fetchValues());
    }
}
