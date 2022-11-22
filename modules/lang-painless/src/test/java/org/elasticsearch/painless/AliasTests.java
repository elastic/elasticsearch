/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.lookup.PainlessLookupBuilder;
import org.elasticsearch.painless.spi.PainlessTestScript;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AliasTests extends ScriptTestCase {

    @Override
    protected Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = new HashMap<>();
        List<Whitelist> whitelists = new ArrayList<>(PainlessPlugin.BASE_WHITELISTS);
        whitelists.add(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.painless.alias"));
        contexts.put(PainlessTestScript.CONTEXT, whitelists);
        return contexts;
    }

    public void testNoShadowing() {
        IllegalArgumentException err = expectThrows(
            IllegalArgumentException.class,
            () -> PainlessLookupBuilder.buildFromWhitelists(
                List.of(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.painless.alias-shadow"))
            )
        );
        assertEquals(
            "Cannot add alias [AliasedTestInnerClass] for [class org.elasticsearch.painless.AliasTestClass$AliasedTestInnerClass]"
                + " that shadows class [class org.elasticsearch.painless.AliasedTestInnerClass]",
            err.getCause().getMessage()
        );
    }

    public void testDefAlias() {
        assertEquals(5, exec("def a = AliasTestClass.getInnerAliased(); AliasedTestInnerClass b = a; b.plus(2, 3)"));
    }

    public void testInnerAlias() {
        assertEquals(5, exec("AliasTestClass.AliasedTestInnerClass a = AliasTestClass.getInnerAliased(); a.plus(2, 3)"));
        assertEquals(5, exec("AliasedTestInnerClass a = AliasTestClass.getInnerAliased(); a.plus(2, 3)"));
    }

    public void testInnerNoAlias() {
        assertEquals(-1, exec("AliasTestClass.UnaliasedTestInnerClass a = AliasTestClass.getInnerUnaliased(); a.minus(2, 3)"));
        IllegalArgumentException e = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec("UnaliasedTestInnerClass a = AliasTestClass.getInnerUnaliased(); a.minus(2, 3)")
        );
        assertEquals("invalid declaration: cannot resolve type [UnaliasedTestInnerClass]", e.getMessage());
    }
}
