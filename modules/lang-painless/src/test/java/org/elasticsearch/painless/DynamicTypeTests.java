/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.spi.PainlessTestScript;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DynamicTypeTests extends ScriptTestCase {

    @Override
    protected Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = new HashMap<>();
        List<Whitelist> whitelists = new ArrayList<>(PainlessPlugin.BASE_WHITELISTS);
        whitelists.add(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.painless.test"));
        whitelists.add(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.painless.dynamic"));
        contexts.put(PainlessTestScript.CONTEXT, whitelists);
        return contexts;
    }

    public interface DynI {

    }

    public static class DynA {}

    public static class DynB extends DynA implements DynI {}

    public static class DynC extends DynB {}

    public static class DynD extends DynB {
        public char letter() {
            return 'D';
        }
    }

    public static class DynE extends DynC {
        public char letter() {
            return 'E';
        }
    }

    public static class DynF extends DynE {}

    public static class DynG extends DynF {
        public char letter() {
            return 'G';
        }

        public int value() {
            return 1;
        }
    }

    public void testDynamicTypeResolution() {
        assertEquals('D', exec("DynamicTypeTests.DynI i = new DynamicTypeTests.DynD(); return i.letter()"));
        assertEquals('E', exec("DynamicTypeTests.DynI i = new DynamicTypeTests.DynE(); return i.letter()"));
        assertEquals('E', exec("DynamicTypeTests.DynI i = new DynamicTypeTests.DynF(); return i.letter()"));
        assertEquals('G', exec("DynamicTypeTests.DynI i = new DynamicTypeTests.DynG(); return i.letter()"));
        IllegalArgumentException iae = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec("DynamicTypeTests.DynI i = new DynamicTypeTests.DynD(); return i.value()")
        );
        assertTrue(iae.getMessage().contains("dynamic method") && iae.getMessage().contains("not found"));
        iae = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec("DynamicTypeTests.DynI i = new DynamicTypeTests.DynE(); return i.value()")
        );
        assertTrue(iae.getMessage().contains("dynamic method") && iae.getMessage().contains("not found"));
        iae = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec("DynamicTypeTests.DynI i = new DynamicTypeTests.DynF(); return i.value()")
        );
        assertTrue(iae.getMessage().contains("dynamic method") && iae.getMessage().contains("not found"));
        assertEquals(1, exec("DynamicTypeTests.DynI i = new DynamicTypeTests.DynG(); return i.value()"));

        iae = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec("DynamicTypeTests.DynA a = new DynamicTypeTests.DynD(); return a.letter()")
        );
        assertTrue(iae.getMessage().contains("member method") && iae.getMessage().contains("not found"));
        iae = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec("DynamicTypeTests.DynA a = new DynamicTypeTests.DynE(); return a.letter()")
        );
        assertTrue(iae.getMessage().contains("member method") && iae.getMessage().contains("not found"));
        iae = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec("DynamicTypeTests.DynA a = new DynamicTypeTests.DynF(); return a.letter()")
        );
        assertTrue(iae.getMessage().contains("member method") && iae.getMessage().contains("not found"));
        iae = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec("DynamicTypeTests.DynA a = new DynamicTypeTests.DynG(); return a.letter()")
        );
        assertTrue(iae.getMessage().contains("member method") && iae.getMessage().contains("not found"));
        iae = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec("DynamicTypeTests.DynA a = new DynamicTypeTests.DynD(); return a.value()")
        );
        assertTrue(iae.getMessage().contains("member method") && iae.getMessage().contains("not found"));
        iae = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec("DynamicTypeTests.DynA a = new DynamicTypeTests.DynE(); return a.value()")
        );
        assertTrue(iae.getMessage().contains("member method") && iae.getMessage().contains("not found"));
        iae = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec("DynamicTypeTests.DynA a = new DynamicTypeTests.DynF(); return a.value()")
        );
        assertTrue(iae.getMessage().contains("member method") && iae.getMessage().contains("not found"));
        iae = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec("DynamicTypeTests.DynA a = new DynamicTypeTests.DynG(); return a.value()")
        );
        assertTrue(iae.getMessage().contains("member method") && iae.getMessage().contains("not found"));

        assertEquals('D', exec("DynamicTypeTests.DynB b = new DynamicTypeTests.DynD(); return b.letter()"));
        assertEquals('E', exec("DynamicTypeTests.DynB b = new DynamicTypeTests.DynE(); return b.letter()"));
        assertEquals('E', exec("DynamicTypeTests.DynB b = new DynamicTypeTests.DynF(); return b.letter()"));
        assertEquals('G', exec("DynamicTypeTests.DynB b = new DynamicTypeTests.DynG(); return b.letter()"));
        iae = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec("DynamicTypeTests.DynB b = new DynamicTypeTests.DynD(); return b.value()")
        );
        assertTrue(iae.getMessage().contains("dynamic method") && iae.getMessage().contains("not found"));
        iae = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec("DynamicTypeTests.DynB b = new DynamicTypeTests.DynE(); return b.value()")
        );
        assertTrue(iae.getMessage().contains("dynamic method") && iae.getMessage().contains("not found"));
        iae = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec("DynamicTypeTests.DynB b = new DynamicTypeTests.DynF(); return b.value()")
        );
        assertTrue(iae.getMessage().contains("dynamic method") && iae.getMessage().contains("not found"));
        assertEquals(1, exec("DynamicTypeTests.DynB b = new DynamicTypeTests.DynG(); return b.value()"));

        iae = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec("DynamicTypeTests.DynC c = new DynamicTypeTests.DynE(); return c.letter()")
        );
        assertTrue(iae.getMessage().contains("member method") && iae.getMessage().contains("not found"));
        iae = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec("DynamicTypeTests.DynC c = new DynamicTypeTests.DynF(); return c.letter()")
        );
        assertTrue(iae.getMessage().contains("member method") && iae.getMessage().contains("not found"));
        iae = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec("DynamicTypeTests.DynC c = new DynamicTypeTests.DynG(); return c.letter()")
        );
        assertTrue(iae.getMessage().contains("member method") && iae.getMessage().contains("not found"));
        iae = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec("DynamicTypeTests.DynC c = new DynamicTypeTests.DynE(); return c.value()")
        );
        assertTrue(iae.getMessage().contains("member method") && iae.getMessage().contains("not found"));
        iae = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec("DynamicTypeTests.DynC c = new DynamicTypeTests.DynF(); return c.value()")
        );
        assertTrue(iae.getMessage().contains("member method") && iae.getMessage().contains("not found"));
        iae = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec("DynamicTypeTests.DynC c = new DynamicTypeTests.DynG(); return c.value()")
        );
        assertTrue(iae.getMessage().contains("member method") && iae.getMessage().contains("not found"));

        assertEquals('D', exec("DynamicTypeTests.DynD d = new DynamicTypeTests.DynD(); return d.letter()"));
        iae = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec("DynamicTypeTests.DynD d = new DynamicTypeTests.DynD(); return d.value()")
        );
        assertTrue(iae.getMessage().contains("member method") && iae.getMessage().contains("not found"));

        assertEquals('E', exec("DynamicTypeTests.DynE e = new DynamicTypeTests.DynE(); return e.letter()"));
        assertEquals('E', exec("DynamicTypeTests.DynE e = new DynamicTypeTests.DynF(); return e.letter()"));
        assertEquals('G', exec("DynamicTypeTests.DynE e = new DynamicTypeTests.DynG(); return e.letter()"));
        iae = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec("DynamicTypeTests.DynE e = new DynamicTypeTests.DynE(); return e.value()")
        );
        assertTrue(iae.getMessage().contains("dynamic method") && iae.getMessage().contains("not found"));
        iae = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec("DynamicTypeTests.DynE e = new DynamicTypeTests.DynF(); return e.value()")
        );
        assertTrue(iae.getMessage().contains("dynamic method") && iae.getMessage().contains("not found"));
        assertEquals(1, exec("DynamicTypeTests.DynE e = new DynamicTypeTests.DynG(); return e.value()"));

        assertEquals('E', exec("DynamicTypeTests.DynF f = new DynamicTypeTests.DynF(); return f.letter()"));
        assertEquals('G', exec("DynamicTypeTests.DynF f = new DynamicTypeTests.DynG(); return f.letter()"));
        iae = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec("DynamicTypeTests.DynF f = new DynamicTypeTests.DynF(); return f.value()")
        );
        assertTrue(iae.getMessage().contains("dynamic method") && iae.getMessage().contains("not found"));
        assertEquals(1, exec("DynamicTypeTests.DynF f = new DynamicTypeTests.DynG(); return f.value()"));

        assertEquals('G', exec("DynamicTypeTests.DynG g = new DynamicTypeTests.DynG(); return g.letter()"));
        assertEquals(1, exec("DynamicTypeTests.DynG g = new DynamicTypeTests.DynG(); return g.value()"));
    }
}
