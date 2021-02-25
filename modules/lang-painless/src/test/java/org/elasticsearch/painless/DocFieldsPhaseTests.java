/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.lookup.PainlessLookupBuilder;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.script.ScriptContext;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DocFieldsPhaseTests extends ScriptTestCase {
    PainlessLookup lookup = PainlessLookupBuilder.buildFromWhitelists(Whitelist.BASE_WHITELISTS);

    ScriptScope compile(String script) {
        Compiler compiler = new Compiler(
            MockDocTestScript.CONTEXT.instanceClazz,
            MockDocTestScript.CONTEXT.factoryClazz,
            MockDocTestScript.CONTEXT.statefulFactoryClazz, lookup
        );

        // Create our loader (which loads compiled code with no permissions).
        final Compiler.Loader loader = AccessController.doPrivileged(new PrivilegedAction<>() {
            @Override
            public Compiler.Loader run() {
                return compiler.createLoader(getClass().getClassLoader());
            }
        });

        return compiler.compile(loader,"test", script, new CompilerSettings());
    }

    public abstract static class MockDocTestScript {
        public static final String[] PARAMETERS = {"doc", "other"};
        public abstract void execute(Map<String, Object> doc, Map<String, Object> other);

        public interface Factory {
            MockDocTestScript newInstance();
        }

        public static final ScriptContext<Factory> CONTEXT =
            new ScriptContext<>("test", MockDocTestScript.Factory.class);
    }

    public void testArray() {
        List<String> expected = List.of("my_field");
        // Order shouldn't matter
        assertEquals(expected, compile("def a = doc['my_field']; def b = other['foo']").docFields());
        assertEquals(expected, compile("def b = other['foo']; def a = doc['my_field']").docFields());

        // Only collect array on doc
        assertEquals(Collections.emptyList(), compile("def a = other['bar']").docFields());

        // Only handle str const
        assertEquals(Collections.emptyList(), compile("String f = 'bar'; def a = other[f]").docFields());
    }

    public void testDot() {
        List<String> expected = List.of("my_field");
        // Order shouldn't matter
        assertEquals(expected, compile("def a = doc.my_field; def b = other.foo").docFields());
        assertEquals(expected, compile("def b = other.foo; def a = doc.my_field").docFields());

        // Only collect doc dots
        assertEquals(Collections.emptyList(), compile("def a = other.bar").docFields());
    }

    public void testGet() {
        // Order shouldn't matter
        List<String> expected = List.of("my_field");
        assertEquals(expected, compile("def a = doc.get('my_field'); def b = other.get('foo')").docFields());
        assertEquals(expected, compile("def b = other.get('foo'); def a = doc.get('my_field')").docFields());

        // Should work in Lambda
        assertEquals(expected, compile("[].sort((a, b) -> doc.get('my_field')); [].sort((a, b) -> doc.equals('bar') ? 1:2)").docFields());

        // Only collect get on doc
        assertEquals(Collections.emptyList(), compile("def a = other.get('bar')").docFields());
        assertEquals(Collections.emptyList(), compile("def a = doc.equals('bar')").docFields());

        // Only handle str const
        assertEquals(Collections.emptyList(), compile("String f = 'bar'; def b = doc.get(f)").docFields());
    }
}
