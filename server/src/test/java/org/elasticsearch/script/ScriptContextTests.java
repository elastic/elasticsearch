/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.test.ESTestCase;

public class ScriptContextTests extends ESTestCase {

    public interface TwoNewInstance {
        String newInstance(int foo, int bar);

        String newInstance(int foo);

        interface StatefulFactory {
            TwoNewInstance newFactory();
        }
    }

    public interface TwoNewFactory {
        String newFactory(int foo, int bar);

        String newFactory(int foo);
    }

    public interface MissingNewInstance {
        String typoNewInstanceMethod(int foo);
    }

    public interface DummyScript {
        int execute(int foo);

        interface Factory {
            DummyScript newInstance();
        }
    }

    public interface DummyStatefulScript {
        int execute(int foo);

        interface StatefulFactory {
            DummyStatefulScript newInstance();
        }

        interface Factory {
            StatefulFactory newFactory();
        }
    }

    public void testTwoNewInstanceMethods() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ScriptContext<>("test", TwoNewInstance.class));
        assertEquals(
            "Cannot have multiple newInstance methods on FactoryType class ["
                + TwoNewInstance.class.getName()
                + "] for script context [test]",
            e.getMessage()
        );
    }

    public void testTwoNewFactoryMethods() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ScriptContext<>("test", TwoNewFactory.class));
        assertEquals(
            "Cannot have multiple newFactory methods on FactoryType class ["
                + TwoNewFactory.class.getName()
                + "] for script context [test]",
            e.getMessage()
        );
    }

    public void testTwoNewInstanceStatefulFactoryMethods() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new ScriptContext<>("test", TwoNewInstance.StatefulFactory.class)
        );
        assertEquals(
            "Cannot have multiple newInstance methods on StatefulFactoryType class ["
                + TwoNewInstance.class.getName()
                + "] for script context [test]",
            e.getMessage()
        );
    }

    public void testMissingNewInstanceMethod() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new ScriptContext<>("test", MissingNewInstance.class)
        );
        assertEquals(
            "Could not find method newInstance or method newFactory on FactoryType class ["
                + MissingNewInstance.class.getName()
                + "] for script context [test]",
            e.getMessage()
        );
    }

    public void testInstanceTypeReflection() {
        ScriptContext<?> context = new ScriptContext<>("test", DummyScript.Factory.class);
        assertEquals("test", context.name);
        assertEquals(DummyScript.class, context.instanceClazz);
        assertNull(context.statefulFactoryClazz);
        assertEquals(DummyScript.Factory.class, context.factoryClazz);
    }

    public void testStatefulFactoryReflection() {
        ScriptContext<?> context = new ScriptContext<>("test", DummyStatefulScript.Factory.class);
        assertEquals("test", context.name);
        assertEquals(DummyStatefulScript.class, context.instanceClazz);
        assertEquals(DummyStatefulScript.StatefulFactory.class, context.statefulFactoryClazz);
        assertEquals(DummyStatefulScript.Factory.class, context.factoryClazz);
    }
}
