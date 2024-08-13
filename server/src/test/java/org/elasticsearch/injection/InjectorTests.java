/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.injection;

import org.elasticsearch.injection.exceptions.InjectionConfigurationException;
import org.elasticsearch.test.ESTestCase;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Set;

public class InjectorTests extends ESTestCase {

    public void testInjectionOfRecordComponents() {
        var first = new First();
        var second = new Second(first);
        Injector injector = Injector.create().addRecordContents(new ExistingInstances(first, second));
        Third third = injector.inject(Third.class);
        assertSame(first, third.first);
        assertSame(second, third.second);
    }

    public record First() {}

    public record Second(First first) {}

    public record Third(First first, Second second) {}

    public record ExistingInstances(First first, Second second) {}

    public void testMultipleResultsMap() {
        Injector injector = Injector.create().addClasses(Service1.class, Component3.class);
        var resultMap = injector.inject(List.of(Service1.class, Component3.class));
        assertEquals(Set.of(Service1.class, Component3.class), resultMap.keySet());
        assertEquals(1, resultMap.get(Service1.class).size());
        assertEquals(1, resultMap.get(Component3.class).size());
        Service1 service1 = (Service1) resultMap.get(Service1.class).get(0);
        Component3 component3 = (Component3) resultMap.get(Component3.class).get(0);
        assertSame(service1, component3.service1());
    }

    /**
     * In most cases, if there are two objects that are instances of a class, that's ambiguous.
     * However, if a concrete (non-abstract) superclass is configured directly, that is not ambiguous:
     * the instance of that superclass takes precedence over any instances of any subclasses.
     */
    public void testOverrideAlias() {
        MethodHandles.lookup();
        assertEquals(
            Superclass.class,
            Injector.create()
                .addClasses(Superclass.class, Subclass.class) // Superclass first
                .inject(Superclass.class)
                .getClass()
        );
        MethodHandles.lookup();
        assertEquals(
            Superclass.class,
            Injector.create()
                .addClasses(Subclass.class, Superclass.class) // Subclass first
                .inject(Superclass.class)
                .getClass()
        );
        MethodHandles.lookup();
        assertEquals(
            Superclass.class,
            Injector.create()
                .addClasses(Subclass.class)
                .inject(Superclass.class) // Superclass is not mentioned until here
                .getClass()
        );
    }

    //
    // Sad paths
    //

    public void testBadInterfaceClass() {
        assertThrows(InjectionConfigurationException.class, () -> {
            MethodHandles.lookup();
            Injector.create().addClass(Listener.class).inject();
        });
    }

    public void testBadUnknownType() {
        // Injector knows only about Component4, discovers Listener, but can't find any subtypes
        MethodHandles.lookup();
        Injector injector = Injector.create().addClass(Component4.class);

        assertThrows(IllegalStateException.class, injector::inject);
    }

    public void testBadCircularDependency() {
        assertThrows(InjectionConfigurationException.class, () -> {
            MethodHandles.lookup();
            Injector.create().addClasses(Circular1.class, Circular2.class).inject();
        });
    }

    /**
     * For this one, we don't explicitly tell the injector about the classes involved in the cycle;
     * it finds them on its own.
     */
    public void testBadCircularDependencyViaParameter() {
        record UsesCircular1(Circular1 circular1) {}
        assertThrows(InjectionConfigurationException.class, () -> {
            MethodHandles.lookup();
            Injector.create().addClass(UsesCircular1.class).inject();
        });
    }

    public void testBadCircularDependencyViaSupertype() {
        interface Service1 {}
        record Service2(Service1 service1) {}
        record Service3(Service2 service2) implements Service1 {}
        assertThrows(InjectionConfigurationException.class, () -> {
            MethodHandles.lookup();
            Injector.create().addClasses(Service2.class, Service3.class).inject();
        });
    }

    // Common injectable things

    public record Service1() {}

    public interface Listener {}

    public record Component1() implements Listener {}

    public record Component2(Component1 component1) {}

    public record Component3(Service1 service1) {}

    public record Component4(Listener listener) {}

    public record GoodService(List<Component1> components) {}

    public record BadService(List<Component1> components) {
        public BadService {
            // Shouldn't be using the component list here!
            assert components.isEmpty() == false;
        }
    }

    public record MultiService(List<Component1> component1s, List<Component2> component2s) {}

    public record Circular1(Circular2 service2) {}

    public record Circular2(Circular1 service2) {}

    public static class Superclass {}

    public static class Subclass extends Superclass {}

}
