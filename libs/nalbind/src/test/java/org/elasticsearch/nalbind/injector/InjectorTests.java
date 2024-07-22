/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nalbind.injector;

import org.elasticsearch.example.module1.Module1ServiceImpl;
import org.elasticsearch.example.module2.Module2ServiceImpl;
import org.elasticsearch.example.module2.api.Module2Service;
import org.elasticsearch.nalbind.api.Actual;
import org.elasticsearch.nalbind.exceptions.InjectionConfigurationException;
import org.elasticsearch.nalbind.exceptions.UnresolvedProxyException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.testSupport.InjectorTestSupport;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Set;

@TestLogging(value="org.elasticsearch.nalbind:TRACE", reason="Debugging")
public class InjectorTests extends ESTestCase {

    public void testBasicFunctionality() {
        Module2Service module2Service = Injector.create(MethodHandles.lookup())
            .addClasses(Module1ServiceImpl.class, Module2ServiceImpl.class)
            .inject(Module2Service.class);
        assertEquals("Module1Service: Hello from Module1ServiceImpl to my 1 listeners", module2Service.statusReport());
    }

    public void testInaccessible() {
        var injector = Injector.create(MethodHandles.lookup());
        Class<?> c = new InjectorTestSupport().addLocalClassToInjector(injector);
        Object object = injector.inject(c);
        assertTrue(c.isInstance(object));
    }

    public void testInjectionOfRecordComponents() {
        record First(){}
        record Second(First first){}
        record Third(First first, Second second){}
        record ExistingInstances(First first, Second second){}

        var first = new First();
        var second = new Second(first);
        Injector injector = Injector.create(MethodHandles.lookup()).addRecordContents(new ExistingInstances(first, second));
        Third third = injector.inject(Third.class);
        assertSame(first, third.first);
        assertSame(second, third.second);
    }

    public void testInjectionOfLists() {
        Injector injector = Injector.create(MethodHandles.lookup()).addClasses(GoodService.class, ActualService.class, Component1.class);
        record InjectedStuff(GoodService goodService, ActualService actualService, Component1 component1) {}
        InjectedStuff injectedStuff = injector.inject(InjectedStuff.class);
        assertEquals(List.of(injectedStuff.component1), injectedStuff.goodService.components());
        assertEquals(List.of(injectedStuff.component1), injectedStuff.actualService.components());
        assertSame(injectedStuff.component1, injectedStuff.goodService.components().get(0));
        assertSame(injectedStuff.component1, injectedStuff.actualService.components().get(0));
    }

    public void testInjectionOfMultipleLists() {
        Injector injector = Injector.create(MethodHandles.lookup()).addClass(MultiService.class);
        record InjectedStuff(MultiService multiService, Component1 component1, Component2 component2) {}
        var injectedStuff = injector.inject(InjectedStuff.class);
        assertEquals(List.of(injectedStuff.component1), injectedStuff.multiService.component1s);
        assertEquals(List.of(injectedStuff.component2), injectedStuff.multiService.component2s);
        assertSame(injectedStuff.component1, injectedStuff.multiService.component1s.get(0));
        assertSame(injectedStuff.component2, injectedStuff.multiService.component2s.get(0));
    }

    public void testMultipleResultsMap() {
        Injector injector = Injector.create(MethodHandles.lookup()).addClasses(Service1.class, Component3.class);
        var resultMap = injector.inject(List.of(Service1.class, Component3.class));
        assertEquals(Set.of(Service1.class, Component3.class), resultMap.keySet());
        assertEquals(1, resultMap.get(Service1.class).size());
        assertEquals(1, resultMap.get(Component3.class).size());
        Service1 service1 = (Service1) resultMap.get(Service1.class).get(0);
        Component3 component3 = (Component3) resultMap.get(Component3.class).get(0);
        assertSame(service1, component3.service1());
    }

    public void testSupertypeList() {
        interface Listener {}
        record Service(List<Listener> listeners) {}
        record SomeListener() implements Listener {}

        Service service = Injector.create(MethodHandles.lookup()).addInstance(this).addClass(SomeListener.class).inject(Service.class);
        assertEquals(1, service.listeners.size());
    }

    public void testMutualInjectionViaList() {
        interface Listener {}
        record Service(List<Listener> listeners) {}
        record SomeListener(Service service) implements Listener {}

        record InjectedStuff(Service service, SomeListener listener){}

        InjectedStuff injected = Injector.create(MethodHandles.lookup()).addClass(SomeListener.class).inject(InjectedStuff.class);
        assertEquals(List.of(injected.listener), injected.service.listeners());
        assertSame(injected.listener, injected.service.listeners().get(0));
        assertSame(injected.service, injected.listener.service);
    }

    /**
     * In most cases, if there are two objects that are instances of a class, that's ambiguous.
     * However, if a concrete (non-abstract) superclass is configured directly, that is not ambiguous:
     * the instance of that superclass takes precedence over any instances of any subclasses.
     */
    public void testOverrideAlias() {
        class Superclass {}
        class Subclass extends Superclass {}

        assertEquals(Superclass.class, Injector.create(MethodHandles.lookup())
            .addClasses(Superclass.class, Subclass.class) // Superclass first
            .inject(Superclass.class)
            .getClass());
        assertEquals(Superclass.class, Injector.create(MethodHandles.lookup())
            .addClasses(Subclass.class, Superclass.class) // Subclass first
            .inject(Superclass.class)
            .getClass());
        assertEquals(Superclass.class, Injector.create(MethodHandles.lookup())
            .addClasses(Subclass.class)
            .inject(Superclass.class) // Superclass is not mentioned until here
            .getClass());
    }

    //
    // Sad paths
    //

    public void testBadUseOfListProxy() {
        Injector injector = Injector.create(MethodHandles.lookup()).addClasses(BadService.class, Component1.class);
        // Note that this throws IllegalStateException rather than InjectionConfigurationException because the problem occurs in user code
        assertThrows(UnresolvedProxyException.class, injector::inject);
    }

    public void testBadInterfaceClass() {
        assertThrows(InjectionConfigurationException.class, () ->
            Injector.create(MethodHandles.lookup()).addClass(Listener.class).inject());
    }

    public void testBadUnknownType() {
        interface Supertype{}
        record Service(Supertype supertype) {}

        // Injector knows only about Service, discovers Supertype, but can't find any subtypes
        Injector injector = Injector.create(MethodHandles.lookup()).addClass(Service.class);

        assertThrows(IllegalStateException.class, injector::inject);
    }

    public void testBadCircularDependency() {
        assertThrows(InjectionConfigurationException.class, () -> {
            Injector.create(MethodHandles.lookup()).addClasses(Circular1.class, Circular2.class).inject();
        });
    }

    /**
     * For this one, we don't explicitly tell the injector about the classes involved in the cycle;
     * it finds them on its own.
     */
    public void testBadCircularDependencyViaParameter() {
        record UsesCircular1(Circular1 circular1){}
        assertThrows(InjectionConfigurationException.class, () -> {
            Injector.create(MethodHandles.lookup()).addClass(UsesCircular1.class).inject();
        });
    }

    public void testBadCircularDependencyViaSupertype() {
        interface Service1 {}
        record Service2(Service1 service1){}
        record Service3(Service2 service2) implements Service1 {}
        assertThrows(InjectionConfigurationException.class, () -> {
            Injector.create(MethodHandles.lookup()).addClasses(Service2.class, Service3.class).inject();
        });
    }

    public void testBadCircularDependencyViaList() {
        interface Listener {}
        record Service(@Actual List<Listener> listeners) {}
        record Whoops(Service service) implements Listener {}

        assertThrows(InjectionConfigurationException.class, () -> {
            Injector.create(MethodHandles.lookup()).addClasses(Whoops.class).inject();
        });
    }

    public void testBadListenerAfterListIsResolved() {

    }

    // Common injectable things

    public record Service1() { }

    public interface Listener{}

    public record Component1() implements Listener {}

    public record Component2(Component1 component1) {}

    public record Component3(Service1 service1) {}

    public record GoodService(List<Component1> components) { }

    public record BadService(List<Component1> components) {
        public BadService {
            // Shouldn't be using the component list here!
            assert components.isEmpty() == false;
        }
    }

    public record ActualService(@Actual List<Component1> components) {
        public ActualService {
            assert components.isEmpty() == false;
        }
    }

    public record MultiService(List<Component1> component1s, List<Component2> component2s) { }

    record Circular1(Circular2 service2) {}
    record Circular2(Circular1 service2) {}

}
