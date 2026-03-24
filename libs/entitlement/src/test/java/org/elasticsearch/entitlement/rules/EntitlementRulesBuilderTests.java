/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.rules;

import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.elasticsearch.entitlement.runtime.registry.InstrumentationRegistryImpl;
import org.elasticsearch.entitlement.runtime.registry.InternalInstrumentationRegistry;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.entitlement.rules.DslTestTypes.AbstractSub;
import static org.elasticsearch.entitlement.rules.DslTestTypes.Concrete;
import static org.elasticsearch.entitlement.rules.DslTestTypes.DummyWithGeneric;
import static org.elasticsearch.entitlement.rules.DslTestTypes.OtherDummy;
import static org.elasticsearch.entitlement.rules.DslTestTypes.TargetInterface;

/**
 * Unit tests for the EntitlementRulesBuilder DSL. Asserts that rules registered via the DSL
 * appear in the registry with the correct MethodKeys. No bytecode instrumentation.
 */
public class EntitlementRulesBuilderTests extends ESTestCase {

    private static final String CONCRETE_INTERNAL = "org/elasticsearch/entitlement/rules/DslTestTypes$Concrete";
    private static final String ABSTRACT_SUB_INTERNAL = "org/elasticsearch/entitlement/rules/DslTestTypes$AbstractSub";
    private static final String TARGET_IFACE_INTERNAL = "org/elasticsearch/entitlement/rules/DslTestTypes$TargetInterface";
    private static final String OTHER_DUMMY_INTERNAL = "org/elasticsearch/entitlement/rules/DslTestTypes$OtherDummy";
    private static final String DUMMY_WITH_GENERIC_INTERNAL = "org/elasticsearch/entitlement/rules/DslTestTypes$DummyWithGeneric";

    private InternalInstrumentationRegistry newRegistry() {
        return new InstrumentationRegistryImpl(null);
    }

    private EntitlementRulesBuilder newBuilder(InternalInstrumentationRegistry registry) {
        return new EntitlementRulesBuilder(registry);
    }

    private void assertHasRule(InternalInstrumentationRegistry registry, MethodKey expectedKey) {
        assertTrue(
            "Expected registry to contain " + expectedKey + " but had: " + registry.getInstrumentedMethods().keySet(),
            registry.getInstrumentedMethods().containsKey(expectedKey)
        );
    }

    private Map<MethodKey, ?> methods(InternalInstrumentationRegistry registry) {
        return registry.getInstrumentedMethods();
    }

    public void testOnConcreteClassInstanceMethod() {
        var registry = newRegistry();
        newBuilder(registry).on(Concrete.class).calling(Concrete::noArg).enforce(Policies::empty).elseThrowNotEntitled();
        assertHasRule(registry, new MethodKey(CONCRETE_INTERNAL, "noArg", List.of()));
    }

    public void testOnConcreteClassInstanceMethodWithParam() {
        var registry = newRegistry();
        newBuilder(registry).on(Concrete.class)
            .calling(Concrete::withArg, String.class)
            .enforce((a, b) -> Policies.empty())
            .elseThrowNotEntitled();
        assertHasRule(registry, new MethodKey(CONCRETE_INTERNAL, "withArg", List.of("java.lang.String")));
    }

    public void testOnInterfaceInstanceMethod() {
        var registry = newRegistry();
        newBuilder(registry).on(TargetInterface.class).calling(TargetInterface::noArg).enforce(Policies::empty).elseThrowNotEntitled();
        assertHasRule(registry, new MethodKey(TARGET_IFACE_INTERNAL, "noArg", List.of()));
    }

    public void testOnInterfaceInstanceMethodWithParam() {
        var registry = newRegistry();
        newBuilder(registry).on(TargetInterface.class)
            .calling(TargetInterface::withArg, String.class)
            .enforce(Policies::empty)
            .elseThrowNotEntitled();
        assertHasRule(registry, new MethodKey(TARGET_IFACE_INTERNAL, "withArg", List.of("java.lang.String")));
    }

    public void testOnAbstractClassInstanceMethod() {
        var registry = newRegistry();
        newBuilder(registry).on(AbstractSub.class).calling(AbstractSub::abstractMethod).enforce(Policies::empty).elseThrowNotEntitled();
        // Method is implemented (declared) in AbstractSub, so declaring class is AbstractSub
        assertHasRule(registry, new MethodKey(ABSTRACT_SUB_INTERNAL, "abstractMethod", List.of()));
    }

    public void testStaticMethodWithReturn() {
        var registry = newRegistry();
        newBuilder(registry).on(Concrete.class).callingStatic(Concrete::staticNoArg).enforce(Policies::empty).elseThrowNotEntitled();
        assertHasRule(registry, new MethodKey(CONCRETE_INTERNAL, "staticNoArg", List.of()));
    }

    public void testStaticMethodWithParam() {
        var registry = newRegistry();
        newBuilder(registry).on(Concrete.class)
            .callingStatic(Concrete::staticWithArg, Integer.class)
            .enforce(Policies::empty)
            .elseThrowNotEntitled();
        assertHasRule(registry, new MethodKey(CONCRETE_INTERNAL, "staticWithArg", List.of("java.lang.Integer")));
    }

    public void testStaticVoidMethod() {
        var registry = newRegistry();
        newBuilder(registry).on(Concrete.class)
            .callingVoidStatic(Concrete::staticVoidNoArg)
            .enforce(Policies::empty)
            .elseThrowNotEntitled();
        assertHasRule(registry, new MethodKey(CONCRETE_INTERNAL, "staticVoidNoArg", List.of()));
    }

    public void testStaticVoidMethodWithParam() {
        var registry = newRegistry();
        newBuilder(registry).on(Concrete.class)
            .callingVoidStatic(Concrete::staticVoidWithArg, String.class)
            .enforce(Policies::empty)
            .elseThrowNotEntitled();
        assertHasRule(registry, new MethodKey(CONCRETE_INTERNAL, "staticVoidWithArg", List.of("java.lang.String")));
    }

    public void testInstanceVoidMethod() {
        var registry = newRegistry();
        newBuilder(registry).on(Concrete.class).callingVoid(Concrete::voidNoArg).enforce(Policies::empty).elseThrowNotEntitled();
        assertHasRule(registry, new MethodKey(CONCRETE_INTERNAL, "voidNoArg", List.of()));
    }

    public void testInstanceVoidMethodWithParam() {
        var registry = newRegistry();
        newBuilder(registry).on(Concrete.class)
            .callingVoid(Concrete::voidWithArg, String.class)
            .enforce(Policies::empty)
            .elseThrowNotEntitled();
        assertHasRule(registry, new MethodKey(CONCRETE_INTERNAL, "voidWithArg", List.of("java.lang.String")));
    }

    public void testProtectedCtorNoArg() {
        var registry = newRegistry();
        newBuilder(registry).on(Concrete.class).protectedCtor().enforce(Policies::empty).elseThrowNotEntitled();
        assertHasRule(registry, new MethodKey(CONCRETE_INTERNAL, "<init>", List.of()));
    }

    public void testProtectedCtorWithArg() {
        var registry = newRegistry();
        newBuilder(registry).on(Concrete.class).protectedCtor(String.class).enforce(Policies::empty).elseThrowNotEntitled();
        assertHasRule(registry, new MethodKey(CONCRETE_INTERNAL, "<init>", List.of("java.lang.String")));
    }

    public void testTypeTokenParameter() {
        var registry = newRegistry();
        newBuilder(registry).on(DummyWithGeneric.class)
            .callingStatic(DummyWithGeneric::takeOne, TypeToken.of(Integer.class))
            .enforce(Policies::empty)
            .elseThrowNotEntitled();
        assertHasRule(registry, new MethodKey(DUMMY_WITH_GENERIC_INTERNAL, "takeOne", List.of("java.lang.Integer")));
    }

    public void testOnClassName() {
        var registry = newRegistry();
        var className = Concrete.class.getName();
        newBuilder(registry).on(className, Concrete.class).calling(Concrete::noArg).enforce(Policies::empty).elseThrowNotEntitled();
        assertHasRule(registry, new MethodKey(CONCRETE_INTERNAL, "noArg", List.of()));
    }

    public void testOnClassWithConsumer() {
        var registry = newRegistry();
        newBuilder(registry).on(Concrete.class, b -> b.calling(Concrete::noArg).enforce(Policies::empty).elseThrowNotEntitled());
        assertHasRule(registry, new MethodKey(CONCRETE_INTERNAL, "noArg", List.of()));
    }

    public void testOnMultipleClasses() {
        var registry = newRegistry();
        var builder = newBuilder(registry);
        builder.on(Concrete.class).calling(Concrete::noArg).enforce(Policies::empty).elseThrowNotEntitled();
        builder.on(OtherDummy.class).calling(OtherDummy::noArg).enforce(Policies::empty).elseThrowNotEntitled();
        assertHasRule(registry, new MethodKey(CONCRETE_INTERNAL, "noArg", List.of()));
        assertHasRule(registry, new MethodKey(OTHER_DUMMY_INTERNAL, "noArg", List.of()));
        assertEquals(2, methods(registry).size());
    }

    public void testMultipleRulesSameClass() {
        var registry = newRegistry();
        newBuilder(registry).on(Concrete.class)
            .calling(Concrete::noArg)
            .enforce(Policies::empty)
            .elseThrowNotEntitled()
            .calling(Concrete::withArg, String.class)
            .enforce(Policies::empty)
            .elseThrowNotEntitled();
        assertHasRule(registry, new MethodKey(CONCRETE_INTERNAL, "noArg", List.of()));
        assertHasRule(registry, new MethodKey(CONCRETE_INTERNAL, "withArg", List.of("java.lang.String")));
        assertEquals(2, methods(registry).size());
    }

    public void testOverloadedMethodDifferentKeys() {
        var registry = newRegistry();
        newBuilder(registry).on(Concrete.class)
            .callingVoid(Concrete::overloaded, Integer.class)
            .enforce(Policies::empty)
            .elseThrowNotEntitled()
            .callingVoid(Concrete::overloaded, String.class)
            .enforce(Policies::empty)
            .elseThrowNotEntitled();
        assertHasRule(registry, new MethodKey(CONCRETE_INTERNAL, "overloaded", List.of("java.lang.Integer")));
        assertHasRule(registry, new MethodKey(CONCRETE_INTERNAL, "overloaded", List.of("java.lang.String")));
        assertEquals(2, methods(registry).size());
    }

    public void testPrimitiveParam() {
        var registry = newRegistry();
        newBuilder(registry).on(Concrete.class).calling(Concrete::withInt, Integer.class).enforce(Policies::empty).elseThrowNotEntitled();
        assertHasRule(registry, new MethodKey(CONCRETE_INTERNAL, "withInt", List.of("java.lang.Integer")));
    }

    public void testArrayParam() {
        var registry = newRegistry();
        newBuilder(registry).on(Concrete.class).calling(Concrete::withArray, byte[].class).enforce(Policies::empty).elseThrowNotEntitled();
        assertHasRule(registry, new MethodKey(CONCRETE_INTERNAL, "withArray", List.of("byte[]")));
    }
}
