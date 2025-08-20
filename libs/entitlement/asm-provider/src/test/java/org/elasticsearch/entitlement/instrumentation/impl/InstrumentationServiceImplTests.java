/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.instrumentation.impl;

import org.elasticsearch.entitlement.instrumentation.CheckMethod;
import org.elasticsearch.entitlement.instrumentation.InstrumentationService;
import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.elasticsearch.test.ESTestCase;
import org.objectweb.asm.Type;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;

public class InstrumentationServiceImplTests extends ESTestCase {

    final InstrumentationService instrumentationService = new InstrumentationServiceImpl();

    interface TestTargetInterface {
        void instanceMethod(int x, String y);
    }

    static class TestTargetClass implements TestTargetInterface {
        @Override
        public void instanceMethod(int x, String y) {}
    }

    abstract static class TestTargetBaseClass {
        abstract void instanceMethod(int x, String y);

        abstract void instanceMethod2(int x, String y);
    }

    abstract static class TestTargetIntermediateClass extends TestTargetBaseClass {
        @Override
        public void instanceMethod2(int x, String y) {}
    }

    static class TestTargetImplementationClass extends TestTargetIntermediateClass {
        @Override
        public void instanceMethod(int x, String y) {}
    }

    interface TestChecker {
        void check$org_example_TestTargetClass$$staticMethod(Class<?> clazz, int arg0, String arg1, Object arg2);

        void check$org_example_TestTargetClass$instanceMethodNoArgs(Class<?> clazz, TestTargetClass that);

        void check$org_example_TestTargetClass$instanceMethodWithArgs(Class<?> clazz, TestTargetClass that, int x, int y);
    }

    interface TestCheckerDerived extends TestChecker {
        void check$org_example_TestTargetClass$instanceMethodNoArgs(Class<?> clazz, TestTargetClass that);

        void check$org_example_TestTargetClass$differentInstanceMethod(Class<?> clazz, TestTargetClass that);
    }

    interface TestCheckerDerived2 extends TestCheckerDerived, TestChecker {}

    interface TestCheckerOverloads {
        void check$org_example_TestTargetClass$$staticMethodWithOverload(Class<?> clazz, int x, int y);

        void check$org_example_TestTargetClass$$staticMethodWithOverload(Class<?> clazz, int x, String y);
    }

    interface TestCheckerCtors {
        void check$org_example_TestTargetClass$(Class<?> clazz);

        void check$org_example_TestTargetClass$(Class<?> clazz, int x, String y);
    }

    interface TestCheckerMixed {
        void check$org_example_TestTargetClass$$staticMethod(Class<?> clazz, int arg0, String arg1, Object arg2);

        void checkInstanceMethodManual(Class<?> clazz, TestTargetInterface that, int x, String y);

        void checkInstanceMethodManual(Class<?> clazz, TestTargetBaseClass that, int x, String y);
    }

    interface TestCheckerDerived3 extends TestCheckerMixed {}

    public void testInstrumentationTargetLookup() throws ClassNotFoundException {
        Map<MethodKey, CheckMethod> checkMethods = instrumentationService.lookupMethods(TestChecker.class);

        assertThat(checkMethods, aMapWithSize(3));
        assertThat(
            checkMethods,
            hasEntry(
                equalTo(new MethodKey("org/example/TestTargetClass", "staticMethod", List.of("I", "java/lang/String", "java/lang/Object"))),
                equalTo(
                    new CheckMethod(
                        "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestChecker",
                        "check$org_example_TestTargetClass$$staticMethod",
                        List.of("Ljava/lang/Class;", "I", "Ljava/lang/String;", "Ljava/lang/Object;")
                    )
                )
            )
        );
        assertThat(
            checkMethods,
            hasEntry(
                equalTo(new MethodKey("org/example/TestTargetClass", "instanceMethodNoArgs", List.of())),
                equalTo(
                    new CheckMethod(
                        "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestChecker",
                        "check$org_example_TestTargetClass$instanceMethodNoArgs",
                        List.of(
                            "Ljava/lang/Class;",
                            "Lorg/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestTargetClass;"
                        )
                    )
                )
            )
        );
        assertThat(
            checkMethods,
            hasEntry(
                equalTo(new MethodKey("org/example/TestTargetClass", "instanceMethodWithArgs", List.of("I", "I"))),
                equalTo(
                    new CheckMethod(
                        "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestChecker",
                        "check$org_example_TestTargetClass$instanceMethodWithArgs",
                        List.of(
                            "Ljava/lang/Class;",
                            "Lorg/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestTargetClass;",
                            "I",
                            "I"
                        )
                    )
                )
            )
        );
    }

    public void testInstrumentationTargetLookupWithOverloads() throws ClassNotFoundException {
        Map<MethodKey, CheckMethod> checkMethods = instrumentationService.lookupMethods(TestCheckerOverloads.class);

        assertThat(checkMethods, aMapWithSize(2));
        assertThat(
            checkMethods,
            hasEntry(
                equalTo(new MethodKey("org/example/TestTargetClass", "staticMethodWithOverload", List.of("I", "java/lang/String"))),
                equalTo(
                    new CheckMethod(
                        "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestCheckerOverloads",
                        "check$org_example_TestTargetClass$$staticMethodWithOverload",
                        List.of("Ljava/lang/Class;", "I", "Ljava/lang/String;")
                    )
                )
            )
        );
        assertThat(
            checkMethods,
            hasEntry(
                equalTo(new MethodKey("org/example/TestTargetClass", "staticMethodWithOverload", List.of("I", "I"))),
                equalTo(
                    new CheckMethod(
                        "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestCheckerOverloads",
                        "check$org_example_TestTargetClass$$staticMethodWithOverload",
                        List.of("Ljava/lang/Class;", "I", "I")
                    )
                )
            )
        );
    }

    public void testInstrumentationTargetLookupWithDerivedClass() throws ClassNotFoundException {
        Map<MethodKey, CheckMethod> checkMethods = instrumentationService.lookupMethods(TestCheckerDerived2.class);

        assertThat(checkMethods, aMapWithSize(4));
        assertThat(
            checkMethods,
            hasEntry(
                equalTo(new MethodKey("org/example/TestTargetClass", "staticMethod", List.of("I", "java/lang/String", "java/lang/Object"))),
                equalTo(
                    new CheckMethod(
                        "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestChecker",
                        "check$org_example_TestTargetClass$$staticMethod",
                        List.of("Ljava/lang/Class;", "I", "Ljava/lang/String;", "Ljava/lang/Object;")
                    )
                )
            )
        );
        assertThat(
            checkMethods,
            hasEntry(
                equalTo(new MethodKey("org/example/TestTargetClass", "instanceMethodNoArgs", List.of())),
                equalTo(
                    new CheckMethod(
                        "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestCheckerDerived",
                        "check$org_example_TestTargetClass$instanceMethodNoArgs",
                        List.of(
                            "Ljava/lang/Class;",
                            "Lorg/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestTargetClass;"
                        )
                    )
                )
            )
        );
        assertThat(
            checkMethods,
            hasEntry(
                equalTo(new MethodKey("org/example/TestTargetClass", "instanceMethodWithArgs", List.of("I", "I"))),
                equalTo(
                    new CheckMethod(
                        "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestChecker",
                        "check$org_example_TestTargetClass$instanceMethodWithArgs",
                        List.of(
                            "Ljava/lang/Class;",
                            "Lorg/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestTargetClass;",
                            "I",
                            "I"
                        )
                    )
                )
            )
        );
        assertThat(
            checkMethods,
            hasEntry(
                equalTo(new MethodKey("org/example/TestTargetClass", "differentInstanceMethod", List.of())),
                equalTo(
                    new CheckMethod(
                        "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestCheckerDerived",
                        "check$org_example_TestTargetClass$differentInstanceMethod",
                        List.of(
                            "Ljava/lang/Class;",
                            "Lorg/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestTargetClass;"
                        )
                    )
                )
            )
        );
    }

    public void testInstrumentationTargetLookupWithCtors() throws ClassNotFoundException {
        Map<MethodKey, CheckMethod> checkMethods = instrumentationService.lookupMethods(TestCheckerCtors.class);

        assertThat(checkMethods, aMapWithSize(2));
        assertThat(
            checkMethods,
            hasEntry(
                equalTo(new MethodKey("org/example/TestTargetClass", "<init>", List.of("I", "java/lang/String"))),
                equalTo(
                    new CheckMethod(
                        "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestCheckerCtors",
                        "check$org_example_TestTargetClass$",
                        List.of("Ljava/lang/Class;", "I", "Ljava/lang/String;")
                    )
                )
            )
        );
        assertThat(
            checkMethods,
            hasEntry(
                equalTo(new MethodKey("org/example/TestTargetClass", "<init>", List.of())),
                equalTo(
                    new CheckMethod(
                        "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestCheckerCtors",
                        "check$org_example_TestTargetClass$",
                        List.of("Ljava/lang/Class;")
                    )
                )
            )
        );
    }

    public void testInstrumentationTargetLookupWithExtraMethods() throws ClassNotFoundException {
        Map<MethodKey, CheckMethod> checkMethods = instrumentationService.lookupMethods(TestCheckerMixed.class);

        assertThat(checkMethods, aMapWithSize(1));
        assertThat(
            checkMethods,
            hasEntry(
                equalTo(new MethodKey("org/example/TestTargetClass", "staticMethod", List.of("I", "java/lang/String", "java/lang/Object"))),
                equalTo(
                    new CheckMethod(
                        "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestCheckerMixed",
                        "check$org_example_TestTargetClass$$staticMethod",
                        List.of("Ljava/lang/Class;", "I", "Ljava/lang/String;", "Ljava/lang/Object;")
                    )
                )
            )
        );
    }

    public void testLookupImplementationMethodWithInterface() throws ClassNotFoundException, NoSuchMethodException {
        var info = instrumentationService.lookupImplementationMethod(
            TestTargetInterface.class,
            "instanceMethod",
            TestTargetClass.class,
            TestCheckerMixed.class,
            "checkInstanceMethodManual",
            int.class,
            String.class
        );

        assertThat(
            info.targetMethod(),
            equalTo(
                new MethodKey(
                    "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestTargetClass",
                    "instanceMethod",
                    List.of("I", "java/lang/String")
                )
            )
        );
        assertThat(
            info.checkMethod(),
            equalTo(
                new CheckMethod(
                    "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestCheckerMixed",
                    "checkInstanceMethodManual",
                    List.of(
                        "Ljava/lang/Class;",
                        "Lorg/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestTargetInterface;",
                        "I",
                        "Ljava/lang/String;"
                    )
                )
            )
        );
    }

    public void testLookupImplementationMethodWithBaseClass() throws ClassNotFoundException, NoSuchMethodException {
        var info = instrumentationService.lookupImplementationMethod(
            TestTargetBaseClass.class,
            "instanceMethod",
            TestTargetImplementationClass.class,
            TestCheckerMixed.class,
            "checkInstanceMethodManual",
            int.class,
            String.class
        );

        assertThat(
            info.targetMethod(),
            equalTo(
                new MethodKey(
                    "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestTargetImplementationClass",
                    "instanceMethod",
                    List.of("I", "java/lang/String")
                )
            )
        );
        assertThat(
            info.checkMethod(),
            equalTo(
                new CheckMethod(
                    "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestCheckerMixed",
                    "checkInstanceMethodManual",
                    List.of(
                        "Ljava/lang/Class;",
                        "Lorg/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestTargetBaseClass;",
                        "I",
                        "Ljava/lang/String;"
                    )
                )
            )
        );
    }

    public void testLookupImplementationMethodWithInheritanceOnTarget() throws ClassNotFoundException, NoSuchMethodException {
        var info = instrumentationService.lookupImplementationMethod(
            TestTargetBaseClass.class,
            "instanceMethod2",
            TestTargetImplementationClass.class,
            TestCheckerMixed.class,
            "checkInstanceMethodManual",
            int.class,
            String.class
        );

        assertThat(
            info.targetMethod(),
            equalTo(
                new MethodKey(
                    "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestTargetIntermediateClass",
                    "instanceMethod2",
                    List.of("I", "java/lang/String")
                )
            )
        );
        assertThat(
            info.checkMethod(),
            equalTo(
                new CheckMethod(
                    "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestCheckerMixed",
                    "checkInstanceMethodManual",
                    List.of(
                        "Ljava/lang/Class;",
                        "Lorg/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestTargetBaseClass;",
                        "I",
                        "Ljava/lang/String;"
                    )
                )
            )
        );
    }

    public void testLookupImplementationMethodWithInheritanceOnChecker() throws ClassNotFoundException, NoSuchMethodException {
        var info = instrumentationService.lookupImplementationMethod(
            TestTargetBaseClass.class,
            "instanceMethod2",
            TestTargetImplementationClass.class,
            TestCheckerDerived3.class,
            "checkInstanceMethodManual",
            int.class,
            String.class
        );

        assertThat(
            info.targetMethod(),
            equalTo(
                new MethodKey(
                    "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestTargetIntermediateClass",
                    "instanceMethod2",
                    List.of("I", "java/lang/String")
                )
            )
        );
        assertThat(
            info.checkMethod(),
            equalTo(
                new CheckMethod(
                    "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestCheckerMixed",
                    "checkInstanceMethodManual",
                    List.of(
                        "Ljava/lang/Class;",
                        "Lorg/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestTargetBaseClass;",
                        "I",
                        "Ljava/lang/String;"
                    )
                )
            )
        );
    }

    public void testParseCheckerMethodSignatureStaticMethod() {
        var methodKey = InstrumentationServiceImpl.parseCheckerMethodSignature(
            "check$org_example_TestClass$$staticMethod",
            new Type[] { Type.getType(Class.class) }
        );

        assertThat(methodKey, equalTo(new MethodKey("org/example/TestClass", "staticMethod", List.of())));
    }

    public void testParseCheckerMethodSignatureStaticMethodWithArgs() {
        var methodKey = InstrumentationServiceImpl.parseCheckerMethodSignature(
            "check$org_example_TestClass$$staticMethod",
            new Type[] { Type.getType(Class.class), Type.getType("I"), Type.getType(String.class) }
        );

        assertThat(methodKey, equalTo(new MethodKey("org/example/TestClass", "staticMethod", List.of("I", "java/lang/String"))));
    }

    public void testParseCheckerMethodSignatureStaticMethodInnerClass() {
        var methodKey = InstrumentationServiceImpl.parseCheckerMethodSignature(
            "check$org_example_TestClass$InnerClass$$staticMethod",
            new Type[] { Type.getType(Class.class) }
        );

        assertThat(methodKey, equalTo(new MethodKey("org/example/TestClass$InnerClass", "staticMethod", List.of())));
    }

    public void testParseCheckerMethodSignatureCtor() {
        var methodKey = InstrumentationServiceImpl.parseCheckerMethodSignature(
            "check$org_example_TestClass$",
            new Type[] { Type.getType(Class.class) }
        );

        assertThat(methodKey, equalTo(new MethodKey("org/example/TestClass", "<init>", List.of())));
    }

    public void testParseCheckerMethodSignatureCtorWithArgs() {
        var methodKey = InstrumentationServiceImpl.parseCheckerMethodSignature(
            "check$org_example_TestClass$",
            new Type[] { Type.getType(Class.class), Type.getType("I"), Type.getType(String.class) }
        );

        assertThat(methodKey, equalTo(new MethodKey("org/example/TestClass", "<init>", List.of("I", "java/lang/String"))));
    }

    public void testParseCheckerMethodSignatureOneDollarSign() {
        assertParseCheckerMethodSignatureThrows("has incorrect name format", "check$method", Type.getType(Class.class));
    }

    public void testParseCheckerMethodSignatureMissingClass() {
        assertParseCheckerMethodSignatureThrows("has incorrect name format", "check$$staticMethod", Type.getType(Class.class));
    }

    public void testParseCheckerMethodSignatureBlankClass() {
        assertParseCheckerMethodSignatureThrows("no class name", "check$$$staticMethod", Type.getType(Class.class));
    }

    public void testParseCheckerMethodSignatureStaticMethodIncorrectArgumentCount() {
        assertParseCheckerMethodSignatureThrows("It must have a first argument of Class<?> type", "check$ClassName$staticMethod");
    }

    public void testParseCheckerMethodSignatureStaticMethodIncorrectArgumentType() {
        assertParseCheckerMethodSignatureThrows(
            "It must have a first argument of Class<?> type",
            "check$ClassName$$staticMethod",
            Type.getType(String.class)
        );
    }

    public void testParseCheckerMethodSignatureInstanceMethod() {
        var methodKey = InstrumentationServiceImpl.parseCheckerMethodSignature(
            "check$org_example_TestClass$instanceMethod",
            new Type[] { Type.getType(Class.class), Type.getType(TestTargetClass.class) }
        );

        assertThat(methodKey, equalTo(new MethodKey("org/example/TestClass", "instanceMethod", List.of())));
    }

    public void testParseCheckerMethodSignatureInstanceMethodWithArgs() {
        var methodKey = InstrumentationServiceImpl.parseCheckerMethodSignature(
            "check$org_example_TestClass$instanceMethod",
            new Type[] { Type.getType(Class.class), Type.getType(TestTargetClass.class), Type.getType("I"), Type.getType(String.class) }
        );

        assertThat(methodKey, equalTo(new MethodKey("org/example/TestClass", "instanceMethod", List.of("I", "java/lang/String"))));
    }

    public void testParseCheckerMethodSignatureInstanceMethodIncorrectArgumentTypes() {
        assertParseCheckerMethodSignatureThrows(
            "It must have a first argument of Class<?> type",
            "check$org_example_TestClass$instanceMethod",
            Type.getType(String.class)
        );
    }

    public void testParseCheckerMethodSignatureInstanceMethodIncorrectArgumentCount() {
        assertParseCheckerMethodSignatureThrows(
            "a second argument of the class containing the method to instrument",
            "check$org_example_TestClass$instanceMethod",
            Type.getType(Class.class)
        );
    }

    public void testParseCheckerMethodSignatureInstanceMethodIncorrectArgumentTypes2() {
        assertParseCheckerMethodSignatureThrows(
            "a second argument of the class containing the method to instrument",
            "check$org_example_TestClass$instanceMethod",
            Type.getType(Class.class),
            Type.getType("I")
        );
    }

    private static void assertParseCheckerMethodSignatureThrows(String messageText, String methodName, Type... methodArgs) {
        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> InstrumentationServiceImpl.parseCheckerMethodSignature(methodName, methodArgs)
        );

        assertThat(exception.getMessage(), containsString(messageText));
    }

}
