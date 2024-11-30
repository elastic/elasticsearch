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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;

@ESTestCase.WithoutSecurityManager
public class InstrumentationServiceImplTests extends ESTestCase {

    final InstrumentationService instrumentationService = new InstrumentationServiceImpl();

    static class TestTargetClass {}

    interface TestChecker {
        void check$org_example_TestTargetClass$staticMethod(Class<?> clazz, int arg0, String arg1, Object arg2);

        void check$$instanceMethodNoArgs(Class<?> clazz, TestTargetClass that);

        void check$$instanceMethodWithArgs(Class<?> clazz, TestTargetClass that, int x, int y);
    }

    interface TestCheckerOverloads {
        void check$org_example_TestTargetClass$staticMethodWithOverload(Class<?> clazz, int x, int y);

        void check$org_example_TestTargetClass$staticMethodWithOverload(Class<?> clazz, int x, String y);
    }

    interface TestCheckerCtors {
        void check$org_example_TestTargetClass$(Class<?> clazz);

        void check$org_example_TestTargetClass$(Class<?> clazz, int x, String y);
    }

    public void testInstrumentationTargetLookup() throws IOException, ClassNotFoundException {
        Map<MethodKey, CheckMethod> checkMethods = instrumentationService.lookupMethodsToInstrument(TestChecker.class.getName());

        assertThat(checkMethods, aMapWithSize(3));
        assertThat(
            checkMethods,
            hasEntry(
                equalTo(new MethodKey("org/example/TestTargetClass", "staticMethod", List.of("I", "java/lang/String", "java/lang/Object"))),
                equalTo(
                    new CheckMethod(
                        "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestChecker",
                        "check$org_example_TestTargetClass$staticMethod",
                        List.of("Ljava/lang/Class;", "I", "Ljava/lang/String;", "Ljava/lang/Object;")
                    )
                )
            )
        );
        assertThat(
            checkMethods,
            hasEntry(
                equalTo(
                    new MethodKey(
                        "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestTargetClass",
                        "instanceMethodNoArgs",
                        List.of()
                    )
                ),
                equalTo(
                    new CheckMethod(
                        "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestChecker",
                        "check$$instanceMethodNoArgs",
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
                equalTo(
                    new MethodKey(
                        "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestTargetClass",
                        "instanceMethodWithArgs",
                        List.of("I", "I")
                    )
                ),
                equalTo(
                    new CheckMethod(
                        "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestChecker",
                        "check$$instanceMethodWithArgs",
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

    public void testInstrumentationTargetLookupWithOverloads() throws IOException, ClassNotFoundException {
        Map<MethodKey, CheckMethod> checkMethods = instrumentationService.lookupMethodsToInstrument(TestCheckerOverloads.class.getName());

        assertThat(checkMethods, aMapWithSize(2));
        assertThat(
            checkMethods,
            hasEntry(
                equalTo(new MethodKey("org/example/TestTargetClass", "staticMethodWithOverload", List.of("I", "java/lang/String"))),
                equalTo(
                    new CheckMethod(
                        "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestCheckerOverloads",
                        "check$org_example_TestTargetClass$staticMethodWithOverload",
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
                        "check$org_example_TestTargetClass$staticMethodWithOverload",
                        List.of("Ljava/lang/Class;", "I", "I")
                    )
                )
            )
        );
    }

    public void testInstrumentationTargetLookupWithCtors() throws IOException, ClassNotFoundException {
        Map<MethodKey, CheckMethod> checkMethods = instrumentationService.lookupMethodsToInstrument(TestCheckerCtors.class.getName());

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

    public void testParseCheckerMethodSignatureStaticMethod() {
        var methodKey = InstrumentationServiceImpl.parseCheckerMethodSignature(
            "check$org_example_TestClass$staticMethod",
            new Type[] { Type.getType(Class.class) }
        );

        assertThat(methodKey, equalTo(new MethodKey("org/example/TestClass", "staticMethod", List.of())));
    }

    public void testParseCheckerMethodSignatureStaticMethodWithArgs() {
        var methodKey = InstrumentationServiceImpl.parseCheckerMethodSignature(
            "check$org_example_TestClass$staticMethod",
            new Type[] { Type.getType(Class.class), Type.getType("I"), Type.getType(String.class) }
        );

        assertThat(methodKey, equalTo(new MethodKey("org/example/TestClass", "staticMethod", List.of("I", "java/lang/String"))));
    }

    public void testParseCheckerMethodSignatureStaticMethodInnerClass() {
        var methodKey = InstrumentationServiceImpl.parseCheckerMethodSignature(
            "check$org_example_TestClass$InnerClass$staticMethod",
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

    public void testParseCheckerMethodSignatureIncorrectName() {
        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> InstrumentationServiceImpl.parseCheckerMethodSignature("check$staticMethod", new Type[] { Type.getType(Class.class) })
        );

        assertThat(exception.getMessage(), containsString("has incorrect name format"));
    }

    public void testParseCheckerMethodSignatureStaticMethodIncorrectArgumentCount() {
        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> InstrumentationServiceImpl.parseCheckerMethodSignature("check$ClassName$staticMethod", new Type[] {})
        );
        assertThat(exception.getMessage(), containsString("It must have a first argument of Class<?> type"));
    }

    public void testParseCheckerMethodSignatureStaticMethodIncorrectArgumentType() {
        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> InstrumentationServiceImpl.parseCheckerMethodSignature(
                "check$ClassName$staticMethod",
                new Type[] { Type.getType(String.class) }
            )
        );
        assertThat(exception.getMessage(), containsString("It must have a first argument of Class<?> type"));
    }

    public void testParseCheckerMethodSignatureInstanceMethod() {
        var methodKey = InstrumentationServiceImpl.parseCheckerMethodSignature(
            "check$$instanceMethod",
            new Type[] { Type.getType(Class.class), Type.getType(TestTargetClass.class) }
        );

        assertThat(
            methodKey,
            equalTo(
                new MethodKey(
                    "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestTargetClass",
                    "instanceMethod",
                    List.of()
                )
            )
        );
    }

    public void testParseCheckerMethodSignatureInstanceMethodWithArgs() {
        var methodKey = InstrumentationServiceImpl.parseCheckerMethodSignature(
            "check$$instanceMethod",
            new Type[] { Type.getType(Class.class), Type.getType(TestTargetClass.class), Type.getType("I"), Type.getType(String.class) }
        );

        assertThat(
            methodKey,
            equalTo(
                new MethodKey(
                    "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestTargetClass",
                    "instanceMethod",
                    List.of("I", "java/lang/String")
                )
            )
        );
    }

    public void testParseCheckerMethodSignatureInstanceMethodIncorrectArgumentTypes() {
        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> InstrumentationServiceImpl.parseCheckerMethodSignature("check$$instanceMethod", new Type[] { Type.getType(String.class) })
        );
        assertThat(exception.getMessage(), containsString("It must have a first argument of Class<?> type"));
    }

    public void testParseCheckerMethodSignatureInstanceMethodIncorrectArgumentCount() {
        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> InstrumentationServiceImpl.parseCheckerMethodSignature("check$$instanceMethod", new Type[] { Type.getType(Class.class) })
        );
        assertThat(exception.getMessage(), containsString("a second argument of the class containing the method to instrument"));
    }

    public void testParseCheckerMethodSignatureInstanceMethodIncorrectArgumentTypes2() {
        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> InstrumentationServiceImpl.parseCheckerMethodSignature(
                "check$$instanceMethod",
                new Type[] { Type.getType(Class.class), Type.getType("I") }
            )
        );
        assertThat(exception.getMessage(), containsString("a second argument of the class containing the method to instrument"));
    }
}
