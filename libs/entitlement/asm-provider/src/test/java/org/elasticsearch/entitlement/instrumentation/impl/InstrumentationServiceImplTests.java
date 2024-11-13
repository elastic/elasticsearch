/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.instrumentation.impl;

import org.elasticsearch.entitlement.bridge.InstrumentationTarget;
import org.elasticsearch.entitlement.instrumentation.CheckerMethod;
import org.elasticsearch.entitlement.instrumentation.InstrumentationService;
import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;

@ESTestCase.WithoutSecurityManager
public class InstrumentationServiceImplTests extends ESTestCase {

    final InstrumentationService instrumentationService = new InstrumentationServiceImpl();

    interface TestEntitlementChecker {
        @InstrumentationTarget(className = "org/example/TestTargetClass", methodName = "staticMethod", isStatic = true)
        void checkStaticMethod(Class<?> clazz, int arg0, String arg1, Object arg2);

        @InstrumentationTarget(className = "org/example/TestTargetClass", methodName = "someMethod")
        void checkInstanceMethodNoArgs(Class<?> clazz);

        @InstrumentationTarget(className = "org/example/TestTargetClass2", methodName = "someMethod2")
        void checkInstanceMethodWithArgs(Class<?> clazz, int x, int y);
    }

    public void testInstrumentationTargetLookup() throws IOException, ClassNotFoundException {

        Map<MethodKey, CheckerMethod> methodsMap = instrumentationService.lookupMethodsToInstrument(TestEntitlementChecker.class.getName());

        assertThat(methodsMap, aMapWithSize(3));

        assertThat(
            methodsMap,
            hasEntry(
                equalTo(
                    new MethodKey("org/example/TestTargetClass", "staticMethod", List.of("I", "java/lang/String", "java/lang/Object"), true)
                ),
                equalTo(
                    new CheckerMethod(
                        "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestEntitlementChecker",
                        "checkStaticMethod",
                        List.of("Ljava/lang/Class;", "I", "Ljava/lang/String;", "Ljava/lang/Object;")
                    )
                )
            )
        );

        assertThat(
            methodsMap,
            hasEntry(
                equalTo(new MethodKey("org/example/TestTargetClass", "someMethod", List.of(), false)),
                equalTo(
                    new CheckerMethod(
                        "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestEntitlementChecker",
                        "checkInstanceMethodNoArgs",
                        List.of("Ljava/lang/Class;")
                    )
                )
            )
        );

        assertThat(
            methodsMap,
            hasEntry(
                equalTo(new MethodKey("org/example/TestTargetClass2", "someMethod2", List.of("I", "I"), false)),
                equalTo(
                    new CheckerMethod(
                        "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestEntitlementChecker",
                        "checkInstanceMethodWithArgs",
                        List.of("Ljava/lang/Class;", "I", "I")
                    )
                )
            )
        );
    }
}
