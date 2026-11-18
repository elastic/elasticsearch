/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.entitlement.runtime.api.EntitlementDelegation;

import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Mock for plugin code that calls the server utilities.
 */
final class TestPluginCode {
    private TestPluginCode() {}

    static Class<?> callCallerUtility(PolicyCheckerImpl checker) {
        return TestCallerUtility.requestingClass(checker);
    }

    static Class<?> callCallerUtilityWithInlineDelegation(PolicyCheckerImpl checker) {
        return TestCallerUtility.requestingClassWithInlineDelegation(checker);
    }

    static Class<?> callCallerUtilityNested(PolicyCheckerImpl checker) {
        return TestCallerUtility.nestedRequestingClass(checker);
    }

    static Class<?> callCallerUtilityThroughOtherMethod(PolicyCheckerImpl checker) {
        return TestCallerUtility.requestingClassThroughOtherMethod(checker);
    }

    static Class<?> callCallerUtilityThroughStream(PolicyCheckerImpl checker) {
        Function<PolicyCheckerImpl, Class<?>> function = TestCallerUtility::requestingClass;
        return Stream.of(checker).map(function).findFirst().orElseThrow();
    }

    static Class<?> callCallerUtilityOutsideCall(PolicyCheckerImpl checker) {
        return TestCallerUtility.requestingClassOutsideCall(checker);
    }

    static Class<?> callServerThroughCallerUtility(PolicyCheckerImpl checker) {
        return TestServerCode.callCallerUtility(checker);
    }

    static Class<?> callCallerUtilityThroughOtherCallerUtility(PolicyCheckerImpl checker) {
        return TestOtherCallerUtility.callCallerUtility(checker);
    }

    static Class<?> callCallerUtilityThroughOtherCallerUtilityMethodReference(PolicyCheckerImpl checker) {
        return TestOtherCallerUtility.call(TestCallerUtility.requestingClassSupplier(checker));
    }

    static void delegatedCheckThroughCallerUtility(PolicyCheckerImpl checker) {
        TestCallerUtility.delegatedCheck(checker);
    }

    static Class<?> callFailingCallerUtility(PolicyCheckerImpl checker) throws Exception {
        return TestCallerUtility.failingRequestingClass(checker);
    }

    static <T> T callCallerUtilityWithCallback(Supplier<T> callback) {
        return TestCallerUtility.withCallback(callback);
    }

    static TestCreatorUtility createCreatorUtility() {
        return new TestCreatorUtility();
    }

    static TestCreatorUtility createCreatorUtilityThroughThisCall() {
        return new TestCreatorUtility("delegates to this()");
    }

    static TestCreatorUtility createCreatorUtilityWithOtherConstructor() {
        return new TestCreatorUtility(1);
    }

    static TestCreatorUtility createCreatorUtilitySubclass() {
        return new TestCreatorUtility() {};
    }

    static TestCreatorUtility createCreatorUtilityThroughConstructorReference() {
        Supplier<TestCreatorUtility> factory = TestCreatorUtility::new;
        return factory.get();
    }

    static TestCreatorUtility createCreatorUtilityThroughCallerUtility() {
        return TestCallerUtility.createCreatorUtility();
    }

    static Class<?> requestingClass(PolicyCheckerImpl checker) {
        return checker.requestingClass(TestPluginCode.class);
    }

    static EntitlementDelegation delegateAsCaller() {
        return EntitlementDelegation.asCaller();
    }
}
