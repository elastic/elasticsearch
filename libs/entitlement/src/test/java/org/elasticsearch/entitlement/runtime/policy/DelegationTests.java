/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.bridge.NotEntitledException;
import org.elasticsearch.entitlement.runtime.delegation.ActiveDelegation;
import org.elasticsearch.entitlement.runtime.delegation.DelegationHolder;
import org.elasticsearch.entitlement.runtime.delegation.DelegationPolicy;
import org.elasticsearch.entitlement.runtime.delegation.DelegationRuntime;
import org.elasticsearch.entitlement.runtime.policy.entitlements.ExitVMEntitlement;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.function.Supplier;

import static org.elasticsearch.entitlement.runtime.policy.PolicyManager.ALL_UNNAMED;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests {@link org.elasticsearch.entitlement.runtime.api.EntitlementDelegation} with the fixture classes
 * {@link TestCallerUtility}, {@link TestOtherCallerUtility}, {@link TestCreatorUtility}, {@link TestServerCode} and
 * {@link TestPluginCode}. {@code TestPluginCode} is a plugin, and the other fixtures are server code. Server is entitled to
 * exit the VM, the plugin is not.
 * <p>
 * The fixtures need "Test" in their names: {@link TestPolicyManager} treats the other classes of
 * {@code org.elasticsearch.entitlement} as part of the entitlement library, whose stack frames delegation skips.
 */
public class DelegationTests extends ESTestCase {

    private PolicyCheckerImpl checker;
    private DelegationPolicy previousPolicy;

    @Before
    public void installDelegationPolicy() {
        var policyManager = new TestPolicyManager(
            new Policy("server", List.of(new Scope(ALL_UNNAMED, List.of(new ExitVMEntitlement())))),
            Map.of(),
            type -> type.getNestHost() == TestPluginCode.class
                ? PolicyManager.PolicyScope.plugin("test-plugin", ALL_UNNAMED)
                : PolicyManager.PolicyScope.server(ALL_UNNAMED),
            EmptyPathLookup.INSTANCE,
            List.of(),
            List.of()
        );
        policyManager.setActive(true);
        policyManager.setTriviallyAllowingTestCode(false);
        checker = new PolicyCheckerImpl(Set.of(), PolicyManager.class.getModule(), policyManager, EmptyPathLookup.INSTANCE);
        previousPolicy = DelegationRuntime.policy();
        DelegationRuntime.install(policyManager.delegationPolicy());
    }

    @After
    public void restoreDelegationPolicy() {
        DelegationRuntime.install(previousPolicy);
    }

    public void testCodeOutsideCallIsNotDelegated() {
        assertEquals(TestCallerUtility.class, TestPluginCode.callCallerUtilityOutsideCall(checker));
        assertEquals(TestCreatorUtility.class, TestPluginCode.createCreatorUtility().requestingClassOutsideCall(checker));
    }

    public void testAsCallerIsCheckedAsTheCaller() {
        assertEquals(TestPluginCode.class, TestPluginCode.callCallerUtility(checker));
        assertEquals(TestPluginCode.class, TestPluginCode.callCallerUtilityWithInlineDelegation(checker));
        assertEquals(TestPluginCode.class, TestPluginCode.callCallerUtilityNested(checker));
        assertEquals(TestPluginCode.class, TestPluginCode.callCallerUtilityThroughOtherMethod(checker));
        assertEquals(TestPluginCode.class, TestPluginCode.callCallerUtilityThroughStream(checker));
    }

    public void testAsCallerCalledByServerCodeIsCheckedAsThatServerCode() {
        assertEquals(TestServerCode.class, TestPluginCode.callServerThroughCallerUtility(checker));
    }

    public void testAsCallerUnderAsCallerIsCheckedAsTheOuterCaller() {
        assertEquals(TestPluginCode.class, TestPluginCode.callCallerUtilityThroughOtherCallerUtility(checker));
        // through a method reference, only JDK and entitlement library frames are between the two delegating classes
        assertEquals(TestPluginCode.class, TestPluginCode.callCallerUtilityThroughOtherCallerUtilityMethodReference(checker));
    }

    public void testCodeCalledUnderDelegationIsCheckedAsItself() {
        assertEquals(TestServerCode.class, TestPluginCode.callCallerUtilityWithCallback(() -> TestServerCode.requestingClass(checker)));
    }

    public void testDelegatedCheckDoesNotUseServerEntitlements() {
        checker.checkEntitlementPresent(TestCallerUtility.class, ExitVMEntitlement.class);
        var exception = expectThrows(NotEntitledException.class, () -> TestPluginCode.delegatedCheckThroughCallerUtility(checker));
        assertThat(exception.getMessage(), containsString("class [" + TestPluginCode.class + "]"));
        assertThat(exception.getMessage(), containsString("delegated by [" + TestCallerUtility.class + "]"));
    }

    public void testDelegationEndsWhenTheOperationThrows() {
        var exception = expectThrows(Exception.class, () -> TestPluginCode.callFailingCallerUtility(checker));
        assertEquals("failed in [" + TestPluginCode.class.getName() + "]", exception.getMessage());
        assertNull(DelegationHolder.current());
        assertEquals(TestCallerUtility.class, TestPluginCode.callCallerUtilityOutsideCall(checker));
    }

    public void testAsCreatorIsCheckedAsTheCreator() {
        assertEquals(TestPluginCode.class, TestPluginCode.createCreatorUtility().requestingClass(checker));
        assertEquals(TestPluginCode.class, TestPluginCode.createCreatorUtilityThroughThisCall().requestingClass(checker));
        assertEquals(TestPluginCode.class, TestPluginCode.createCreatorUtilityWithOtherConstructor().requestingClass(checker));
        assertEquals(TestPluginCode.class, TestPluginCode.createCreatorUtilityThroughConstructorReference().requestingClass(checker));
        assertEquals(TestServerCode.class, TestServerCode.createCreatorUtility().requestingClass(checker));
    }

    public void testAsCreatorAppliesOnAnyThread() {
        var pluginCreated = TestPluginCode.createCreatorUtility();
        assertEquals(TestPluginCode.class, onOtherThread(() -> pluginCreated.requestingClass(checker)));
    }

    public void testAsCallerAppliesOnlyOnTheCallingThread() {
        List<Class<?>> requestingClasses = TestPluginCode.callCallerUtilityWithCallback(
            () -> List.of(
                TestCallerUtility.requestingClassOutsideCall(checker),
                onOtherThread(() -> TestCallerUtility.requestingClassOutsideCall(checker))
            )
        );
        assertEquals(List.of(TestPluginCode.class, TestCallerUtility.class), requestingClasses);
    }

    public void testSubclassIsTheCreator() {
        var subclassInstance = TestPluginCode.createCreatorUtilitySubclass();
        assertEquals(subclassInstance.getClass(), subclassInstance.requestingClass(checker));
    }

    public void testAsCreatorUnderAsCallerIsCheckedAsTheOuterCaller() {
        assertEquals(TestPluginCode.class, TestPluginCode.createCreatorUtilityThroughCallerUtility().requestingClass(checker));
    }

    public void testNestedDelegationRestoresTheOuterOne() {
        var pluginCreated = TestPluginCode.createCreatorUtility();
        var serverCreated = TestServerCode.createCreatorUtility();
        Class<?> afterReturn = pluginCreated.withCallback(() -> {
            assertEquals(TestServerCode.class, serverCreated.requestingClass(checker));
            return pluginCreated.requestingClassOutsideCall(checker);
        });
        Class<?> afterThrow = pluginCreated.withCallback(() -> {
            expectThrows(IllegalStateException.class, () -> serverCreated.withCallback(() -> { throw new IllegalStateException(); }));
            return pluginCreated.requestingClassOutsideCall(checker);
        });
        assertEquals(TestPluginCode.class, afterReturn);
        assertEquals(TestPluginCode.class, afterThrow);
        assertNull(DelegationHolder.current());
    }

    public void testPluginCodeCannotDelegate() {
        var exception = expectThrows(IllegalCallerException.class, TestPluginCode::delegateAsCaller);
        assertThat(exception.getMessage(), containsString("[" + TestPluginCode.class.getName() + "] may not delegate"));
    }

    public void testActiveDelegationOfCodeThatCanDelegateApplies() {
        var serverDelegation = new ActiveDelegation(TestServerCode.class, TestPluginCode.class);
        assertEquals(TestPluginCode.class, DelegationHolder.call(serverDelegation, () -> TestServerCode.requestingClass(checker)));
    }

    public void testActiveDelegationOfCodeThatCannotDelegateIsIgnored() {
        var pluginDelegation = new ActiveDelegation(TestPluginCode.class, TestServerCode.class);
        assertEquals(TestPluginCode.class, DelegationHolder.call(pluginDelegation, () -> TestPluginCode.requestingClass(checker)));
    }

    public void testAsCreatorOutsideConstructorFails() {
        var exception = expectThrows(IllegalStateException.class, TestServerCode::asCreatorFromMethod);
        assertThat(exception.getMessage(), containsString("not from [" + TestServerCode.class.getName() + ".asCreatorFromMethod]"));

        var staticInitializerFailure = expectThrows(
            ExceptionInInitializerError.class,
            () -> assertNotNull(TestServerCode.StaticCreatorDelegation.DELEGATION)
        );
        assertThat(staticInitializerFailure.getCause(), instanceOf(IllegalStateException.class));
        assertThat(staticInitializerFailure.getCause().getMessage(), containsString(".<clinit>]"));
    }

    @SuppressForbidden(reason = "the type of the private holder field shows which version the multi-release jar selected")
    public void testHolderMatchesTheJavaVersion() throws NoSuchFieldException {
        Class<?> holderType = DelegationHolder.class.getDeclaredField("CURRENT").getType();
        assertEquals(Runtime.version().feature() >= 25 ? "java.lang.ScopedValue" : ThreadLocal.class.getName(), holderType.getName());
    }

    private static <T> T onOtherThread(Supplier<T> task) {
        FutureTask<T> future = new FutureTask<>(task::get);
        Thread thread = new Thread(future, "delegation-test");
        thread.start();
        try {
            thread.join();
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new AssertionError(e);
        }
    }
}
