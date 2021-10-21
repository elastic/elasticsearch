/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.mockito.internal.creation.bytebuddy.SubclassByteBuddyMockMaker;
import org.mockito.internal.util.reflection.LenientCopyTool;
import org.mockito.invocation.MockHandler;
import org.mockito.mock.MockCreationSettings;
import org.mockito.plugins.MockMaker;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.DomainCombiner;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A {@link MockMaker} that works with {@link SecurityManager}.
 */
public class SecureMockMaker implements MockMaker {

    private static final AccessControlContext context;
    static {
        // This combiner extracts the protection domain of mockito if it exists and
        // uses only that. Without it, the mock maker delegated calls would run with
        // the Elasticsearch test framework on the call stack. This effectively removes
        // the test framework from the call stack, so that we only need to grant privileges
        // to mockito itself.
        DomainCombiner combiner = (current, assigned) -> Arrays.stream(current)
            .filter(pd -> pd.getCodeSource().getLocation().getFile().contains("mockito-core"))
            .findFirst()
            .map(pd -> new ProtectionDomain[]{ pd })
            .orElse(current);

        AccessControlContext acc = new AccessControlContext(AccessController.getContext(), combiner);

        // getContext must be called with the new acc so that a combined context will be created
        context = AccessController.doPrivileged((PrivilegedAction<AccessControlContext>) AccessController::getContext, acc);
    }

    // forces static init to run
    public static void init() {}

    // TODO: consider using InlineByteBuddyMockMaker, but this requires using a java agent for instrumentation
    private final SubclassByteBuddyMockMaker delegate;

    public SecureMockMaker() {
        delegate = wrap(SubclassByteBuddyMockMaker::new);
    }

    // wrap the given call to play nice with SecurityManager
    private static <T> T wrap(Supplier<T> call) {
        return AccessController.doPrivileged((PrivilegedAction<T>) call::get, context);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public <T> T createMock(MockCreationSettings<T> mockCreationSettings, MockHandler mockHandler) {
        return wrap(() -> delegate.createMock(mockCreationSettings, mockHandler));
    }

    @SuppressWarnings("rawtypes")
    @Override
    public <T> Optional<T> createSpy(MockCreationSettings<T> settings, MockHandler handler, T object) {
        // spies are not implemented by the bytebuddy delegate implementation
        return wrap(() -> {
            T instance = delegate.createMock(settings, handler);
            new LenientCopyTool().copyToMock(object, instance);
            return Optional.of(instance);
        });
    }

    @SuppressWarnings("rawtypes")
    @Override
    public MockHandler getHandler(Object o) {
        return delegate.getHandler(o);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void resetMock(Object o, MockHandler mockHandler, MockCreationSettings mockCreationSettings) {
        wrap(() -> {
            delegate.resetMock(o, mockHandler, mockCreationSettings);
            return (Void) null;
        });
    }

    @Override
    public TypeMockability isTypeMockable(Class<?> type) {
        return delegate.isTypeMockable(type);
    }

    @Override
    public void clearAllCaches() {
        delegate.clearAllCaches();
    }
}
