/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.mockito;

import org.mockito.MockedConstruction;
import org.mockito.internal.creation.bytebuddy.SubclassByteBuddyMockMaker;
import org.mockito.internal.util.reflection.LenientCopyTool;
import org.mockito.invocation.MockHandler;
import org.mockito.mock.MockCreationSettings;
import org.mockito.plugins.MockMaker;

import java.util.Optional;
import java.util.function.Function;

import static org.elasticsearch.test.mockito.SecureMockUtil.wrap;

/**
 * A {@link MockMaker} that works with {@link SecurityManager}.
 */
public class SecureMockMaker implements MockMaker {

    // delegates to initializing util, which we don't want to have public
    public static void init() {
        SecureMockUtil.init();
    }

    // TODO: consider using InlineByteBuddyMockMaker, but this requires using a java agent for instrumentation
    private final SubclassByteBuddyMockMaker delegate;

    public SecureMockMaker() {
        delegate = wrap(SubclassByteBuddyMockMaker::new);
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

    @SuppressWarnings("rawtypes")
    @Override
    public <T> StaticMockControl<T> createStaticMock(Class<T> type, MockCreationSettings<T> settings, MockHandler handler) {
        return delegate.createStaticMock(type, settings, handler);
    }

    @Override
    public <T> ConstructionMockControl<T> createConstructionMock(
        Class<T> type,
        Function<MockedConstruction.Context, MockCreationSettings<T>> settingsFactory,
        Function<MockedConstruction.Context, MockHandler<T>> handlerFactory,
        MockedConstruction.MockInitializer<T> mockInitializer
    ) {
        return delegate.createConstructionMock(type, settingsFactory, handlerFactory, mockInitializer);
    }

    @Override
    public void clearAllCaches() {
        delegate.clearAllCaches();
    }
}
