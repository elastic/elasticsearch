/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.mockito;

import org.mockito.creation.instance.Instantiator;
import org.mockito.internal.creation.instance.DefaultInstantiatorProvider;
import org.mockito.mock.MockCreationSettings;
import org.mockito.plugins.InstantiatorProvider2;

/**
 * A wrapper around the default provider which itself just wraps
 * {@link Instantiator} instances to play nice with {@link SecurityManager}.
 */
public class SecureObjectInstantiatorProvider implements InstantiatorProvider2 {
    private final DefaultInstantiatorProvider delegate;

    public SecureObjectInstantiatorProvider() {
        delegate = new DefaultInstantiatorProvider();
    }

    @Override
    public Instantiator getInstantiator(MockCreationSettings<?> settings) {
        return new SecureObjectInstantiator(delegate.getInstantiator(settings));
    }
}
