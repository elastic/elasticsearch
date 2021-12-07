/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.mockito;

import org.mockito.creation.instance.Instantiator;

/**
 * A wrapper for instantiating objects reflectively, but plays nice with SecurityManager.
 */
class SecureObjectInstantiator implements Instantiator {
    private final Instantiator delegate;

    SecureObjectInstantiator(Instantiator delegate) {
        this.delegate = delegate;
    }

    @Override
    public <T> T newInstance(Class<T> cls) {
        return SecureMockUtil.wrap(() -> delegate.newInstance(cls));
    }
}
