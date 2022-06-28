/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.mockito;

import org.mockito.internal.configuration.InjectingAnnotationEngine;
import org.mockito.plugins.AnnotationEngine;

import static org.elasticsearch.test.mockito.SecureMockUtil.wrap;

public class SecureAnnotationEngine implements AnnotationEngine {
    private final AnnotationEngine delegate;

    public SecureAnnotationEngine() {
        delegate = wrap(InjectingAnnotationEngine::new);
    }

    @Override
    public AutoCloseable process(Class<?> clazz, Object testInstance) {
        return wrap(() -> delegate.process(clazz, testInstance));
    }
}
