/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.batching;

import org.elasticsearch.test.ESTestCase;

import java.util.Iterator;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RequestBatcherFactoryTests extends ESTestCase {
    @SuppressWarnings("unchecked")
    public static RequestBatcherFactory<TestAccount> createMockFactory(BatchingComponents components) {
        var mockRequestBatcher = mock(RequestBatcher.class);
        when(mockRequestBatcher.iterator()).thenReturn(mock(Iterator.class));

        var mockFactory = mock(RequestBatcherFactory.class);
        when(mockFactory.create(any())).thenReturn(mockRequestBatcher);

        return mock(RequestBatcherFactory.class);
    }

    public record TestAccount(String name) {}
}
