/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

// Unit tests for delegating storage objects
public abstract class DelegateStorageObjectTests extends ESTestCase {
    public abstract StorageObject makeStorageObject(StorageObject delegate);

    public void testAsyncCpuNanosDelegated() {
        StorageObject delegate = TestStorageObjects.withAsyncCpuNanos(12345L);
        StorageObject obj = makeStorageObject(delegate);
        assertEquals(12345L, obj.asyncCpuNanos());
    }
}
