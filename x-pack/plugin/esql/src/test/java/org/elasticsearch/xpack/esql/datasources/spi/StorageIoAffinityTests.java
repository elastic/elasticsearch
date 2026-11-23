/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;

public class StorageIoAffinityTests extends ESTestCase {

    public void testCurrentIsNullOutsideScope() {
        assertNull(StorageIoAffinity.current());
    }

    public void testOpenInstallsLeaseAndClearsOnClose() {
        RowGroupIo lease = new RowGroupIo();
        try (StorageIoAffinity.Scope scope = StorageIoAffinity.open(lease, true)) {
            assertSame(scope, StorageIoAffinity.current());
            assertSame(lease, scope.lease());
            assertTrue(scope.countGets);
        }
        assertNull(StorageIoAffinity.current());
    }

    public void testNestedScopeRestoresOuter() {
        RowGroupIo outerLease = new RowGroupIo();
        RowGroupIo innerLease = new RowGroupIo();
        try (StorageIoAffinity.Scope outer = StorageIoAffinity.open(outerLease, true)) {
            assertSame(outerLease, StorageIoAffinity.current().lease());
            try (StorageIoAffinity.Scope inner = StorageIoAffinity.open(innerLease, false)) {
                assertSame(innerLease, StorageIoAffinity.current().lease());
                assertFalse(inner.countGets);
            }
            assertSame(outer, StorageIoAffinity.current());
            assertSame(outerLease, StorageIoAffinity.current().lease());
            assertTrue(StorageIoAffinity.current().countGets);
        }
        assertNull(StorageIoAffinity.current());
    }

    public void testScopeRestoredAfterException() {
        RowGroupIo lease = new RowGroupIo();
        expectThrows(RuntimeException.class, () -> {
            try (StorageIoAffinity.Scope ignored = StorageIoAffinity.open(lease, true)) {
                throw new RuntimeException("boom");
            }
        });
        assertNull(StorageIoAffinity.current());
    }
}
