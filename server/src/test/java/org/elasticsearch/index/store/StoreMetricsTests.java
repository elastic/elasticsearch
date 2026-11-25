/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class StoreMetricsTests extends AbstractWireSerializingTestCase<StoreMetrics> {

    @Override
    protected Writeable.Reader<StoreMetrics> instanceReader() {
        return StoreMetrics::new;
    }

    @Override
    protected StoreMetrics createTestInstance() {
        return new StoreMetrics(randomNonNegativeLong());
    }

    @Override
    protected StoreMetrics mutateInstance(StoreMetrics instance) {
        long newBytesRead = randomValueOtherThan(instance.getBytesRead(), () -> randomNonNegativeLong());
        return new StoreMetrics(newBytesRead);
    }

    public void testMerge() {
        StoreMetrics a = new StoreMetrics(100);
        StoreMetrics b = new StoreMetrics(200);
        StoreMetrics merged = a.merge(b);
        assertEquals(300, merged.getBytesRead());
    }

    public void testCopy() {
        StoreMetrics original = new StoreMetrics(42);
        StoreMetrics copy = original.copy();
        assertEquals(original.getBytesRead(), copy.getBytesRead());
    }

    public void testDelta() {
        StoreMetrics metrics = new StoreMetrics(0);
        var delta = metrics.delta();
        metrics.addBytesRead(50);
        StoreMetrics result = delta.get();
        assertEquals(50, result.getBytesRead());
    }
}
