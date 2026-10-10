/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

/**
 * Test seam so HTTP cancel tests in another package can wrap a real {@code HttpStorageObject}
 * the way production does ({@link ConcurrencyLimitedStorageObject}) and inspect the permit
 * count. {@code ConcurrencyLimitedStorageObject} is package-private.
 */
public final class ConcurrencyLimitTestSupport {

    private final ConcurrencyLimiter limiter;
    private final StorageObject object;

    public ConcurrencyLimitTestSupport(StorageObject delegate, int permits) {
        this.limiter = new ConcurrencyLimiter("http", new ExternalSourceSettings.BlobStoreConcurrency(permits, false));
        this.object = new ConcurrencyLimitedStorageObject(delegate, limiter);
    }

    public StorageObject object() {
        return object;
    }

    public int availablePermits() {
        return limiter.availablePermits();
    }
}
