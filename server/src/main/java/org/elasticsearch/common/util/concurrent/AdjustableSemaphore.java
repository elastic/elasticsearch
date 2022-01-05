/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import java.util.concurrent.Semaphore;

public class AdjustableSemaphore extends Semaphore {

    private final Object maxPermitsMutex = new Object();
    private int maxPermits;

    public AdjustableSemaphore(int maxPermits, boolean fair) {
        super(maxPermits, fair);
        this.maxPermits = maxPermits;
    }

    public void setMaxPermits(int permits) {
        synchronized (maxPermitsMutex) {
            final int diff = Math.subtractExact(permits, maxPermits);
            if (diff > 0) {
                // add permits
                release(diff);
            } else if (diff < 0) {
                // remove permits
                reducePermits(Math.negateExact(diff));
            }

            maxPermits = permits;
        }
    }
}
