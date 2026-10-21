/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.tracking;

import org.elasticsearch.columnar.string.PageBudget;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Releasable;

/**
 * A page budget that charges a circuit breaker and gives back what it charged when it is released. The
 * storage it accounts for belongs to a reader and lives as long as the reader does, so a reader takes one
 * of these and closes it, rather than the accounting being spread over the calls that grew the storage.
 */
public final class BreakerPageBudget implements PageBudget, Releasable {

    private final CircuitBreaker breaker;
    private long charged;
    private boolean released;

    public BreakerPageBudget(CircuitBreaker breaker) {
        this.breaker = breaker;
    }

    @Override
    public void charge(long bytes) {
        assert released == false : "a released budget has given back what it held and cannot be charged again";
        breaker.addEstimateBytesAndMaybeBreak(bytes, "load blocks");
        charged += bytes;
    }

    @Override
    public void close() {
        released = true;
        if (charged > 0) {
            breaker.addWithoutBreaking(-charged);
            charged = 0;
        }
    }
}
