/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import java.util.concurrent.atomic.AtomicBoolean;

abstract class AbstractArray implements BigArray {

    private final BigArrays bigArrays;
    public final boolean clearOnResize;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    AbstractArray(BigArrays bigArrays, boolean clearOnResize) {
        this.bigArrays = bigArrays;
        this.clearOnResize = clearOnResize;
    }

    @Override
    public final void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                bigArrays.adjustBreaker(-ramBytesUsed(), true);
            } finally {
                doClose();
            }
        }
    }

    protected abstract void doClose();
}
