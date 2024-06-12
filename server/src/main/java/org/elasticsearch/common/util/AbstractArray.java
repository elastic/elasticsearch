/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

abstract class AbstractArray implements BigArray {

    protected static final VarHandle VH_RELEASED_FIELD;

    static {
        try {
            VH_RELEASED_FIELD = MethodHandles.lookup().in(AbstractArray.class).findVarHandle(AbstractArray.class, "released", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("FieldMayBeFinal") // updated via VH_RELEASED_FIELD (and _only_ via VH_RELEASED_FIELD)
    private volatile int released = 0;

    private final BigArrays bigArrays;
    public final boolean clearOnResize;

    AbstractArray(BigArrays bigArrays, boolean clearOnResize) {
        this.bigArrays = bigArrays;
        this.clearOnResize = clearOnResize;
    }

    @Override
    public final void close() {
        if (VH_RELEASED_FIELD.compareAndSet(this, 0, 1)) {
            try {
                bigArrays.adjustBreaker(-ramBytesUsed(), true);
            } finally {
                doClose();
            }
        }
    }

    protected abstract void doClose();
}
