/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm;

import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.core.Nullable;

import java.util.Objects;
import java.util.function.Consumer;

public abstract class AbstractAsyncInstrument<T extends AutoCloseable> extends AbstractInstrument<T> implements AutoCloseable {

    private final Consumer<AbstractInstrument<?>> deregisterFunc;

    protected AbstractAsyncInstrument(Meter meter, Builder<T> builder, Consumer<AbstractInstrument<?>> deregisterFunc) {
        super(meter, builder);
        this.deregisterFunc = deregisterFunc;
    }

    @Override
    void setProvider(@Nullable Meter meter) {
        var oldInstrument = delegate.getAndSet(instrumentBuilder.apply(Objects.requireNonNull(meter)));
        closeOtelInstrument(oldInstrument);
    }

    @Override
    public void close() {
        // deregister this instrument first and close the underlying one second: this avoids the setProvider() method being called in
        // the meantime and creating a new OTel instrument that'd leak out
        deregisterFunc.accept(this);
        closeOtelInstrument(getInstrument());
    }

    private static <T extends AutoCloseable> void closeOtelInstrument(T oldInstrument) {
        try {
            oldInstrument.close();
        } catch (Exception e) {
            assert true : "OTel metrics must not throw on close()";
            throw new IllegalStateException("OTel metrics must not throw on close()", e);
        }
    }
}
