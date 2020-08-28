/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.CheckedSupplier;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Encapsulates a {@link CheckedSupplier} which is lazily invoked once on the
 * first call to {@code #getOrCompute()}. The value which the
 * <code>supplier</code> returns is memorized and will be served until
 * {@code #reset()} is called. Each value returned by {@code #getOrCompute()},
 * newly minted or cached, will be passed to the <code>onGet</code>
 * {@link Consumer}. On {@code #reset()} the value will be passed to the
 * <code>onReset</code> {@code Consumer} and the next {@code #getOrCompute()}
 * will regenerate the value.
 */
public final class LazyInitializable<T, E extends Exception> {

    private final CheckedSupplier<T, E> supplier;
    private final Consumer<T> onGet;
    private final Consumer<T> onReset;
    private volatile T value;

    /**
     * Creates the simple LazyInitializable instance.
     *
     * @param supplier
     *            The {@code CheckedSupplier} to generate values which will be
     *            served on {@code #getOrCompute()} invocations.
     */
    public LazyInitializable(CheckedSupplier<T, E> supplier) {
        this(supplier, v -> {}, v -> {});
    }

    /**
     * Creates the complete LazyInitializable instance.
     *
     * @param supplier
     *            The {@code CheckedSupplier} to generate values which will be
     *            served on {@code #getOrCompute()} invocations.
     * @param onGet
     *            A {@code Consumer} which is called on each value, newly forged or
     *            stale, that is returned by {@code #getOrCompute()}
     * @param onReset
     *            A {@code Consumer} which is invoked on the value that will be
     *            erased when calling {@code #reset()}
     */
    public LazyInitializable(CheckedSupplier<T, E> supplier, Consumer<T> onGet, Consumer<T> onReset) {
        this.supplier = supplier;
        this.onGet = onGet;
        this.onReset = onReset;
    }

    /**
     * Returns a value that was created by <code>supplier</code>. The value might
     * have been previously created, if not it will be created now, thread safe of
     * course.
     */
    public T getOrCompute() throws E {
        final T readOnce = value; // Read volatile just once...
        final T result = readOnce == null ? maybeCompute(supplier) : readOnce;
        onGet.accept(result);
        return result;
    }

    /**
     * Clears the value, if it has been previously created by calling
     * {@code #getOrCompute()}. The <code>onReset</code> will be called on this
     * value. The next call to {@code #getOrCompute()} will recreate the value.
     */
    public synchronized void reset() {
        if (value != null) {
            onReset.accept(value);
            value = null;
        }
    }

    /**
     * Creates a new value thread safely.
     */
    private synchronized T maybeCompute(CheckedSupplier<T, E> supplier) throws E {
        if (value == null) {
            value = Objects.requireNonNull(supplier.get());
        }
        return value;
    }

}
