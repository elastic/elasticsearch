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

public class Lazy<T, E extends Exception> {

    private final CheckedSupplier<T, E> supplier;
    private final Consumer<T> finalizer;
    private volatile T value;

    public Lazy(CheckedSupplier<T, E> supplier) {
        this(supplier, v -> {});
    }

    public Lazy(CheckedSupplier<T, E> supplier, Consumer<T> finalizer) {
        this.supplier = supplier;
        this.finalizer = finalizer;
    }

    public T getOrCompute() throws E {
        final T result = value; // Read volatile just once...
        return result == null ? maybeCompute(supplier) : result;
    }

    public synchronized void clear() {
        if (value != null) {
            finalizer.accept(value);
            value = null;
        }
    }

    private synchronized T maybeCompute(CheckedSupplier<T, E> supplier) throws E {
        if (value == null) {
            value = Objects.requireNonNull(supplier.get());
        }
        return value;
    }

}
