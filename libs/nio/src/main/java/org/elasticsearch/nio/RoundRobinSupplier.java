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

package org.elasticsearch.nio;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

final class RoundRobinSupplier<S> implements Supplier<S> {

    private final AtomicBoolean selectorsSet = new AtomicBoolean(false);
    private volatile S[] selectors;
    private AtomicInteger counter = new AtomicInteger(0);

    RoundRobinSupplier() {
        this.selectors = null;
    }

    RoundRobinSupplier(S[] selectors) {
        this.selectors = selectors;
        this.selectorsSet.set(true);
    }

    @Override
    public S get() {
        S[] selectors = this.selectors;
        return selectors[counter.getAndIncrement() % selectors.length];
    }

    void setSelectors(S[] selectors) {
        if (selectorsSet.compareAndSet(false, true)) {
            this.selectors = selectors;
        } else {
            throw new AssertionError("Selectors already set. Should only be set once.");
        }
    }

    int count() {
        return selectors.length;
    }
}
