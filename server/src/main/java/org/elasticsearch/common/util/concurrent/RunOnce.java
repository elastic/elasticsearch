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

package org.elasticsearch.common.util.concurrent;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A class which wraps an existing {@link Runnable} and allows to execute it exactly once
 * whatever the number of times the wrapped {@link Runnable#run()} method is called.
 */
public class RunOnce implements Runnable {

    private final AtomicBoolean executed;
    private final Runnable delegate;

    public RunOnce(final Runnable delegate) {
        this.delegate = Objects.requireNonNull(delegate);
        this.executed = new AtomicBoolean(false);
    }

    @Override
    public void run() {
        if (executed.compareAndSet(false, true)) {
            delegate.run();
        }
    }

    /**
     *  {@code true} if the {@link RunOnce} has been executed once.
     */
    public boolean hasRun() {
        return executed.get();
    }
}
