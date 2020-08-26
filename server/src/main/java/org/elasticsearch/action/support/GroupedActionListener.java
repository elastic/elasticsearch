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
package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.CountDown;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An action listener that delegates its results to another listener once
 * it has received N results (either successes or failures). This allows synchronous
 * tasks to be forked off in a loop with the same listener and respond to a
 * higher level listener once all tasks responded.
 */
public final class GroupedActionListener<T> implements ActionListener<T> {
    private final CountDown countDown;
    private final AtomicInteger pos = new AtomicInteger();
    private final AtomicArray<T> results;
    private final ActionListener<Collection<T>> delegate;
    private final AtomicReference<Exception> failure = new AtomicReference<>();

    /**
     * Creates a new listener
     * @param delegate the delegate listener
     * @param groupSize the group size
     */
    public GroupedActionListener(ActionListener<Collection<T>> delegate, int groupSize) {
        if (groupSize <= 0) {
            throw new IllegalArgumentException("groupSize must be greater than 0 but was " + groupSize);
        }
        results = new AtomicArray<>(groupSize);
        countDown = new CountDown(groupSize);
        this.delegate = delegate;
    }

    @Override
    public void onResponse(T element) {
        results.setOnce(pos.incrementAndGet() - 1, element);
        if (countDown.countDown()) {
            if (failure.get() != null) {
                delegate.onFailure(failure.get());
            } else {
                List<T> collect = this.results.asList();
                delegate.onResponse(Collections.unmodifiableList(collect));
            }
        }
    }

    @Override
    public void onFailure(Exception e) {
        if (failure.compareAndSet(null, e) == false) {
            failure.accumulateAndGet(e, (current, update) -> {
                // we have to avoid self-suppression!
                if (update != current) {
                    current.addSuppressed(update);
                }
                return current;
            });
        }
        if (countDown.countDown()) {
            delegate.onFailure(failure.get());
        }
    }
}
