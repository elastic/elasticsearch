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

package org.elasticsearch.common.breaker;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public abstract class DebuggingBreaker implements CircuitBreaker {

    private final ConcurrentMap<BreakerKey, LinkedList<CircuitNode>> state = new ConcurrentHashMap<>();
    private final boolean stashStacks;
    private final boolean debug;

    public DebuggingBreaker() {
        this(false, false);
    }

    public DebuggingBreaker(boolean debug, boolean stashStacks) {
        this.debug = debug;
        this.stashStacks = stashStacks;
    }

    @Override
    public void addEstimateBytesAndMaybeBreak(BreakerKey key, long bytes) throws CircuitBreakingException {
        addEstimateBytesAndMaybeBreak(bytes, key.getLabel());
        modifyKeyAndState(key, bytes);
    }

    @Override
    public void addWithoutBreaking(BreakerKey key, long bytes) throws CircuitBreakingException {
        addWithoutBreaking(bytes);
        modifyKeyAndState(key, bytes);
    }

    @Override
    public void release(BreakerKey key) {
        if (debug) {
            LinkedList<CircuitNode> removed = state.remove(key);
            if (removed != null) {
                long delta = -key.getBytes();
                addWithoutBreaking(delta);
                key.incBytes(delta);
            } else {
                throw new IllegalStateException("Key does not exist in breaker");
            }
        } else {
            long delta = -key.getBytes();
            addWithoutBreaking(delta);
            key.incBytes(delta);
        }
    }

    public Map<CircuitBreaker.BreakerKey, LinkedList<CircuitNode>> getState() {
        return Collections.unmodifiableMap(state);
    }

    private void modifyKeyAndState(BreakerKey key, long bytes) {
        key.incBytes(bytes);

        if (debug) {
            LinkedList<CircuitNode> value = new LinkedList<>();
            LinkedList<CircuitNode> oldValue = state.putIfAbsent(key, value);
            if (oldValue != null) {
                value = oldValue;
            }

            if (stashStacks) {
                StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
                value.add(new CircuitNode(bytes, stackTrace));
            } else {
                value.add(new CircuitNode(bytes));
            }
        }
    }

    public static class CircuitNode {

        private final long delta;
        private final StackTraceElement[] stackTraceElements;

        private CircuitNode(long delta) {
            this.delta = delta;
            this.stackTraceElements = null;
        }

        private CircuitNode(long delta, StackTraceElement[] stackTraceElements) {
            this.delta = delta;
            this.stackTraceElements = stackTraceElements;
        }

        public long getDelta() {
            return delta;
        }

        public StackTraceElement[] getStackTraceElements() {
            return stackTraceElements;
        }

        @Override
        public String toString() {
            return "CircuitNode{" +
                "delta=" + delta +
                ", stackTraceElements=" + Arrays.toString(stackTraceElements) +
                '}';
        }
    }
}
