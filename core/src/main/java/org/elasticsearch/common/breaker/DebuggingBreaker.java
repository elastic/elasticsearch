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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public abstract class DebuggingBreaker implements CircuitBreaker {

    private final ConcurrentMap<BreakerKey, Object> keys = new ConcurrentHashMap<>();

    @Override
    public void addEstimateBytesAndMaybeBreak(BreakerKey key, long bytes) throws CircuitBreakingException {
        addEstimateBytesAndMaybeBreak(bytes, key.getLabel());
        key.incBytes(bytes);
        keys.putIfAbsent(key, bytes);
    }

    @Override
    public void addWithoutBreaking(BreakerKey key, long bytes) throws CircuitBreakingException {
        addWithoutBreaking(bytes);
        key.incBytes(bytes);
        keys.putIfAbsent(key, bytes);
    }

    @Override
    public void release(BreakerKey key) {
        Object removed = keys.remove(key);
        if (removed != null) {
            addWithoutBreaking(-key.getBytes());
        } else {
            throw new IllegalStateException();
        }
    }
}
