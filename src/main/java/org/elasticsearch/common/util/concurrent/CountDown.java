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

import org.elasticsearch.ElasticsearchIllegalArgumentException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A simple thread safe count-down class that in contrast to a {@link CountDownLatch}
 * never blocks. This class is useful if a certain action has to wait for N concurrent 
 * tasks to return or a timeout to occur in order to proceed.
 */
public final class CountDown {

    private final AtomicInteger countDown;
    private final int originalCount;

    public CountDown(int count) {
        if (count < 0) {
            throw new ElasticsearchIllegalArgumentException("count must be greater or equal to 0 but was: " + count);
        }
        this.originalCount = count;
        this.countDown = new AtomicInteger(count);
    }

    /**
     * Decrements the count-down and returns <code>true</code> iff this call
     * reached zero otherwise <code>false</code>
     */
    public boolean countDown() {
        assert originalCount > 0;
        for (;;) {
            final int current = countDown.get();
            assert current >= 0;
            if (current == 0) {
                return false;
            }
            if (countDown.compareAndSet(current, current - 1)) {
                return current == 1;
            }
        }
    }

    /**
     * Fast forwards the count-down to zero and returns <code>true</code> iff
     * the count down reached zero with this fast forward call otherwise
     * <code>false</code>
     */
    public boolean fastForward() {
        assert originalCount > 0;
        assert countDown.get() >= 0;
        return countDown.getAndSet(0) > 0;
    }
    
    /**
     * Returns <code>true</code> iff the count-down has reached zero. Otherwise <code>false</code>
     */
    public boolean isCountedDown() {
        assert countDown.get() >= 0;
        return countDown.get() == 0;
    }
}
