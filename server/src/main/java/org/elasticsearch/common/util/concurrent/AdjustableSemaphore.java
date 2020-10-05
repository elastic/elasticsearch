/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util.concurrent;

import java.util.concurrent.Semaphore;

public class AdjustableSemaphore extends Semaphore {

    private final Object maxPermitsMutex = new Object();
    private int maxPermits;

    public AdjustableSemaphore(int maxPermits, boolean fair) {
        super(maxPermits, fair);
        this.maxPermits = maxPermits;
    }

    public void setMaxPermits(int permits) {
        synchronized (maxPermitsMutex) {
            final int diff = Math.subtractExact(permits, maxPermits);
            if (diff > 0) {
                // add permits
                release(diff);
            } else if (diff < 0) {
                // remove permits
                reducePermits(Math.negateExact(diff));
            }

            maxPermits = permits;
        }
    }

    public int getMaxPermits() {
        return maxPermits;
    }
}
