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

package org.elasticsearch.index.translog;

import org.apache.lucene.util.Counter;

import java.util.HashMap;
import java.util.Map;

public class TranslogDeletionPolicy {

    /** Records how many views are held against each
     *  translog generation */
    private final Map<Long, Counter> translogRefCounts = new HashMap<>();

    /**
     * the translog generation that is requires to properly recover from the oldest non deleted
     * {@link org.apache.lucene.index.IndexCommit}.
     */
    private long minTranslogGenerationForRecovery = 1;

    public synchronized void setMinTranslogGenerationForRecovery(long newGen) {
        if (newGen < minTranslogGenerationForRecovery) {
            throw new IllegalArgumentException("minTranslogGenerationForRecovery can't go backwards. new [" + newGen + "] current [" +
                minTranslogGenerationForRecovery+ "]");
        }
        minTranslogGenerationForRecovery = newGen;
    }

    /**
     * acquires the basis generation for a new view. Any translog generation above, and including, the returned generation
     * will not be deleted until a corresponding call to {@link #releaseTranslogGenView(long)} is called.
     */
    synchronized long acquireTranslogGenForView() {
        translogRefCounts.computeIfAbsent(minTranslogGenerationForRecovery, l -> Counter.newCounter(false)).addAndGet(1);
        return minTranslogGenerationForRecovery;
    }

    /** returns the number of generations that were acquired for views */
    synchronized int pendingViewsCount() {
        return translogRefCounts.size();
    }

    /**
     * releases a generation that was acquired by {@link #acquireTranslogGenForView()}
     */
    synchronized void releaseTranslogGenView(long translogGen) {
        Counter current = translogRefCounts.get(translogGen);
        if (current == null || current.get() <= 0) {
            throw new IllegalArgumentException("translog gen [" + translogGen + "] wasn't acquired");
        }
        if (current.addAndGet(-1) == 0) {
            translogRefCounts.remove(translogGen);
        }
    }

    /**
     * returns the minimum translog generation that is still required by the system. Any generation below
     * the returned value may be safely deleted
     */
    synchronized long minTranslogGenRequired() {
        long viewRefs = translogRefCounts.keySet().stream().reduce(Math::min).orElse(Long.MAX_VALUE);
        return Math.min(viewRefs, minTranslogGenerationForRecovery);
    }

    /** returns the translog generation that will be used as a basis of a future store/peer recovery */
    public synchronized long getMinTranslogGenerationForRecovery() {
        return minTranslogGenerationForRecovery;
    }
}
