/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2025 Elasticsearch B.V.
 */

package org.elasticsearch.index.codec.vectors.es819.hnsw;

import org.apache.lucene.util.hnsw.HnswConcurrentMergeBuilder;
import org.apache.lucene.util.hnsw.OnHeapHnswGraph;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Provide (read-and-write) striped locks for access to nodes of an {@link OnHeapHnswGraph}. For use
 * by {@link HnswConcurrentMergeBuilder} and its HnswGraphBuilders.
 */
final class HnswLock {
    private static final int NUM_LOCKS = 512;
    private final ReentrantReadWriteLock[] locks;

    HnswLock() {
        locks = new ReentrantReadWriteLock[NUM_LOCKS];
        for (int i = 0; i < NUM_LOCKS; i++) {
            locks[i] = new ReentrantReadWriteLock();
        }
    }

    Lock read(int level, int node) {
        int lockid = hash(level, node) % NUM_LOCKS;
        Lock lock = locks[lockid].readLock();
        lock.lock();
        return lock;
    }

    Lock write(int level, int node) {
        int lockid = hash(level, node) % NUM_LOCKS;
        Lock lock = locks[lockid].writeLock();
        lock.lock();
        return lock;
    }

    private static int hash(int v1, int v2) {
        return v1 * 31 + v2;
    }
}
