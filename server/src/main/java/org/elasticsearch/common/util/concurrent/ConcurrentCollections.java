/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import java.util.Collections;
import java.util.Deque;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedTransferQueue;

public abstract class ConcurrentCollections {

    static final int aggressiveConcurrencyLevel;

    static {
        aggressiveConcurrencyLevel = Math.max(Runtime.getRuntime().availableProcessors() * 2, 16);
    }

    /**
     * Creates a new CHM with an aggressive concurrency level, aimed at high concurrent update rate long living maps.
     */
    public static <K, V> ConcurrentMap<K, V> newConcurrentMapWithAggressiveConcurrency() {
        return newConcurrentMapWithAggressiveConcurrency(16);
    }

    /**
     * Creates a new CHM with an aggressive concurrency level, aimed at high concurrent update rate long living maps.
     */
    public static <K, V> ConcurrentMap<K, V> newConcurrentMapWithAggressiveConcurrency(int initalCapacity) {
        return new ConcurrentHashMap<>(initalCapacity, 0.75f, aggressiveConcurrencyLevel);
    }

    public static <K, V> ConcurrentMap<K, V> newConcurrentMap() {
        return new ConcurrentHashMap<>();
    }

    public static <V> Set<V> newConcurrentSet() {
        return Collections.newSetFromMap(ConcurrentCollections.<V, Boolean>newConcurrentMap());
    }

    public static <T> Queue<T> newQueue() {
        return new ConcurrentLinkedQueue<>();
    }

    public static <T> Deque<T> newDeque() {
        return new ConcurrentLinkedDeque<>();
    }

    public static <T> BlockingQueue<T> newBlockingQueue() {
        return new LinkedTransferQueue<>();
    }

    private ConcurrentCollections() {

    }
}
