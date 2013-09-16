/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.Sets;
import jsr166e.ConcurrentHashMapV8;
import jsr166y.LinkedTransferQueue;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public abstract class ConcurrentCollections {

    private final static boolean useConcurrentHashMapV8 = Boolean.parseBoolean(System.getProperty("es.useConcurrentHashMapV8", "false"));
    private final static boolean useLinkedTransferQueue = Boolean.parseBoolean(System.getProperty("es.useLinkedTransferQueue", "false"));

    static final int aggressiveConcurrencyLevel;

    static {
        aggressiveConcurrencyLevel = Math.max(Runtime.getRuntime().availableProcessors() * 2, 16);
    }

    /**
     * Creates a new CHM with an aggressive concurrency level, aimed at high concurrent update rate long living maps.
     */
    public static <K, V> ConcurrentMap<K, V> newConcurrentMapWithAggressiveConcurrency() {
        if (useConcurrentHashMapV8) {
            return new ConcurrentHashMapV8<K, V>(16, 0.75f, aggressiveConcurrencyLevel);
        }
        return new ConcurrentHashMap<K, V>(16, 0.75f, aggressiveConcurrencyLevel);
    }

    public static <K, V> ConcurrentMap<K, V> newConcurrentMap() {
        if (useConcurrentHashMapV8) {
            return new ConcurrentHashMapV8<K, V>();
        }
        return new ConcurrentHashMap<K, V>();
    }

    /**
     * Creates a new CHM with an aggressive concurrency level, aimed at highly updateable long living maps.
     */
    public static <V> ConcurrentMapLong<V> newConcurrentMapLongWithAggressiveConcurrency() {
        return new ConcurrentHashMapLong<V>(ConcurrentCollections.<Long, V>newConcurrentMapWithAggressiveConcurrency());
    }

    public static <V> ConcurrentMapLong<V> newConcurrentMapLong() {
        return new ConcurrentHashMapLong<V>(ConcurrentCollections.<Long, V>newConcurrentMap());
    }

    public static <V> Set<V> newConcurrentSet() {
        return Sets.newSetFromMap(ConcurrentCollections.<V, Boolean>newConcurrentMap());
    }

    public static <T> Queue<T> newQueue() {
        if (useLinkedTransferQueue) {
            return new LinkedTransferQueue<T>();
        }
        return new ConcurrentLinkedQueue<T>();
    }

    public static <T> BlockingQueue<T> newBlockingQueue() {
        return new LinkedTransferQueue<T>();
    }

    private ConcurrentCollections() {

    }
}
