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

import jsr166y.LinkedTransferQueue;
import org.elasticsearch.common.collect.MapBackedSet;

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

    private final static boolean useNonBlockingMap = Boolean.parseBoolean(System.getProperty("es.useNonBlockingMap", "false"));
    private final static boolean useLinkedTransferQueue = Boolean.parseBoolean(System.getProperty("es.useLinkedTransferQueue", "false"));

    public static <K, V> ConcurrentMap<K, V> newConcurrentMap() {
//        if (useNonBlockingMap) {
//            return new NonBlockingHashMap<K, V>();
//        }
        return new ConcurrentHashMap<K, V>();
    }

    public static <V> ConcurrentMapLong<V> newConcurrentMapLong() {
//        if (useNonBlockingMap) {
//            return new NonBlockingHashMapLong<V>();
//        }
        return new ConcurrentHashMapLong<V>();
    }

    public static <V> Set<V> newConcurrentSet() {
//        if (useNonBlockingMap) {
//            return new NonBlockingHashSet<V>();
//        }
        return new MapBackedSet<V>(new ConcurrentHashMap<V, Boolean>());
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
