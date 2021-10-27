/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.cache;

public class RemovalNotification<K, V> {
    public enum RemovalReason {
        REPLACED,
        INVALIDATED,
        EVICTED
    }

    private final K key;
    private final V value;
    private final RemovalReason removalReason;

    public RemovalNotification(K key, V value, RemovalReason removalReason) {
        this.key = key;
        this.value = value;
        this.removalReason = removalReason;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public RemovalReason getRemovalReason() {
        return removalReason;
    }
}
