/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.collect;

import java.util.NoSuchElementException;

public final class PersistentQueue<V> {

    private static final PersistentQueue<?> empty = new PersistentQueue<>(PersistentStack.empty(), PersistentStack.empty());

    @SuppressWarnings("unchecked")
    public static <V> PersistentQueue<V> empty() {
        return (PersistentQueue<V>) empty;
    }

    private final PersistentStack<V> inbound;
    private final PersistentStack<V> outbound;

    private PersistentQueue(PersistentStack<V> inbound, PersistentStack<V> outbound) {
        this.inbound = inbound;
        this.outbound = outbound;
    }

    public boolean isEmpty() {
        return inbound.isEmpty() && outbound.isEmpty();
    }

    public PersistentQueue<V> add(V item) {
        return new PersistentQueue<>(inbound.push(item), outbound);
    }

    public Tuple<V, PersistentQueue<V>> remove() {
        if (outbound.isEmpty() && inbound.isEmpty()) {
            throw new NoSuchElementException();
        } else if (outbound.isEmpty()) {
            PersistentStack<V> s = PersistentStack.empty();
            for (V item : inbound) {
                s = s.push(item);
            }
            return new Tuple<>(s.head(), new PersistentQueue<>(PersistentStack.empty(), s.tail()));
        } else {
            return new Tuple<>(outbound.head(), new PersistentQueue<>(inbound, outbound.tail()));
        }
    }
}
