/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.Priority;

import java.util.concurrent.Callable;

public abstract class PrioritizedCallable<T> implements Callable<T>, Comparable<PrioritizedCallable<T>> {

    private final Priority priority;

    public static <T> PrioritizedCallable<T> wrap(Callable<T> callable, Priority priority) {
        return new Wrapped<>(callable, priority);
    }

    protected PrioritizedCallable(Priority priority) {
        this.priority = priority;
    }

    @Override
    public int compareTo(PrioritizedCallable<T> pc) {
        return priority.compareTo(pc.priority);
    }

    public Priority priority() {
        return priority;
    }

    static class Wrapped<T> extends PrioritizedCallable<T> {

        private final Callable<T> callable;

        private Wrapped(Callable<T> callable, Priority priority) {
            super(priority);
            this.callable = callable;
        }

        @Override
        public T call() throws Exception {
            return callable.call();
        }
    }
}
