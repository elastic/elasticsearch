/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * Attaches failures to one another without ever creating a loop in the resulting exception graph.
 * <p>
 * ES|QL can hand the same exception instance to many splits (e.g. every waiter on a failed footer load), and
 * several places collect failures by suppressing later ones onto the first one they saw. Two such places adopting
 * two shared instances in opposite orders would leave A suppressing B and B suppressing A. Both the REST renderer
 * and the transport serializer bound the <em>depth</em> of such a graph but not the number of paths through it, so
 * rendering a loop can exhaust the heap.
 * <p>
 * Every ES|QL site that suppresses a failure onto one it did not create itself (and so may be shared) must go
 * through {@link #attach}. A single lock shared by all of them makes the reachability check and the edge it guards
 * atomic with respect to every other attach, so no interleaving of sites can close a loop. This is only ever on the
 * failure path, against small graphs. The lock is taken before the monitors of the exceptions it visits, so callers
 * must not hold an exception's monitor ({@code synchronized (exception)}) when calling {@link #attach}.
 * <p>
 * It does not stop two failures that share a deeper exception (e.g. two wrappers around one shared cause) from being
 * attached to each other: that shared exception is then reachable along two paths and rendered once per path. A
 * coordinator replaces a failure whose graph renders excessively (see {@code EsqlFailureBounds}).
 */
public final class SuppressedFailures {

    private static final Object LOCK = new Object();

    private SuppressedFailures() {}

    /**
     * Suppresses {@code failure} onto {@code carrier}, unless that would repeat or loop: it is skipped if it is
     * {@code null}, is the carrier itself, is already reachable from the carrier (through cause or suppressed links),
     * or can itself reach the carrier.
     *
     * @return {@code true} if {@code failure} was attached
     */
    public static boolean attach(Throwable carrier, Throwable failure) {
        if (failure == null || failure == carrier) {
            return false;
        }
        synchronized (LOCK) {
            if (reaches(carrier, failure) || reaches(failure, carrier)) {
                return false;
            }
            carrier.addSuppressed(failure);
            return true;
        }
    }

    /**
     * Whether {@code target} is {@code from} itself or can be reached from it through cause and suppressed links.
     * Safe on graphs that already loop.
     */
    static boolean reaches(Throwable from, Throwable target) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        Deque<Throwable> pending = new ArrayDeque<>();
        pending.push(from);
        while (pending.isEmpty() == false) {
            Throwable current = pending.pop();
            if (current == target) {
                return true;
            }
            if (seen.add(current) == false) {
                continue;
            }
            Throwable cause = current.getCause();
            if (cause != null) {
                pending.push(cause);
            }
            for (Throwable suppressed : current.getSuppressed()) {
                pending.push(suppressed);
            }
        }
        return false;
    }
}
