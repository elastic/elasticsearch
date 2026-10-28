/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Leak tracking mechanism for ensuring that tracked resources are properly released. When assertions are enabled, resources registered
 * via {@link #track}, {@link #wrap(Releasable)}, or {@link #wrap(RefCounted)} are verified by {@link TrackingWindow}s in tests.
 */
public final class LeakTracker {

    private static final int TARGET_RECORDS = 25;

    /**
     * When assertions are enabled, holds all live {@link TrackedResource} instances for deterministic leak detection in tests.
     * {@code null} when assertions are disabled.
     */
    private static final Set<TrackedResource> activeTrackers = Assertions.ENABLED ? ConcurrentCollections.newConcurrentSet() : null;

    private LeakTracker() {}

    /**
     * Opens a tracking window for synchronous leak detection. Call {@link TrackingWindow#assertNoLeaks()} at the
     * end of the window (e.g. from {@code @After} or {@code @AfterClass}) to assert that all resources opened
     * within the window were properly released. Windows nest: {@link TrackingWindow#assertNoLeaks()} only checks
     * resources added after the window was opened, so suite-level resources are invisible to per-test windows
     * and vice versa. No-op (returns a no-op window) when assertions are disabled.
     */
    public static TrackingWindow newWindow() {
        return activeTrackers == null ? TrackingWindow.NOOP : new TrackingWindow(new HashSet<>(activeTrackers));
    }

    /**
     * A bounded window for leak detection. Created by {@link #newWindow()} and checked by {@link #assertNoLeaks()}.
     */
    public static final class TrackingWindow {

        private static final TrackingWindow NOOP = new TrackingWindow(null);

        private final Set<TrackedResource> preExistingTrackers;

        private TrackingWindow(Set<TrackedResource> preExistingTrackers) {
            this.preExistingTrackers = preExistingTrackers;
        }

        /**
         * Returns {@code true} if any resources opened within this window have not yet been released.
         * Unlike {@link #assertNoLeaks()}, this method does not drain detected leaks, making it safe to use
         * in a polling loop before a final call to {@link #assertNoLeaks()}.
         * No-op (returns {@code false}) when assertions are disabled.
         */
        public boolean hasLeaks() {
            if (preExistingTrackers == null) {
                return false;
            }
            List<TrackedResource> current = new ArrayList<>(activeTrackers);
            current.removeAll(preExistingTrackers);
            return current.isEmpty() == false;
        }

        /**
         * Asserts that all resources opened within this scope have been properly released. Drains detected leaks
         * from the global set so an enclosing scope does not double-report them. No-op when assertions are disabled.
         */
        public void assertNoLeaks() {
            if (preExistingTrackers == null) {
                return;
            }
            List<TrackedResource> leaked = new ArrayList<>(activeTrackers);
            leaked.removeAll(preExistingTrackers);
            activeTrackers.removeAll(leaked);
            if (leaked.isEmpty() == false) {
                StringBuilder sb = new StringBuilder("Leaked resources (").append(leaked.size()).append("):\n");
                for (TrackedResource trackedResource : leaked) {
                    sb.append(trackedResource.toString()).append('\n');
                }
                throw new AssertionError(sb.toString());
            }
        }
    }

    /**
     * Track the given object. Returns a handle that must be closed before {@code obj} goes out of scope.
     * Use this method when {@link #wrap(Releasable)} or {@link #wrap(RefCounted)} are not applicable.
     */
    public static Releasable track(Object obj) {
        if (activeTrackers != null) {
            TrackedResource trackedResource = new TrackedResource(obj);
            activeTrackers.add(trackedResource);
            return trackedResource;
        }
        return () -> {};
    }

    public static Releasable wrap(Releasable releasable) {
        if (activeTrackers == null) {
            return releasable;
        }
        TrackedResource leak = new TrackedResource(releasable);
        activeTrackers.add(leak);
        return new Releasable() {
            @Override
            public void close() {
                try {
                    releasable.close();
                } finally {
                    leak.close();
                }
            }

            @Override
            public int hashCode() {
                // It's legitimate to wrap the resource twice, with two different wrap() calls, which would yield different objects
                // if and only if assertions are enabled. So we'd better not ever use these things as map keys etc.
                throw new AssertionError("almost certainly a mistake to need the hashCode() of a leak-tracking Releasable");
            }

            @Override
            public boolean equals(Object obj) {
                // It's legitimate to wrap the resource twice, with two different wrap() calls, which would yield different objects
                // if and only if assertions are enabled. So we'd better not ever use these things as map keys etc.
                throw new AssertionError("almost certainly a mistake to compare a leak-tracking Releasable for equality");
            }
        };
    }

    public static RefCounted wrap(RefCounted refCounted) {
        if (activeTrackers == null) {
            return refCounted;
        }
        TrackedResource leak = new TrackedResource(refCounted);
        activeTrackers.add(leak);
        return new RefCounted() {
            @Override
            public void incRef() {
                leak.record();
                refCounted.incRef();
            }

            @Override
            public boolean tryIncRef() {
                leak.record();
                return refCounted.tryIncRef();
            }

            @Override
            public boolean decRef() {
                if (refCounted.decRef()) {
                    leak.close();
                    return true;
                }
                leak.record();
                return false;
            }

            @Override
            public boolean hasReferences() {
                return refCounted.hasReferences();
            }

            @Override
            public int hashCode() {
                // It's legitimate to wrap the resource twice, with two different wrap() calls, which would yield different objects
                // if and only if assertions are enabled. So we'd better not ever use these things as map keys etc.
                throw new AssertionError("almost certainly a mistake to need the hashCode() of a leak-tracking RefCounted");
            }

            @Override
            public boolean equals(Object obj) {
                // It's legitimate to wrap the resource twice, with two different wrap() calls, which would yield different objects
                // if and only if assertions are enabled. So we'd better not ever use these things as map keys etc.
                throw new AssertionError("almost certainly a mistake to compare a leak-tracking RefCounted for equality");
            }
        };
    }

    /**
     * Must be {@link #close()}d when the tracked object is released. Failure to do so is reported by
     * {@link LeakTracker.TrackingWindow#assertNoLeaks()}.
     */
    public static final class TrackedResource implements Releasable {

        private static final AtomicReferenceFieldUpdater<TrackedResource, Record> headUpdater = AtomicReferenceFieldUpdater.newUpdater(
            TrackedResource.class,
            Record.class,
            "head"
        );

        private static final AtomicIntegerFieldUpdater<TrackedResource> droppedRecordsUpdater = AtomicIntegerFieldUpdater.newUpdater(
            TrackedResource.class,
            "droppedRecords"
        );

        @SuppressWarnings("unused")
        private volatile Record head;
        @SuppressWarnings("unused")
        private volatile int droppedRecords;

        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final Object obj;

        private TrackedResource(Object obj) {
            this.obj = obj;
            head = new Record(Record.BOTTOM);
        }

        /**
         * Records the current stack trace as an access record.
         */
        public void record() {
            Record oldHead;
            Record newHead;
            boolean dropped;
            do {
                Record prevHead;
                if ((prevHead = oldHead = headUpdater.get(this)) == null) {
                    // already closed.
                    return;
                }
                final int numElements = oldHead.pos + 1;
                if (numElements >= TARGET_RECORDS) {
                    final int backOffFactor = Math.min(numElements - TARGET_RECORDS, 30);
                    if (dropped = Randomness.get().nextInt(1 << backOffFactor) != 0) {
                        prevHead = oldHead.next;
                    }
                } else {
                    dropped = false;
                }
                newHead = new Record(prevHead);
            } while (headUpdater.compareAndSet(this, oldHead, newHead) == false);
            if (dropped) {
                droppedRecordsUpdater.incrementAndGet(this);
            }
        }

        /**
         * Marks the tracked object as properly released.
         */
        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                headUpdater.set(this, null);
                activeTrackers.remove(this);
            }
        }

        @Override
        public String toString() {
            Record oldHead = headUpdater.get(this);
            if (oldHead == null) {
                return "";
            }

            final int dropped = droppedRecordsUpdater.get(this);
            int duped = 0;

            int present = oldHead.pos + 1;
            // Guess about 2 kilobytes per stack trace
            StringBuilder buf = new StringBuilder(present * 2048).append('\n');
            buf.append("Tracked object: ").append(obj).append('\n');
            buf.append("Recent access records: ").append('\n');

            int i = 1;
            Set<String> seen = Sets.newHashSetWithExpectedSize(present);
            for (; oldHead != Record.BOTTOM; oldHead = oldHead.next) {
                String s = oldHead.toString();
                if (seen.add(s)) {
                    if (oldHead.next == Record.BOTTOM) {
                        buf.append("Created at:").append('\n').append(s);
                    } else {
                        buf.append('#').append(i++).append(':').append('\n').append(s);
                    }
                } else {
                    duped++;
                }
            }

            if (duped > 0) {
                buf.append(": ").append(duped).append(" leak records were discarded because they were duplicates").append('\n');
            }

            if (dropped > 0) {
                buf.append(": ")
                    .append(dropped)
                    .append(" leak records were discarded because the leak record count is targeted to ")
                    .append(TARGET_RECORDS)
                    .append('.')
                    .append('\n');
            }
            buf.setLength(buf.length() - "\n".length());
            return buf.toString();
        }
    }

    private static final class Record extends Throwable {

        private static final Record BOTTOM = new Record();

        private final Record next;
        private final int pos;

        private final String threadName;

        Record(Record next) {
            this.next = next;
            this.pos = next.pos + 1;
            threadName = Thread.currentThread().getName();
        }

        private Record() {
            next = null;
            pos = -1;
            threadName = Thread.currentThread().getName();
        }

        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder("\tin [").append(threadName).append("]\n");
            StackTraceElement[] array = getStackTrace();
            // Skip the first three elements since those are just related to the leak tracker.
            for (int i = 3; i < array.length; i++) {
                StackTraceElement element = array[i];
                buf.append('\t');
                buf.append(element.toString());
                buf.append('\n');
            }
            return buf.toString();
        }
    }
}
