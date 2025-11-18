/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;

import java.lang.ref.Cleaner;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Leak tracking mechanism that allows for ensuring that a resource has been properly released before a given object is garbage collected.
 *
 */
public final class LeakTracker {

    private static final Logger logger = LogManager.getLogger(LeakTracker.class);

    private static final Cleaner cleaner = Cleaner.create();

    private static final int TARGET_RECORDS = 25;

    private final ConcurrentMap<String, Boolean> reportedLeaks = ConcurrentCollections.newConcurrentMap();

    public static final LeakTracker INSTANCE = new LeakTracker();

    private static volatile String contextHint = "";

    private LeakTracker() {}

    /**
     * Track the given object.
     *
     * @param obj object to track
     * @return leak object that must be released by a call to {@link LeakTracker.Leak#close()} before {@code obj} goes out of scope
     */
    public Leak track(Object obj) {
        return new Leak(obj);
    }

    /**
     * Set a hint string that will be recorded with every leak that is recorded. Used by unit tests to allow identifying the exact test
     * that caused a leak by setting the test name here.
     * @param hint hint value
     */
    public static void setContextHint(String hint) {
        contextHint = hint;
    }

    public static Releasable wrap(Releasable releasable) {
        if (Assertions.ENABLED == false) {
            return releasable;
        }
        var leak = INSTANCE.track(releasable);
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
        if (Assertions.ENABLED == false) {
            return refCounted;
        }
        var leak = INSTANCE.track(refCounted);
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

    public final class Leak implements Runnable {

        private static final AtomicReferenceFieldUpdater<Leak, Record> headUpdater = AtomicReferenceFieldUpdater.newUpdater(
            Leak.class,
            Record.class,
            "head"
        );

        private static final AtomicIntegerFieldUpdater<Leak> droppedRecordsUpdater = AtomicIntegerFieldUpdater.newUpdater(
            Leak.class,
            "droppedRecords"
        );

        @SuppressWarnings("unused")
        private volatile Record head;
        @SuppressWarnings("unused")
        private volatile int droppedRecords;

        private final AtomicBoolean closed = new AtomicBoolean(false);

        private final Cleaner.Cleanable cleanable;

        @SuppressWarnings("this-escape")
        private Leak(Object referent) {
            this.cleanable = cleaner.register(referent, this);
            headUpdater.set(this, new Record(Record.BOTTOM));
        }

        @Override
        public void run() {
            if (closed.compareAndSet(false, true) == false || logger.isErrorEnabled() == false) {
                return;
            }
            String records = toString();
            if (reportedLeaks.putIfAbsent(records, Boolean.TRUE) == null) {
                logger.error("LEAK: resource was not cleaned up before it was garbage-collected.{}", records);
            }
        }

        /**
         * Adds an access record that includes the current stack trace to the leak.
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
         * Stop tracking the object that this leak was created for.
         *
         * @return true if the leak was released by this call, false if the leak had already been released
         */
        public boolean close() {
            if (closed.compareAndSet(false, true)) {
                cleanable.clean();
                headUpdater.set(this, null);
                return true;
            }
            return false;
        }

        @Override
        public String toString() {
            Record oldHead = headUpdater.get(this);
            if (oldHead == null) {
                // Already closed
                return "";
            }

            final int dropped = droppedRecordsUpdater.get(this);
            int duped = 0;

            int present = oldHead.pos + 1;
            // Guess about 2 kilobytes per stack trace
            StringBuilder buf = new StringBuilder(present * 2048).append('\n');
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

        private final String contextHint = LeakTracker.contextHint;

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
            StringBuilder buf = new StringBuilder("\tin [").append(threadName).append("][").append(contextHint).append("]\n");
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
