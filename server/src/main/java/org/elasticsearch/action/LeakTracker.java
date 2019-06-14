/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.elasticsearch.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class LeakTracker {

    private static final Logger logger = LogManager.getLogger(LeakTracker.class);

    private static final boolean ENABLED = Boolean.parseBoolean(System.getProperty("test.leak.tracker.enabled", "false"));

    private static final int TARGET_RECORDS = 4;

    private final Set<Leak<?>> allLeaks = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final ReferenceQueue<Object> refQueue = new ReferenceQueue<>();
    private final ConcurrentMap<String, Boolean> reportedLeaks = ConcurrentCollections.newConcurrentMap();

    private final String resourceType;

    public LeakTracker(Class<?> clazz) {
        this.resourceType = clazz.getName();
    }

    public final <T> Leak<T> track(T obj) {
        if (ENABLED == false) {
            return null;
        }

        reportLeak();
        return new Leak<>(obj, refQueue, allLeaks);
    }

    private void clearRefQueue() {
        for (; ; ) {
            Leak<?> ref = (Leak<?>) refQueue.poll();
            if (ref == null) {
                break;
            }
            ref.dispose();
        }
    }

    private void reportLeak() {
        if (!logger.isErrorEnabled()) {
            clearRefQueue();
            return;
        }
        for (; ; ) {
            Leak<?> ref = (Leak<?>) refQueue.poll();
            if (ref == null) {
                break;
            }

            if (!ref.dispose()) {
                continue;
            }

            String records = ref.toString();
            if (reportedLeaks.putIfAbsent(records, Boolean.TRUE) == null) {
                logger.error("LEAK: {} was not cleaned up before it was garbage-collected.{}", resourceType, records);
            }
        }
    }

    public static final class Leak<T> extends WeakReference<Object> {

        @SuppressWarnings("unchecked") // generics and updaters do not mix.
        private static final AtomicReferenceFieldUpdater<Leak<?>, Record> headUpdater =
            (AtomicReferenceFieldUpdater) AtomicReferenceFieldUpdater.newUpdater(Leak.class, Record.class, "head");

        @SuppressWarnings("unchecked") // generics and updaters do not mix.
        private static final AtomicIntegerFieldUpdater<Leak<?>> droppedRecordsUpdater =
            (AtomicIntegerFieldUpdater) AtomicIntegerFieldUpdater.newUpdater(Leak.class, "droppedRecords");

        @SuppressWarnings("unused")
        private volatile Record head;
        @SuppressWarnings("unused")
        private volatile int droppedRecords;

        private final Set<Leak<?>> allLeaks;
        private final int trackedHash;

        Leak(Object referent, ReferenceQueue<Object> refQueue, Set<Leak<?>> allLeaks) {
            super(referent, refQueue);

            assert referent != null;

            // Store the hash of the tracked object to later assert it in the close(...) method.
            // It's important that we not store a reference to the referent as this would disallow it from
            // be collected via the WeakReference.
            trackedHash = System.identityHashCode(referent);
            allLeaks.add(this);
            // Create a new Record so we always have the creation stacktrace included.
            headUpdater.set(this, new Record(Record.BOTTOM));
            this.allLeaks = allLeaks;
        }

        public void record() {
            Record oldHead;
            Record prevHead;
            Record newHead;
            boolean dropped;
            do {
                if ((prevHead = oldHead = headUpdater.get(this)) == null) {
                    // already closed.
                    return;
                }
                final int numElements = oldHead.pos + 1;
                if (numElements >= TARGET_RECORDS) {
                    final int backOffFactor = Math.min(numElements - TARGET_RECORDS, 30);
                    if (dropped = ThreadLocalRandom.current().nextInt(1 << backOffFactor) != 0) {
                        prevHead = oldHead.next;
                    }
                } else {
                    dropped = false;
                }
                newHead = new Record(prevHead);
            } while (!headUpdater.compareAndSet(this, oldHead, newHead));
            if (dropped) {
                droppedRecordsUpdater.incrementAndGet(this);
            }
        }

        boolean dispose() {
            clear();
            return allLeaks.remove(this);
        }

        public boolean close() {
            if (allLeaks.remove(this)) {
                // Call clear so the reference is not even enqueued.
                clear();
                headUpdater.set(this, null);
                return true;
            }
            return false;
        }

        public boolean close(T trackedObject) {
            assert trackedHash == System.identityHashCode(trackedObject);

            try {
                return close();
            } finally {
                reachabilityFence0(trackedObject);
            }
        }

        private static void reachabilityFence0(Object ref) {
            if (ref != null) {
                synchronized (ref) {
                    // empty on purpose
                }
            }
        }
    }

    private static final class Record extends Throwable {

        private static final Record BOTTOM = new Record();

        private final Record next;
        private final int pos;

        Record(Record next) {
            this.next = next;
            this.pos = next.pos + 1;
        }

        private Record() {
            next = null;
            pos = -1;
        }
    }
}
