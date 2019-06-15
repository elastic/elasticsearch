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
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
        while (true) {
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
        while (true) {
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

        @SuppressWarnings("unchecked")
        private static final AtomicReferenceFieldUpdater<Leak<?>, Record> headUpdater =
            (AtomicReferenceFieldUpdater) AtomicReferenceFieldUpdater.newUpdater(Leak.class, Record.class, "head");

        @SuppressWarnings("unchecked")
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

            trackedHash = System.identityHashCode(referent);
            allLeaks.add(this);
            headUpdater.set(this, new Record(Record.BOTTOM));
            this.allLeaks = allLeaks;
        }

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

        @Override
        public String toString() {
            Record oldHead = headUpdater.getAndSet(this, null);
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
            Set<String> seen = new HashSet<>(present);
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
                buf.append(": ")
                    .append(duped)
                    .append(" leak records were discarded because they were duplicates")
                    .append('\n');
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

        Record(Record next) {
            this.next = next;
            this.pos = next.pos + 1;
        }

        private Record() {
            next = null;
            pos = -1;
        }

        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
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
