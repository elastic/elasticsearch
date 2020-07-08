/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.recoveries;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public interface PersistentCacheTracker {
    class FilterPersistentCacheTracker implements PersistentCacheTracker {
        private PersistentCacheTracker delegate;

        public FilterPersistentCacheTracker(PersistentCacheTracker delegate) {
            this.delegate = delegate;
        }

        public synchronized void trackPersistedBytesToFile(String name, long bytes) {
            delegate.trackPersistedBytesToFile(name, bytes);
        }

        public synchronized void trackFileEviction(String name) {
            delegate.trackFileEviction(name);
        }

        public synchronized PersistentCacheTracker merge(PersistentCacheTracker recoveryTracker) {
            return delegate.merge(recoveryTracker);
        }

        public synchronized Map<String, Long> getValues() {
            return delegate.getValues();
        }

        public synchronized void setNewDelegate(PersistentCacheTracker newDelegate) {
            PersistentCacheTracker oldDelegate = this.delegate;
            this.delegate = newDelegate;
            newDelegate.merge(oldDelegate);
        }
    }

    class AccumulatingRecoveryTracker implements PersistentCacheTracker {

        public final Map<String, LongAdder> recoveredFiles = new HashMap<>();

        @Override
        public synchronized void trackPersistedBytesToFile(String name, long bytes) {
            recoveredFiles.computeIfAbsent(name, n -> new LongAdder()).add(bytes);
        }

        @Override
        public synchronized void trackFileEviction(String name) {
            LongAdder longAdder = recoveredFiles.get(name);
            if (longAdder != null) {
                longAdder.reset();
            }
        }

        @Override
        public PersistentCacheTracker merge(PersistentCacheTracker recoveryTracker) {
            return this;
        }

        @Override
        public synchronized Map<String, Long> getValues() {
            return recoveredFiles.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().sum()));
        }
    }

    PersistentCacheTracker NO_OP = new PersistentCacheTracker() {
        @Override
        public void trackPersistedBytesToFile(String name, long bytes) {

        }

        @Override
        public void trackFileEviction(String name) {

        }

        @Override
        public PersistentCacheTracker merge(PersistentCacheTracker recoveryTracker) {
            return recoveryTracker;
        }

        @Override
        public Map<String, Long> getValues() {
            return Collections.emptyMap();
        }
    };

    void trackPersistedBytesToFile(String name, long bytes);

    void trackFileEviction(String name);

    PersistentCacheTracker merge(PersistentCacheTracker recoveryTracker);

    Map<String, Long> getValues();
}
