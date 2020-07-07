/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.recoveries;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public interface RecoveryTracker {
    class AccumulatingRecoveryTracker implements RecoveryTracker {

        public final Map<String, LongAdder> recoveredFiles = new HashMap<>();

        @Override
        public synchronized void addRecoveredBytesToFile(String name, long bytes) {
            recoveredFiles.computeIfAbsent(name, n -> new LongAdder()).add(bytes);
        }

        @Override
        public synchronized void trackFileEviction(String name) {
            recoveredFiles.get(name).reset();
        }

        @Override
        public void merge(RecoveryTracker recoveryTracker) {}

        @Override
        public synchronized Map<String, Long> getValues() {
            return recoveredFiles.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().sum()));
        }
    }

    void addRecoveredBytesToFile(String name, long bytes);

    void trackFileEviction(String name);

    void merge(RecoveryTracker recoveryTracker);

    Map<String, Long> getValues();
}
