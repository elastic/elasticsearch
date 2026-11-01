/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Extension point for reporting JVM heap bytes required to load trained models referenced by ingest pipelines.
 * Implemented by the ML plugin; consumed by serverless index-tier memory metrics without a compile dependency on ML.
 */
public interface IngestModelMemoryProvider {

    record HeapRequirement(long heapBytes, boolean isExact) {}

    AtomicReference<IngestModelMemoryProvider> REFERENCE = new AtomicReference<>(Noop.INSTANCE);

    static IngestModelMemoryProvider getInstance() {
        return REFERENCE.get();
    }

    static void setInstance(IngestModelMemoryProvider instance) {
        Objects.requireNonNull(instance, "provider instance must not be null");
        REFERENCE.set(instance);
    }

    /**
     * Aggregate JVM heap bytes for trained models referenced by ingest pipelines; no heap-to-container conversion.
     */
    HeapRequirement getRequiredHeapBytes();

    final class Noop implements IngestModelMemoryProvider {

        static final Noop INSTANCE = new Noop();

        private Noop() {}

        @Override
        public HeapRequirement getRequiredHeapBytes() {
            return new HeapRequirement(0L, true);
        }
    }
}
