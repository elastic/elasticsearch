/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.codec.profiler.CounterMeasurement;
import org.elasticsearch.codec.profiler.Metric;
import org.elasticsearch.codec.profiler.MetricRecorder;
import org.elasticsearch.codec.profiler.ProfileCollector;
import org.elasticsearch.codec.profiler.Scope;
import org.elasticsearch.codec.profiler.Scopes;

import java.io.IOException;

/**
 * The only class of {@link ColumnarProfilingOverheadBenchmark} that uses the codec profiler API.
 */
final class ProfilingScope implements AutoCloseable {

    static final boolean AVAILABLE = true;

    private final Scopes.Binding binding;

    private ProfilingScope(Scopes.Binding binding) {
        this.binding = binding;
    }

    /**
     * Binds a scope that sends the readers opened on this thread to a new collector until the returned value is closed.
     * When {@code counters} and {@code timers} are both false, no scope is bound. {@code timers} adds elapsed time,
     * {@code cpuTime} adds CPU time, and {@code detail} gives every reader a detail collector of the collector, so each
     * measurement is recorded in both.
     */
    static ProfilingScope enter(boolean counters, boolean timers, boolean cpuTime, boolean detail) {
        if (counters == false && timers == false) {
            return new ProfilingScope(null);
        }
        final ProfileCollector collector = ProfileCollector.create(
            cpuTime ? ProfileCollector.Mode.ELAPSED_AND_CPU : timers ? ProfileCollector.Mode.ELAPSED : ProfileCollector.Mode.COUNTERS
        );
        final ProfileCollector target = detail ? collector.detail(bytes -> {}) : collector;
        return new ProfilingScope(Scopes.bind(everyReaderTo(target)));
    }

    /** The number of compressed blocks of {@code field} that one full scan inflates, or 0 when the codec counts none. */
    static long inflatedBlocks(LeafReader leaf, String field) throws IOException {
        final ProfileCollector collector = ProfileCollector.create(ProfileCollector.Mode.COUNTERS);
        try (Scopes.Binding ignored = Scopes.bind(everyReaderTo(collector))) {
            final BinaryDocValues column = leaf.getBinaryDocValues(field);
            while (column.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                column.binaryValue();
            }
        }
        return collector.snapshot().get(BLOCKS_DECOMPRESSED) instanceof CounterMeasurement counter ? counter.value() : 0L;
    }

    private static final Metric.Id BLOCKS_DECOMPRESSED = new Metric.Id("ColumNAR", "blocks", "decompressed");

    /** A scope whose readers all record into {@code metrics}. */
    private static Scope everyReaderTo(MetricRecorder metrics) {
        return (codec, segment, field, facts) -> metrics;
    }

    @Override
    public void close() {
        if (binding != null) {
            binding.close();
        }
    }
}
