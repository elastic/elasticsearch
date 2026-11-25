/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.Nullable;

/**
 * Mutable, per-operator counter struct for format-specific read instrumentation.
 * <p>
 * Instances are created by {@link FormatReader#newReadCounters()} once per driver (in
 * {@code AsyncExternalSourceOperatorFactory.get(DriverContext)}) and owned by the operator, not
 * the reader. Each {@link FormatReader#read} and {@link RangeAwareFormatReader#readRange} call
 * receives the counters via {@link FormatReadContext#readCounters()} /
 * {@link RangeReadContext#readCounters()} and writes into them directly. The operator snapshots
 * them via {@link #snapshot()} to produce the {@code format_reader} field of the operator status.
 * <p>
 * This design makes the counter struct independent of the reader's copy/wither graph: no reader
 * field to forward through every {@code with*} method, no per-operator mint protocol. The reader
 * casts the incoming struct to its format-specific type (e.g. {@code ParquetReaderCounters}) and
 * writes into it.
 * <p>
 * Callers that do not participate in instrumentation (tests, benchmarks, planning-time metadata
 * reads) pass {@code null}; readers guard all counter writes with a null check.
 */
public interface FormatReadCounters {

    /**
     * Returns an immutable snapshot of the current counter values, suitable for folding into the
     * operator-status envelope as the {@code format_reader} field.
     */
    @Nullable
    FormatReaderStatus snapshot();
}
