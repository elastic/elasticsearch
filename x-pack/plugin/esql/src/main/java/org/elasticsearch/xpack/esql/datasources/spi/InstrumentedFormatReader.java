/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

/**
 * Capability marker for {@link FormatReader} implementations that maintain I/O counters
 * accessible via {@link FormatReader#statusSnapshot()}.
 * <p>
 * Presence of this interface is the capability signal: callers use
 * {@code instanceof InstrumentedFormatReader} rather than a separate flag.
 * Readers that track no counters simply omit it — their {@code statusSnapshot()} returns {@code null}.
 * <p>
 * <b>Invariant:</b> every wither on an implementing reader returns an instance that shares
 * the same counter struct as its parent. {@link #withFreshCounters()} is the <em>only</em>
 * method that allocates a new struct, and the operator factory ({@code AsyncExternalSourceOperatorFactory})
 * is the <em>only</em> caller — once per {@code get(DriverContext)} invocation. This ensures
 * that a base reader and the entire transitive fork closure it spawns all write into the same
 * struct, so snapshotting the base reflects everything any descendant recorded.
 * <p>
 * Do <em>not</em> add this interface to {@link FormatReader} as a default method: a reader
 * whose {@code statusSnapshot()} is non-null but that silently omits the interface makes
 * "has counters but forgot to mint" indistinguishable from "has no counters".
 * <p>
 * Modelled on {@link DynamicThresholdAware}.
 */
public interface InstrumentedFormatReader {

    /**
     * Returns a reader copy that starts accumulating into a <b>fresh</b> counter struct,
     * leaving the receiver's struct intact and at zero contribution from this operator's reads.
     * <p>
     * Node-shared caches (footer caches, I/O watermark) MUST be forwarded, not reallocated —
     * only the counter struct is new. The returned reader is otherwise identical to the receiver.
     */
    FormatReader withFreshCounters();

    /**
     * Convenience helper that calls {@link #withFreshCounters()} when {@code reader} implements
     * this interface, or returns {@code reader} unchanged when it does not. Callers should use
     * this instead of an inline {@code instanceof} check.
     * <p>
     * {@link #withFreshCounters()} must return a non-null copy; a null return violates the interface
     * contract and throws {@link IllegalStateException}.
     */
    static FormatReader freshCounters(FormatReader reader) {
        if (reader instanceof InstrumentedFormatReader instrumented) {
            FormatReader minted = instrumented.withFreshCounters();
            if (minted == null) {
                throw new IllegalStateException(
                    "withFreshCounters() returned null for reader ["
                        + reader.getClass().getName()
                        + "]; implementations must return a non-null copy"
                );
            }
            return minted;
        }
        return reader;
    }
}
