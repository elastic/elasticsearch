/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.fixtures;

import java.util.Set;

/**
 * The (dimension, value, format) cells some generator renders AND some suite can select.
 *
 * <p>One source for both consumers: the audit asks whether a cell is reachable, and the selection asks
 * whether a vector can run. Two lists would agree until they did not, and the disagreement would decide
 * silently which vectors execute -- the failure this contract exists to prevent, and one already made
 * twice here with per-format defaults.
 *
 * <p>Deliberately sparse, and a row is added by the same change that implements the rendering. A
 * {@code read_key} does not earn a row: it says how a value would announce itself, not that anything
 * writes the bytes.
 */
public final class FixtureCapabilities {

    private FixtureCapabilities() {}

    /**
     * Fixture cells that both render and are selectable today.
     *
     * <p>The three format rows record that a vector suite exists for that format and reads its tree.
     * csv needs no row: it is the contract's default format, so a csv vector never carries the format
     * slot off its default. ORC has no row because no ORC vector suite consumes its tree -- its
     * fixtures are generated in full, which is exactly why the absence has to be declared rather than
     * inferred from an empty directory.
     */
    private static final Set<String> RENDERED_AND_SELECTABLE = Set.of("format=tsv@tsv", "format=ndjson@ndjson", "format=parquet@parquet");

    /** Whether a generator writes this cell's bytes and a suite can select them. */
    public static boolean renders(String dimension, String value, String format) {
        return RENDERED_AND_SELECTABLE.contains(dimension + "=" + value + "@" + format);
    }

    /**
     * Whether any suite consumes this format's vectors at all.
     *
     * <p>Separate from {@link #renders} because the format axis selects WHICH suite runs rather than
     * varying a setting within one. A format nothing consumes yields no vectors however well its
     * fixtures are generated.
     */
    public static boolean formatIsConsumed(FixtureDimensions dimensions, String format) {
        return format.equals(dimensions.defaultValue("format")) || renders("format", format, format);
    }

    /** Every declared cell, for the audit's report and for tests that pin the set. */
    public static Set<String> cells() {
        return RENDERED_AND_SELECTABLE;
    }
}
