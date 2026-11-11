/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.Locale;

/**
 * How much per-stripe statistics a row-format external read should harvest while it scans, threaded to
 * the readers through {@link FormatReadContext#statsColumnScope()}. Orthogonal to the stripe grid size
 * ({@link FormatReadContext#statsStripeSize()}): the grid decides which stripe a record lands in, this
 * scope decides what is summarised per stripe.
 * <p>
 * Row count is harvested in every mode except {@link #NONE}. The min/max/null per-column summaries are
 * gated separately so that a {@code COUNT(*)} read (which projects zero columns) still records a
 * stripe's row count — the regression this enum was introduced to fix, where a zero-projection read
 * harvested nothing and a warm {@code COUNT(*)} re-scanned the whole file.
 * <p>
 * The four modes are strictly nested — each contains everything the previous does and adds one band:
 * {@code NONE} ⊂ {@code COUNT} (adds the row count) ⊂ {@code PROJECTED} (adds min/max/null for the
 * query's projected columns) ⊂ {@code ALL} (adds min/max/null for every remaining file column). So
 * {@code ALL}'s committed per-stripe column set is always a superset of what {@code PROJECTED} would
 * commit for the same scan. {@code ALL} pays to type-convert and compare every file column per row; it
 * is the opt-in broad mode whose payoff is serving a warm {@code MIN}/{@code MAX} of a column the cold
 * query never projected.
 * <p>
 * The constants are uppercase to satisfy {@code Setting.enumSetting}; {@link #toString()} renders the
 * lowercase token ({@code none}/{@code count}/{@code projected}/{@code all}) used as the setting value.
 */
public enum StripeColumnScope {

    /** Harvest no per-stripe statistics. A warm aggregate over this read always re-scans. */
    NONE,

    /** Harvest per-stripe row count only — no per-column min/max/null. Enough to serve a warm {@code COUNT(*)}. */
    COUNT,

    /** Harvest per-stripe row count plus min/max/null for the query's projected columns (the default). */
    PROJECTED,

    /** Harvest per-stripe row count plus min/max/null for every column in the file's schema. */
    ALL;

    /** Whether per-column min/max/null statistics are harvested in this mode (true for {@link #PROJECTED} and {@link #ALL}). */
    public boolean harvestsColumns() {
        return this == PROJECTED || this == ALL;
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
