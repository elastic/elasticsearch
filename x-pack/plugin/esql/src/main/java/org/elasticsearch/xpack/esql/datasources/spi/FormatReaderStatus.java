/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.xcontent.ToXContentFragment;

/**
 * Typed per-format reader counter snapshot folded into the {@code format_reader} field of the
 * external-source operator status. Each format module ({@code parquet}, {@code orc},
 * {@code ndjson}, {@code csv}) contributes one concrete implementation and registers it as a
 * {@link NamedWriteable} from its data-source plugin, so the coordinating node can deserialize
 * whichever format produced the profile.
 * <p>
 * Modeled as a {@link NamedWriteable} rather than a generic {@code Map} so the fields stay
 * machine-readable across versions — the operator already scrapes {@link #readNanos()} to roll
 * format-reader time up into the operator status, and a {@code Map} on the wire is a one-way door
 * (converting it to a typed shape after release is a wire-compatibility break). The
 * {@code esql_external_source_profile} transport version that gates this payload is unreleased,
 * so the typed shape ships from the first release that carries it.
 * <p>
 * The three accessors are the fields every format shares; format-specific counters live on each
 * implementation and surface through its {@link ToXContentFragment#toXContent} body.
 */
public interface FormatReaderStatus extends NamedWriteable, ToXContentFragment {

    /** Format identifier as it appears in the profile, e.g. {@code "parquet"}, {@code "tsv"}. */
    String format();

    /** Rows the reader emitted into the operator. */
    long rowsEmitted();

    /** Cumulative producer-thread time spent inside the format reader (open, decode, decompress). */
    long readNanos();
}
