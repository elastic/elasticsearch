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
    private static final Set<String> RENDERED_AND_SELECTABLE = Set.of(
        "format=tsv@tsv",
        "format=ndjson@ndjson",
        "format=parquet@parquet",
        // Compressed variants of every text fixture are written at fixture-load time by
        // AbstractExternalSourceSpecTestCase.generateCompressedFixtures, so these bytes already exist for
        // all three text formats -- no generator work earned these rows, only the consumption that reads
        // them. lz4 and snappy are absent because COMPRESSED_EXTENSIONS does not produce them yet.
        "text_codec=gzip@csv",
        "text_codec=gzip@tsv",
        "text_codec=gzip@ndjson",
        "text_codec=zstd@csv",
        "text_codec=zstd@tsv",
        "text_codec=zstd@ndjson",
        "text_codec=bzip2@csv",
        "text_codec=bzip2@tsv",
        "text_codec=bzip2@ndjson",
        // Dialect trees rendered by <Format>FixtureGenerator --vector-variants into vector/<slug>/, and
        // selected by AbstractExternalSourceSpecTestCase.fixturesBase. Both text generators now have a
        // vector mode and both are wired in esql-datasource-csv/qa, which hosts the Csv and Tsv suites
        // alike. The rows differ per format because the DEFAULT differs: csv defaults to quoted, so its
        // off-default dialects are escaped and plain; tsv defaults to plain, so its off-default dialects
        // are quoted and escaped. A row for a format's own default would claim a cell that no vector
        // carries, since a vector never pins a slot at its default.
        "text_mode=escaped@csv",
        "text_mode=plain@csv",
        "mv_syntax=brackets@csv",
        "text_mode=quoted@tsv",
        "text_mode=escaped@tsv",
        "mv_syntax=brackets@tsv",
        // Codec-specific parquet trees written by compressed-parquet-fixtures.gradle into
        // standalone-<codec>/. These bytes predate the vector regime -- ParquetCompressedFormatSpecIT has
        // read them all along -- so no generator work earned these rows either, only the selection added
        // to vectorFixturesBase. lz4_legacy has no row because no generator writes that codec anywhere,
        // which is a different absence and stays declared as a gap.
        "parquet_codec=snappy@parquet",
        "parquet_codec=gzip@parquet",
        "parquet_codec=zstd@parquet",
        "parquet_codec=lz4_raw@parquet"
    );

    /**
     * Resolver-bound values a suite can actually ask for.
     *
     * <p>Here rather than in the audit or the selection, because both must answer this the same way and
     * they have now disagreed twice -- once on fixture cells, once on these. One source is the only
     * arrangement where they cannot drift.
     *
     * <p>{@code glob} is served: a standalone template resolves to a prefix pattern instead of a name,
     * which reaches the file through the resolver's LISTING path rather than a direct get. {@code
     * comma_list} is not, and its rule says why: one element is indistinguishable from exact.
     */
    private static final Set<String> RESOLVER_SERVED = Set.of("path_shape=glob");

    /** Whether a suite can ask for this resolver-bound value. */
    public static boolean resolverServes(String dimension, String value) {
        return RESOLVER_SERVED.contains(dimension + "=" + value);
    }

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
