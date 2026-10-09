/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.core.Nullable;

import java.util.Objects;

/**
 * Address of what ONE read measured about one file: the file's own address, plus the read configuration the
 * measurement was taken under.
 * <p>
 * Composed from {@link SchemaCacheKey} rather than copying its components, so the two cannot drift: a statistics
 * record is always about the same path, mtime, dataset and rail as the schema record beside it, and only the
 * read differs. It carries the declared-strict rail for free, because the file key already does.
 * <p>
 * <b>Why the read is part of the address.</b> A statistic measures the rows one read produced, and which rows a
 * read produces depends on the schema it was handed. Two reads of one file that resolved different schemas
 * measured different things and must not share an address. A schema record, by contrast, describes the file
 * itself and is the same answer whoever asks, so it keeps the address it has.
 * <p>
 * {@code readConfig} is never null and never empty. A rail that recorded no read configuration — columnar,
 * which harvests from footer metadata and is deliberately never stamped, or a text read of a file with no
 * schema to describe ({@link ReadConfigFingerprint#UNKNOWN}) — gets {@link #UNSTAMPED} instead, so every such
 * read of one file shares ONE address.
 * <p>
 * Sharing is correct for them rather than a compromise: two reads that recorded no configuration are
 * indistinguishable, so there is nothing an address could separate. It is also exactly what they did before
 * the schema and statistics stores were split, when every unstamped measurement landed on the one schema
 * record. Refusing them an address instead would silently drop a real measurement and take those rails cold.
 */
public record StatisticsKey(SchemaCacheKey file, String readConfig) {

    public StatisticsKey {
        Objects.requireNonNull(file, "a statistics address needs the file it is about");
        if (readConfig == null || readConfig.isEmpty()) {
            throw new IllegalArgumentException("a statistics address needs the read that produced it");
        }
    }

    /**
     * The one address shared by every read of a file that recorded no configuration. Not a valid fingerprint:
     * {@link ReadConfigFingerprint} renders exactly 32 hex characters or the empty string, so this can never
     * collide with a real one.
     */
    public static final String UNSTAMPED = "unstamped";

    /** The statistics address for {@code file} under {@code readConfig}, or the shared unstamped one. */
    public static StatisticsKey of(SchemaCacheKey file, @Nullable String readConfig) {
        return new StatisticsKey(file, readConfig == null || readConfig.isEmpty() ? UNSTAMPED : readConfig);
    }
}
