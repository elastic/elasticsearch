/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

/**
 * The on-disk wire format version stamped into every ColumNAR segment file.
 *
 * <h2>Read-side vs write-side usage</h2>
 * These two sides have different responsibilities and must not be confused.
 *
 * <p><b>Read side.</b> {@link org.elasticsearch.columnar.substrate.ColumnarCodecUtil#checkHeader}
 * rejects files outside {@code [MIN_SUPPORTED, CURRENT]} with a Lucene-typed exception before any
 * data is read. The returned {@link FormatVersion} is then passed into every {@code readFrom}
 * method, which compares against a {@code VERSION_*} constant to read fields added in a later
 * layout version:
 * <pre>{@code
 * if (formatVersion.version() >= FormatVersion.VERSION_V1_EXTRA_FLAGS) {
 *     flags = in.readVInt();
 * }
 * }</pre>
 *
 * <p><b>Write side.</b> The header version alone does not prove that the payload is valid for that
 * version: a writer could stamp BASELINE while emitting next-version ids, producing a corrupt
 * segment that passes the header check and fails mid-decode. Pipeline factories that require a
 * minimum version compare {@link ColumnarWriteProfile#version()} against a {@code VERSION_*}
 * constant at build time, not at header write time.
 *
 * <h2>When to bump</h2>
 * Bump {@link #CURRENT} whenever the set of frozen component ids that a writer may emit grows.
 * Adding any encoder, field type, or block-compression codec requires a bump; the new version is
 * the gate that turns "unknown id mid-decode" into
 * {@link org.apache.lucene.index.IndexFormatTooNewException} at segment open, before any data
 * is read. A metadata layout change that does not add frozen ids still requires a format bump,
 * but may only need read-side branching in the affected {@code readFrom} methods rather than new
 * id registrations.
 *
 * <h2>How to introduce the next format version</h2>
 * <ol>
 *   <li>Declare {@code public static final int VERSION_X = N} and a matching
 *       {@code public static final FormatVersion DESCRIPTIVE_NAME = new FormatVersion(N)}.
 *   <li>Update {@link #CURRENT} to point at the new constant.
 *   <li>Branch on {@code formatVersion.version() >= VERSION_X} in the affected
 *       {@code readFrom} methods.
 *   <li>In pipeline factories that require the new version, check
 *       {@code profile.version().version() >= VERSION_X} at build time.
 *   <li>Register the new id in the relevant registry (field types, numeric transforms and terminals,
 *       block-byte codecs, skip-index codecs).
 *   <li>Add BASELINE fixtures to {@code ColumnarBwcFixtureTests} before the bump merges.
 * </ol>
 */
public record FormatVersion(int version) implements Comparable<FormatVersion> {

    /** First shipped format: numeric long/double columns, adaptive pipeline, multi-level skip index. */
    public static final FormatVersion BASELINE = new FormatVersion(0);

    /** Version written by the current code. */
    public static final FormatVersion CURRENT = BASELINE;

    /** Oldest version a current reader will accept. Advance this when old-format support is dropped. */
    public static final FormatVersion MIN_SUPPORTED = BASELINE;

    public FormatVersion {
        if (version < 0) {
            throw new IllegalArgumentException("format version must be non-negative, got: " + version);
        }
    }

    /**
     * Throws {@link IllegalArgumentException} if this version is outside the readable range
     * {@code [MIN_SUPPORTED, CURRENT]}. Used directly in unit tests; production code relies on
     * {@link org.elasticsearch.columnar.substrate.ColumnarCodecUtil#checkHeader}, which produces
     * the Lucene-typed exceptions with file context.
     */
    public void ensureReadable() {
        if (version < MIN_SUPPORTED.version) {
            throw new IllegalArgumentException(
                "format version " + version + " is older than the minimum supported " + MIN_SUPPORTED.version
            );
        }
        if (version > CURRENT.version) {
            throw new IllegalArgumentException("format version " + version + " is newer than the current " + CURRENT.version);
        }
    }

    /** Returns {@code true} if this version equals {@code other}. */
    public boolean matches(final FormatVersion other) {
        return this.equals(other);
    }

    @Override
    public int compareTo(final FormatVersion o) {
        return Integer.compare(version, o.version);
    }
}
