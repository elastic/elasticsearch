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
 * <p>{@link org.elasticsearch.columnar.substrate.ColumnarCodecUtil#checkHeader} rejects files
 * outside {@code [BASELINE, CURRENT]} with a Lucene-typed exception before any data is read.
 * The returned {@link FormatVersion} is then passed into every {@code readFrom} method, which
 * branches on a named constant to read fields added in a later layout version:
 * <pre>{@code
 * if (formatVersion.onOrAfter(FormatVersion.V1_EXTRA_FLAGS)) {
 *     flags = in.readVInt();
 * }
 * }</pre>
 *
 * <h2>When to bump</h2>
 * Bump {@link #CURRENT} on layout changes: a new field in {@code readFrom}, different block
 * framing, a different offset-table encoding. Those parse silently and return wrong values on an
 * old reader; a header bump turns that into
 * {@link org.apache.lucene.index.IndexFormatTooNewException} at segment open. Id additions do not
 * require a bump: an unknown id already throws {@code IllegalArgumentException} at first field
 * access, which is loud enough.
 *
 * <h2>How to introduce the next format version</h2>
 * <ol>
 *   <li>Declare {@code public static final FormatVersion DESCRIPTIVE_NAME = new FormatVersion(N)}.
 *   <li>Update {@link #CURRENT} to point at the new constant.
 *   <li>Branch on {@code formatVersion.onOrAfter(DESCRIPTIVE_NAME)} in the affected
 *       {@code readFrom} methods.
 *   <li>Register the new id in the relevant registry (field types, numeric transforms and terminals,
 *       block-byte codecs, skip-index codecs).
 *   <li>Add BASELINE fixtures to a BWC fixture test class before the bump merges.
 * </ol>
 */
public record FormatVersion(int version) {

    /** First shipped format: numeric long/double columns, adaptive pipeline, multi-level skip index. */
    public static final FormatVersion BASELINE = new FormatVersion(0);

    /** Equals {@link #BASELINE} until the first format bump; update this when a new layout version ships. */
    public static final FormatVersion CURRENT = BASELINE;

    public FormatVersion {
        if (version < 0) {
            throw new IllegalArgumentException("format version must be non-negative, got: " + version);
        }
    }

    /**
     * Returns {@code true} if this version is greater than or equal to {@code other}.
     */
    public boolean onOrAfter(final FormatVersion other) {
        return this.version >= other.version;
    }
}
