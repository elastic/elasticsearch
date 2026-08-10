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
 * Describes the format version used for newly written segments.
 *
 * <p>Segments are immutable; readers accept any version in {@code [MIN_SUPPORTED, CURRENT]}.
 * Writers only need to decide what version new segments may use. Server-side wiring will derive a
 * profile from the index's creation {@code IndexVersion} and inject it into
 * {@link ColumNARDocValuesFormat} at construction time, keeping the library free of server
 * dependencies while ensuring merges and new segments are not stamped with a format version newer
 * than what the oldest node in the cluster can read.
 *
 * <p>The {@link #version} must be in {@code [FormatVersion.MIN_SUPPORTED, FormatVersion.CURRENT]}.
 *
 * <p><b>Enforcement boundary.</b> {@link org.elasticsearch.columnar.numeric.NumericPipelineSelector}
 * routes by field semantics only; it does not know the write profile. Pipeline factories that
 * require a minimum version compare {@link #version()} against a {@code VERSION_*} constant at
 * build time, turning a misconfiguration into an early {@link IllegalArgumentException} rather
 * than a corrupt segment that passes the header check and fails mid-decode on an old reader.
 *
 * <p><b>Merge contract.</b> Merges re-encode through the current writer pipeline and stamp the
 * output segment with this profile's version. Therefore pipeline construction must validate against
 * the profile version; otherwise a merge could write next-version ids into a BASELINE-stamped
 * segment.
 */
public record ColumnarWriteProfile(FormatVersion version) {

    public ColumnarWriteProfile {
        if (version.compareTo(FormatVersion.CURRENT) > 0) {
            throw new IllegalArgumentException(
                "write profile version " + version.version() + " exceeds current " + FormatVersion.CURRENT.version()
            );
        }
        if (version.compareTo(FormatVersion.MIN_SUPPORTED) < 0) {
            throw new IllegalArgumentException(
                "write profile version " + version.version() + " is below the minimum supported " + FormatVersion.MIN_SUPPORTED.version()
            );
        }
    }

    /** Returns a profile at the current code's format version. */
    public static ColumnarWriteProfile current() {
        return new ColumnarWriteProfile(FormatVersion.CURRENT);
    }

}
