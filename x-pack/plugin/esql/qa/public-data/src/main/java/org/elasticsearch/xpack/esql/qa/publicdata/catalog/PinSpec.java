/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import java.time.Instant;
import java.util.List;

/**
 * Metadata pin of a variant's remote objects, captured via HTTP {@code HEAD} or S3
 * {@code ListObjectsV2} only — never by fetching object bodies. The scheduled pipeline's pre-step
 * re-verifies pins so upstream drift surfaces as attributed maintenance ({@code PIN_DRIFT}), never
 * as a fake product regression.
 *
 * @param method      {@code HEAD} (single object) or {@code LIST} (prefix/glob)
 * @param verifiedAt  when the pin was last verified against live metadata
 * @param objectCount number of objects covered by the pin
 * @param totalBytes  sum of the covered objects' sizes
 * @param samples     pinned per-object identities; the full set for small layouts, a stable sample
 *                    (first/last keys) for large ones — enough to detect any re-publish
 * @param isVolatile  the publisher re-publishes these objects on a schedule, so ETag and
 *                    Last-Modified drift is expected and is NOT drift worth a human. Only existence
 *                    and an approximate size are checked. Set this only where the volatility is
 *                    documented upstream — the validator demands a {@code notes:} justification —
 *                    and pair it with an invariant-asserted workload, because no frozen expected
 *                    table can survive bytes that move nightly
 * @param sizeTolerancePercent how far a volatile object's size may move before it counts as drift
 */
public record PinSpec(
    String method,
    Instant verifiedAt,
    long objectCount,
    long totalBytes,
    List<PinnedObject> samples,
    boolean isVolatile,
    int sizeTolerancePercent
) {

    /** Default tolerance for a volatile pin: a daily re-publish of the same content barely moves. */
    public static final int DEFAULT_SIZE_TOLERANCE_PERCENT = 10;

    /** A single pinned object identity. {@code etag} may be null where the store returns none. */
    public record PinnedObject(String key, String etag, long sizeBytes) {}

    /**
     * A pin that could not possibly have been verified is degenerate and rejected. Zero total
     * bytes is legitimate — the dirty-data corpus pins a real zero-byte object — but zero objects
     * or no samples means nothing was looked at.
     */
    public boolean degenerate() {
        return objectCount <= 0 || totalBytes < 0 || samples == null || samples.isEmpty() || verifiedAt == null;
    }

    /** Whether {@code liveBytes} is within this pin's tolerance of {@code pinnedBytes}. */
    public boolean sizeWithinTolerance(long pinnedBytes, long liveBytes) {
        long allowed = Math.max(1L, Math.abs(pinnedBytes) / 100L * sizeTolerancePercent);
        return Math.abs(liveBytes - pinnedBytes) <= allowed;
    }
}
