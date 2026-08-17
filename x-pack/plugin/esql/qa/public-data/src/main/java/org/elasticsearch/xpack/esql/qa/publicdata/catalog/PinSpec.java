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
 */
public record PinSpec(String method, Instant verifiedAt, long objectCount, long totalBytes, List<PinnedObject> samples) {

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
}
