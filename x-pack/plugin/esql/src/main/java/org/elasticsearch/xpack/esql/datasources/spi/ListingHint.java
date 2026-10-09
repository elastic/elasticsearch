/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

/**
 * Metadata about a file already known from a directory listing, passed to
 * {@link ExternalSourceFactory#resolveMetadataAsync} so the factory can build the storage object
 * without a synchronous existence/HEAD probe before the async metadata read. A {@code null} hint
 * means nothing is known (e.g. a single, explicitly-referenced path) and the factory is responsible
 * for verifying existence itself.
 * <p>
 * New listing-derived fields (etag/version-id, checksum, storage class, ...) should be added here
 * rather than as extra parameters, so the async-resolution signature and its overrides stay stable.
 * For the same reason the planner's per-file schema sample share rides here too, although the listing does
 * not report it: this is the one per-file channel from the planner to the metadata read.
 *
 * @param length the object's length in bytes
 * @param lastModifiedMillis the object's last-modified time, in epoch milliseconds
 * @param schemaSampleShare how many files of the listing share one schema sample budget with this one, see
 *                          {@link FormatReader#withSchemaSampleShare}; {@code 1} when this file gets the whole sample
 */
public record ListingHint(long length, long lastModifiedMillis, int schemaSampleShare) {

    public ListingHint {
        if (schemaSampleShare < 1) {
            throw new IllegalArgumentException("schemaSampleShare must be at least 1, got [" + schemaSampleShare + "]");
        }
    }

    public ListingHint(long length, long lastModifiedMillis) {
        this(length, lastModifiedMillis, 1);
    }
}
