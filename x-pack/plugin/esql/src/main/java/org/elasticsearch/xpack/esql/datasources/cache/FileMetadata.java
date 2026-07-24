/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

/**
 * A single object's cheap physical metadata: byte {@code length} and last-modified epoch millis.
 * mtime is stored purely as the version token that rebuilds the {@link SchemaCacheKey} and populates
 * the resolved {@code StorageEntry}; staleness is bounded by the file-metadata cache's hard TTL alone,
 * never by mtime acting as a second freshness clock. {@code length} is cached alongside because the
 * warm single-file resolve rebuilds its singleton file list from both, so caching mtime without length
 * would still force the per-query object probe.
 * <p>
 * Consequence of caching the version token: a file overwritten in place is not observed until its
 * {@link FileMetadataCacheKey} entry expires (the schema TTL, 5m default) — a warm hit serves the
 * prior length/mtime and its schema/stats. Bounded staleness traded for the removed per-query probe.
 */
public record FileMetadata(long length, long mtimeMillis) {}
