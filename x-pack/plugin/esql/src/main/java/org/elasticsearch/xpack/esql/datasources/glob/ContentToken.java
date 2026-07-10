/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

/**
 * The 128-bit content fingerprint of a resolved file set, carried as two 64-bit Murmur3 lanes.
 * <p>
 * Computed by {@link FileListContentToken#compute} as a commutative fold over every file's
 * {@code (path, mtime, size)}: the same set listed in any order yields the same token, and any file
 * added, removed, or modified yields a different one. That makes token-derived cache keys
 * correct-or-miss by construction — no invalidation protocol. It is an identity for cache keying,
 * not a cryptographic commitment: a 128-bit non-adversarial collision is negligible.
 */
public record ContentToken(long high, long low) {}
