/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

/**
 * The 128-bit fingerprint of a resolved file SET, carried as two 64-bit Murmur3 lanes.
 * <p>
 * Computed by {@code FileSetFingerprints#compute} as a commutative fold over every file's
 * {@code (path, mtime, size)}, with the file count mixed into the final avalanche: the same set listed
 * in any order yields the same fingerprint, and any file added, removed, or modified yields a different
 * one. That makes fingerprint-derived cache keys correct-or-miss by construction — no invalidation
 * protocol.
 * <p>
 * 128 bits, not 64, because a collision serves one set's row count for a different set — a wrong answer,
 * not a slow path. A 64-bit hash birthday-collides around 2^32 distinct sets (reachable on a long-lived
 * coordinator); 128 bits (~2^64) makes it negligible. Non-cryptographic (Murmur3): guards accidental
 * staleness, not an adversary crafting a collision.
 * <p>
 * Lives in {@code datasources} (not the {@code glob} impl package) because it is the return type of the
 * {@code spi.FileList} SPI method, alongside its sibling value type {@code PartitionMetadata}.
 */
public record FileSetFingerprint(long high, long low) {}
