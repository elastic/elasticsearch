/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.util.ByteUtils;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Cache key for file listing results. A listing is what a principal can <i>see</i>, so it is isolated by
 * credential — unlike the schema and file-metadata keys, which describe what a file contains and deliberately
 * share across principals. The storage identity is included because the same bucket on different endpoints
 * contains different objects.
 *
 * <p>Both identities are supplied by the storage provider that would list the prefix, never derived here. This key
 * used to hold a hash it computed itself from seven credential names written beside it — which carried
 * {@code access_key} and {@code secret_key} and not {@code session_token}, {@code role_arn} or {@code auth}, so two
 * roles over one bucket addressed one listing, and that hash was the only thing between them on any path. A
 * provider knows which of its own fields are declared secret; a cache cannot.
 *
 * <p>The {@code listingDiscriminatorH1/H2} are a 128-bit hash of everything about the query that changes which
 * files the listing contains — the filter hints that narrow it, and the resolved partition config (strategy and
 * path template) that decides whether they can and how partition columns are derived. Without it the key would
 * not determine its value: a filtered query would cache a narrowed
 * listing under the key an unfiltered query then hits, silently serving it fewer files than the dataset holds.
 * The discriminator string is produced by {@code GlobExpander.listingCacheDiscriminator} (from the same code the
 * listing itself goes through) and hashed here rather than carried: a filter's {@code IN}-list can be arbitrarily
 * large, and the cache weighs only the value, so the key must not grow with the query.
 *
 * <p>The discriminator uses a <b>collision-resistant</b> digest (SHA-256, truncated to 128 bits). A collision here
 * would serve one filter's narrowed listing to a different filter — a wrong answer — and its pre-image is built
 * from the query's own filter literals, so a non-cryptographic hash would let a caller
 * <i>construct</i> a collision. With SHA-256 truncated to 128 bits, two distinct discriminators collide with
 * probability 2<sup>-128</sup>, and a cache would need ~2<sup>64</sup> co-resident entries to reach even-odds
 * birthday risk — negligible, and below the rate of undetected hardware bit-errors every result already passes
 * through.
 */
public record ListingCacheKey(
    String scheme,
    String bucketOrContainer,
    String prefixAndGlob,
    String storageIdentity,
    String secretIdentity,
    long listingDiscriminatorH1,
    long listingDiscriminatorH2,
    String definitionVersion
) {
    /**
     * @param storageIdentity what the storage provider that would list this prefix says identifies the objects it
     *                        reads. Passed in rather than read out of {@code config}: only that provider knows
     *                        which of its settings name the same store twice, and this key used to guess with two
     *                        literals that named nothing for a provider addressed by an account.
     * @param secretIdentity  a digest of the declared-secret settings that provider consumed, from
     *                        {@code Configured.secretIdentityOf}. Empty when it consumed none, which is a correct
     *                        answer and not a missing one — an anonymous store has no credential to isolate by.
     */
    public static ListingCacheKey build(
        String scheme,
        String bucket,
        String prefixAndGlob,
        String storageIdentity,
        String secretIdentity,
        Map<String, Object> config,
        String listingDiscriminator
    ) {
        long[] discriminatorHash = sha256Truncated(listingDiscriminator);
        return new ListingCacheKey(
            scheme,
            bucket,
            prefixAndGlob,
            storageIdentity,
            secretIdentity,
            discriminatorHash[0],
            discriminatorHash[1],
            SchemaCacheKey.definitionVersionOf(config)
        );
    }

    /**
     * SHA-256 of the discriminator, truncated to its first 128 bits. Collision-resistant because the pre-image is
     * built from user-supplied filter literals (see the class javadoc). An empty/absent discriminator maps to zero,
     * which no real discriminator reaches (it always begins with the length-prefixed partition strategy name).
     */
    static long[] sha256Truncated(String value) {
        if (value == null || value.isEmpty()) {
            return new long[] { 0L, 0L };
        }
        byte[] digest = MessageDigests.sha256().digest(value.getBytes(StandardCharsets.UTF_8));
        return new long[] { ByteUtils.readLongBE(digest, 0), ByteUtils.readLongBE(digest, 8) };
    }

}
