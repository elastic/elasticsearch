/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.util.ByteUtils;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Cache key for file listing results. Includes a 128-bit Murmur3 hash of credential
 * values (NOT the credential keys) for isolation between users with different credentials.
 * Endpoint and region are included because the same bucket on different endpoints
 * contains different objects.
 *
 * <p>The {@code listingDiscriminatorH1/H2} are a 128-bit hash of everything about the query that changes which
 * files the listing contains — the filter hints that narrow it, and the hive-partitioning flag that decides
 * whether they can. Without it the key would not determine its value: a filtered query would cache a narrowed
 * listing under the key an unfiltered query then hits, silently serving it fewer files than the dataset holds.
 * The discriminator string is produced by {@code GlobExpander.listingCacheDiscriminator} (from the same code the
 * listing itself goes through) and hashed here for the same size reason as the credentials: a filter's
 * {@code IN}-list can be arbitrarily large, and the cache weighs only the value, so the key must not grow with the
 * query.
 *
 * <p>Unlike the credential hash, the discriminator uses a <b>collision-resistant</b> digest (SHA-256, truncated to
 * 128 bits). A collision here would serve one filter's narrowed listing to a different filter — a wrong answer. The
 * credential pre-image is secret, so its hash cannot be collided on purpose and Murmur3 suffices; the discriminator
 * pre-image is built from the query's own filter literals, so a non-cryptographic hash would let a caller
 * <i>construct</i> a collision. With SHA-256 truncated to 128 bits, two distinct discriminators collide with
 * probability 2<sup>-128</sup>, and a cache would need ~2<sup>64</sup> co-resident entries to reach even-odds
 * birthday risk — negligible, and below the rate of undetected hardware bit-errors every result already passes
 * through.
 */
public record ListingCacheKey(
    String scheme,
    String bucketOrContainer,
    String prefixAndGlob,
    String endpoint,
    String region,
    long credentialHashH1,
    long credentialHashH2,
    long listingDiscriminatorH1,
    long listingDiscriminatorH2
) {
    private static final Set<String> CREDENTIAL_PARAMS = Set.of(
        "access_key",
        "secret_key",
        "connection_string",
        "key",
        "sas_token",
        "credentials",
        "token"
    );

    public static ListingCacheKey build(
        String scheme,
        String bucket,
        String prefixAndGlob,
        Map<String, Object> config,
        String listingDiscriminator
    ) {
        EndpointRegion location = EndpointRegion.of(config);
        long[] hash = computeCredentialHash(config);
        long[] discriminatorHash = sha256Truncated(listingDiscriminator);
        return new ListingCacheKey(
            scheme,
            bucket,
            prefixAndGlob,
            location.endpoint(),
            location.region(),
            hash[0],
            hash[1],
            discriminatorHash[0],
            discriminatorHash[1]
        );
    }

    /**
     * SHA-256 of the discriminator, truncated to its first 128 bits. Collision-resistant because the pre-image is
     * built from user-supplied filter literals (see the class javadoc). An empty/absent discriminator maps to zero,
     * which no real discriminator reaches (it always begins with the hive-partitioning flag).
     */
    static long[] sha256Truncated(String value) {
        if (value == null || value.isEmpty()) {
            return new long[] { 0L, 0L };
        }
        byte[] digest = MessageDigests.sha256().digest(value.getBytes(StandardCharsets.UTF_8));
        return new long[] { ByteUtils.readLongBE(digest, 0), ByteUtils.readLongBE(digest, 8) };
    }

    static long[] computeCredentialHash(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return new long[] { 0L, 0L };
        }
        TreeMap<String, String> credentialValues = new TreeMap<>();
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            if (CREDENTIAL_PARAMS.contains(entry.getKey()) && entry.getValue() != null) {
                credentialValues.put(entry.getKey(), entry.getValue().toString());
            }
        }
        if (credentialValues.isEmpty()) {
            return new long[] { 0L, 0L };
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : credentialValues.entrySet()) {
            sb.append(entry.getKey()).append('=').append(entry.getValue()).append('\0');
        }
        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(bytes, 0, bytes.length, 0, new MurmurHash3.Hash128());
        return new long[] { hash.h1, hash.h2 };
    }
}
