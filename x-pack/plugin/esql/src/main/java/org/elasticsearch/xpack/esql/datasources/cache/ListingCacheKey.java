/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.common.hash.MurmurHash3;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Cache key for file listing results. Includes a 128-bit Murmur3 hash of credential
 * values (NOT the credential keys) for isolation between users with different credentials.
 * Endpoint and region are included because the same bucket on different endpoints
 * contains different objects.
 */
public record ListingCacheKey(
    String scheme,
    String bucketOrContainer,
    String prefixAndGlob,
    String endpoint,
    String region,
    long credentialHashH1,
    long credentialHashH2
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

    public static ListingCacheKey build(String scheme, String bucket, String prefixAndGlob, Map<String, Object> config) {
        String endpoint = config != null ? String.valueOf(config.getOrDefault("endpoint", "")) : "";
        String region = config != null ? String.valueOf(config.getOrDefault("region", "")) : "";
        long[] hash = computeCredentialHash(config);
        return new ListingCacheKey(scheme, bucket, prefixAndGlob, endpoint, region, hash[0], hash[1]);
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
