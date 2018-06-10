/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.support;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

public class HasherFactory {
    private static final Pattern BCRYPTNAME_MATCHER = Pattern.compile("bcrypt\\d{1,2}");
    private static final int DEFAULT_BCRYPT_COST = 10;
    private static final int DEFAULT_PBKDF2_COST = 10000;

    private HasherFactory() {
        throw new IllegalStateException("Utility class should not be instantiated");
    }

    public static Hasher getHasher(String algorithm, int cost) {
        String algorithmId = algorithm.toLowerCase(Locale.ROOT);
        if ("bcrypt".equals(algorithmId)) {
            return new Hasher.BcryptHasher(cost == -1 ? DEFAULT_BCRYPT_COST : cost);
        } else if (BCRYPTNAME_MATCHER.matcher(algorithmId).matches()) {
            // Explicitly set cost takes precedence over the bcryptX cost
            int implicitCost = Integer.parseInt(algorithmId.replace("bcrypt", ""));
            return new Hasher.BcryptHasher(cost == -1 ? implicitCost : cost);
        } else if ("pbkdf2".equals(algorithmId)) {
            return new Hasher.PBKDF2Hasher(cost == -1 ? DEFAULT_PBKDF2_COST : cost);
        } else if ("ssha256".equals(algorithmId)) {
            return new Hasher.SSHA256Hasher();
        } else if ("sha1".equals(algorithmId)) {
            return new Hasher.SHA1Hasher();
        } else if ("md5".equals(algorithmId)) {
            return new Hasher.MD5Hasher();
        } else if ("noop".equals(algorithmId) || "clear_text".equals(algorithmId)) {
            return new Hasher.NoopHasher();
        } else {
            throw new IllegalArgumentException("Invalid hashing algorithm identifer: " + algorithm);
        }
    }

    public static Hasher getHasher(String algorithmId) {
        algorithmId = algorithmId.toLowerCase(Locale.ROOT);
        if ("bcrypt".equals(algorithmId)) {
            return getHasher("bcrypt", 10);
        } else if (BCRYPTNAME_MATCHER.matcher(algorithmId).matches()) {
            int cost = Integer.parseInt(algorithmId.replace("bcrypt", ""));
            return getHasher("bcrypt", cost);
        } else if ("pbkdf2".equals(algorithmId)) {
            return getHasher(algorithmId, 10000);
        } else {
            return getHasher(algorithmId, 0);
        }
    }

    public static List<String> getAvailableAlgorithms() {
        return Arrays.asList("bcrypt", "ssha256", "md5", "sha1", "pbkdf2", "noop");
    }
}

