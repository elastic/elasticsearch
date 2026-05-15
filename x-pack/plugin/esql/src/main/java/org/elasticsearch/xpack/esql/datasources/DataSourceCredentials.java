/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.io.stream.GenericNamedWriteable;
import org.elasticsearch.xpack.core.crypto.EncryptedData;
import org.elasticsearch.xpack.core.crypto.EncryptionService;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Bridges the Guice-bound {@link EncryptionService} from the security plugin to the data-node-side
 * connector decryption seam.
 *
 * <p>The {@link #initialize(EncryptionService)} setter is called by Guice-injected transport actions
 * at plugin construction time (typically the data-source CRUD actions, which are instantiated on every
 * node when the action graph is wired). The data-node side then calls {@link #decryptInPlace(Map)}
 * at the catalog-invocation seam to materialize plaintext String values from {@link EncryptedData}
 * carriers before the connector consumes the config map.
 *
 * <p>The decrypt step targets Phase 1's "encrypt at rest, in transit, in propagation" goal while
 * preserving today's connector contract ({@code value.toString()} on the config map yields a usable
 * String). Phase 2 (post-GA, tracked separately) reshapes the connector SPI so the framework owns
 * the {@code SecureString} lifecycle structurally; until then, the data-node-side decrypt window is
 * documented as the residual plaintext-on-heap surface.
 */
public final class DataSourceCredentials {

    /**
     * Holder for the {@link EncryptionService} bound by the security plugin. Volatile rather than
     * {@code SetOnce} so that test harnesses constructing multiple plugin lifecycles in one JVM can
     * re-initialize without failing — production has exactly one plugin lifecycle per process.
     */
    private static volatile EncryptionService encryptionService;

    private DataSourceCredentials() {}

    /** Wire the security-plugin-bound {@link EncryptionService} for use at the decrypt seam. */
    public static void initialize(EncryptionService service) {
        encryptionService = service;
    }

    /**
     * Returns a new config map with any encrypted-carrier values replaced by their plaintext String
     * form. Non-encrypted values pass through unchanged.
     *
     * <p>Carriers handled today:
     * <ul>
     *   <li>{@link EncryptedData} — decrypt via {@link EncryptionService#decrypt} and UTF-8-decode.</li>
     *   <li>{@code Map<String, Object>} matching the {@code EncryptedData} XContent shape
     *       ({@code key_id} + {@code data}) — reconstruct an {@link EncryptedData}, then decrypt.
     *       Used for entries parsed from on-disk gateway XContent before the new
     *       {@link GenericNamedWriteable} dispatch existed.</li>
     *   <li>Anything else (String, Integer, Boolean, etc.) — pass through.</li>
     * </ul>
     *
     * <p>If {@link #initialize(EncryptionService)} has not been called, the function is a pass-through
     * — appropriate when the security plugin isn't loaded or the encryption feature hasn't fired.
     */
    public static Map<String, Object> decryptInPlace(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return config;
        }
        EncryptionService service = encryptionService;
        if (service == null) {
            return config;
        }
        Map<String, Object> result = new HashMap<>(config.size());
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            result.put(entry.getKey(), decryptOne(entry.getValue(), service));
        }
        return result;
    }

    private static Object decryptOne(Object value, EncryptionService service) {
        if (value instanceof EncryptedData encrypted) {
            return decryptToString(encrypted, service);
        }
        if (value instanceof Map<?, ?> rawMap && rawMap.get("key_id") instanceof String keyId) {
            // XContent-parsed encrypted-data shape; the "data" field is the base64-decoded byte[].
            Object dataValue = rawMap.get("data");
            byte[] payload = coerceToBytes(dataValue);
            if (payload != null) {
                return decryptToString(new EncryptedData(keyId, payload), service);
            }
        }
        return value;
    }

    private static String decryptToString(EncryptedData encrypted, EncryptionService service) {
        byte[] plaintext = service.decrypt(encrypted);
        try {
            return new String(plaintext, StandardCharsets.UTF_8);
        } finally {
            // Best-effort wipe of the intermediate byte[]; the returned Java String remains on heap
            // until GC — out-of-scope-here irreducible window. Tracked under the post-GA Phase 2 work.
            Arrays.fill(plaintext, (byte) 0);
        }
    }

    private static byte[] coerceToBytes(Object value) {
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof String s) {
            // Already-base64-decoded String form (not expected from XContentParser.binaryValue but defensive)
            return s.getBytes(StandardCharsets.UTF_8);
        }
        return null;
    }
}
