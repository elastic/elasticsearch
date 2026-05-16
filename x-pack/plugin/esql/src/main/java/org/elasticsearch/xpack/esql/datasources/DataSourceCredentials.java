/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.core.crypto.EncryptedData;
import org.elasticsearch.xpack.core.crypto.EncryptionService;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Decrypts secret values at the catalog-invocation seam. {@link #initialize} is called once by the
 * Guice-injected put-data-source transport action; {@link #decryptInPlace} runs on every connector
 * call to replace {@link EncryptedData} carriers with plaintext just before the connector consumes them.
 */
public final class DataSourceCredentials {

    private static volatile EncryptionService encryptionService;

    private DataSourceCredentials() {}

    public static void initialize(EncryptionService service) {
        encryptionService = service;
    }

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
            Object value = entry.getValue();
            if (value instanceof EncryptedData encrypted) {
                result.put(entry.getKey(), decryptToString(encrypted, service));
            } else {
                result.put(entry.getKey(), value);
            }
        }
        return result;
    }

    private static String decryptToString(EncryptedData encrypted, EncryptionService service) {
        byte[] plaintext = service.decrypt(encrypted);
        try {
            return new String(plaintext, StandardCharsets.UTF_8);
        } finally {
            Arrays.fill(plaintext, (byte) 0);
        }
    }
}
