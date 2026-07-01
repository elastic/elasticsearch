/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Decrypts secret values at the connector boundary. Created once per node by {@code EsqlPlugin} with the node's
 * {@link EncryptionService}; the lazy wrappers in {@code DataSourceModule} call {@link #decryptInPlace} before each connector use.
 *
 * <p>The per-node service will need to become a per-project lookup once the encryption service is project-aware.
 */
public final class DataSourceCredentials {

    private final EncryptionService encryptionService;

    public DataSourceCredentials(EncryptionService encryptionService) {
        this.encryptionService = Objects.requireNonNull(encryptionService, "encryptionService");
    }

    public Map<String, Object> decryptInPlace(Map<String, Object> config) {
        if (config == null) {
            return null;
        }
        Map<String, Object> result = new HashMap<>(config.size());
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof EncryptedData encrypted) {
                result.put(entry.getKey(), decryptValue(encrypted, encryptionService));
            } else {
                result.put(entry.getKey(), value);
            }
        }
        return result;
    }

    /** Decrypt the carrier and deserialize the plaintext back to the original value type. */
    private static Object decryptValue(EncryptedData encrypted, EncryptionService service) {
        byte[] plaintext = service.decrypt(encrypted);
        try (StreamInput in = new BytesArray(plaintext).streamInput()) {
            return in.readGenericValue();
        } catch (IOException e) {
            throw new ElasticsearchStatusException(
                "cannot decrypt secret data-source setting: decrypted payload is malformed",
                RestStatus.INTERNAL_SERVER_ERROR,
                e
            );
        } finally {
            Arrays.fill(plaintext, (byte) 0);
        }
    }
}
