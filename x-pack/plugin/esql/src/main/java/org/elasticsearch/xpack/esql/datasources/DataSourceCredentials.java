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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Decrypts secret values at the connector boundary. Created once per node by {@code EsqlPlugin}, with the
 * {@link EncryptionService} pushed in from {@code TransportPutDataSourceAction}'s ctor; the lazy wrappers
 * in {@code DataSourceModule} call {@link #decryptInPlace} before each connector use.
 *
 * <p>The data-source feature is coupled to the project-encryption-key feature, so the service is always
 * bound when a data source is served; the {@code 503} below guards the otherwise-impossible unbound case.
 *
 * <p>The per-node service slot will need to become a per-project lookup once the encryption service is
 * project-aware.
 */
public final class DataSourceCredentials {

    @Nullable
    private volatile EncryptionService encryptionService;

    public void setEncryptionService(@Nullable EncryptionService encryptionService) {
        this.encryptionService = encryptionService;
    }

    public Map<String, Object> decryptInPlace(Map<String, Object> config) {
        if (config == null) {
            return null;
        }
        EncryptionService service = this.encryptionService;
        Map<String, Object> result = new HashMap<>(config.size());
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof EncryptedData encrypted) {
                if (service == null) {
                    throw new ElasticsearchStatusException(
                        "cannot decrypt secret data-source settings: encryption service is not bound on this node",
                        RestStatus.SERVICE_UNAVAILABLE
                    );
                }
                result.put(entry.getKey(), decryptValue(encrypted, service));
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
