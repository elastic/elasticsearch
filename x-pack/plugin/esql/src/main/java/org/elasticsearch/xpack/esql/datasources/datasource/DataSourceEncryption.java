/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.cluster.metadata.DataSourceSetting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.crypto.EncryptedData;
import org.elasticsearch.xpack.core.crypto.EncryptionService;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Master-side encrypt seam for data-source secret values. Wraps the optional {@link EncryptionService}
 * binding and produces the on-wire {@code byte[]} blob shape (the binary {@code writeTo} output of
 * an {@link EncryptedData}) for each secret-flagged setting that arrives with a plaintext {@code String}.
 *
 * <p>When the wrapped service is {@code null} (security plugin disabled or PEK feature flag off),
 * {@link #isAvailable()} returns {@code false} and callers are expected to take the plaintext-fallback
 * path on the producer side. {@link #encrypt(Map)} is only safe to call when {@link #isAvailable()};
 * the method short-circuits with an {@link IllegalStateException} if invoked without a service to
 * encourage callers to gate.
 */
public final class DataSourceEncryption {

    @Nullable
    private final EncryptionService service;

    public DataSourceEncryption(@Nullable EncryptionService service) {
        this.service = service;
    }

    public boolean isAvailable() {
        return service != null;
    }

    /**
     * Encrypt every secret-flagged plaintext-String value in {@code settings}. Non-secret values pass
     * through unchanged. Already-encrypted carriers (cluster-state replay paths) also pass through.
     *
     * @throws IllegalStateException if no service is bound; callers must gate on {@link #isAvailable()}.
     */
    public Map<String, DataSourceSetting> encrypt(Map<String, DataSourceSetting> settings) {
        if (service == null) {
            throw new IllegalStateException("encrypt() called without a bound EncryptionService; gate on isAvailable()");
        }
        Map<String, DataSourceSetting> result = new HashMap<>(settings.size());
        for (var entry : settings.entrySet()) {
            DataSourceSetting setting = entry.getValue();
            if (setting.secret() == false) {
                result.put(entry.getKey(), setting);
                continue;
            }
            Object raw = setting.rawValue();
            if (raw == null) {
                result.put(entry.getKey(), setting);
                continue;
            }
            if (raw instanceof String plaintext) {
                byte[] bytes = plaintext.getBytes(StandardCharsets.UTF_8);
                try {
                    EncryptedData encrypted = service.encrypt(bytes);
                    BytesStreamOutput out = new BytesStreamOutput();
                    try {
                        encrypted.writeTo(out);
                    } catch (IOException e) {
                        throw new ElasticsearchStatusException(
                            "failed to serialize encrypted value for setting [" + entry.getKey() + "]",
                            RestStatus.INTERNAL_SERVER_ERROR,
                            e
                        );
                    }
                    result.put(entry.getKey(), new DataSourceSetting(BytesReference.toBytes(out.bytes()), true));
                } finally {
                    Arrays.fill(bytes, (byte) 0);
                }
            } else {
                // Already a ciphertext blob (constructor guarantees String | byte[] | null).
                result.put(entry.getKey(), setting);
            }
        }
        return result;
    }
}
