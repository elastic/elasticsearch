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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Decrypts secret values at the catalog-invocation point. The single per-node instance is created
 * by {@code EsqlPlugin.createComponents} and bound for Guice injection; {@code TransportPutDataSourceAction}
 * binds an optional {@link EncryptionService} into it at Guice setter time. The lazy wrappers in
 * {@code DataSourceModule} hold the same instance and call {@link #decryptInPlace} on every
 * connector call to materialize plaintext from encrypted blobs.
 *
 * <p>Asymmetry with the producer side is intentional. PUT is lax: if the encryption service is
 * unavailable, secrets are stored as plaintext (the cluster has no way to encrypt). FROM is strict:
 * if an {@link EncryptedSecret} arrives at the connector boundary but no service is bound to decrypt
 * it, the call fails with 503 — passing the SDK opaque bytes it can't read would surface as a
 * confusing auth error or worse.
 *
 * <p>TODO(#149194): the volatile-slot pattern mirrors the same per-project mismatch the linked issue
 * flags inside {@code PrimaryEncryptionKeyService}. When that lands and the service becomes
 * project-aware, replace the slot with a per-call lookup that carries {@code ProjectId} context.
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
            if (value instanceof EncryptedSecret encrypted) {
                if (service == null) {
                    throw new ElasticsearchStatusException(
                        "cannot decrypt secret data-source settings: encryption service is not bound on this node",
                        RestStatus.SERVICE_UNAVAILABLE
                    );
                }
                result.put(entry.getKey(), decryptToString(encrypted.blob(), service));
            } else {
                result.put(entry.getKey(), value);
            }
        }
        return result;
    }

    private static String decryptToString(byte[] blob, EncryptionService service) {
        EncryptedData encrypted;
        try (StreamInput in = new BytesArray(blob).streamInput()) {
            encrypted = new EncryptedData(in);
        } catch (IOException e) {
            throw new ElasticsearchStatusException(
                "cannot decrypt secret data-source setting: encrypted blob is malformed",
                RestStatus.INTERNAL_SERVER_ERROR,
                e
            );
        }
        byte[] plaintext = service.decrypt(encrypted);
        try {
            return new String(plaintext, StandardCharsets.UTF_8);
        } finally {
            Arrays.fill(plaintext, (byte) 0);
        }
    }
}
