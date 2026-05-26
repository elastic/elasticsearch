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
 * Decrypts secret values at the catalog-invocation point. The single per-node instance is created
 * by {@code EsqlPlugin.createComponents}; {@code TransportPutDataSourceAction} (constructed on every
 * node at startup, with {@link EncryptionService} as a hard injection) pushes the service into it. The
 * lazy wrappers in {@code DataSourceModule} hold the same instance and call {@link #decryptInPlace} on
 * every connector call to materialize plaintext from {@link EncryptedData} carriers just before the
 * connector uses them.
 *
 * <p>{@code EsqlPlugin} couples the data-source feature to the project-encryption-key feature, so a node
 * serving a data source always has the service bound. The strict path below guards the otherwise
 * impossible unbound state: if an
 * {@link EncryptedData} reaches the connector boundary but no service is bound to decrypt it, the call
 * fails with {@code 503} — passing the SDK opaque bytes it can't read would surface as a confusing auth
 * error or worse.
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
