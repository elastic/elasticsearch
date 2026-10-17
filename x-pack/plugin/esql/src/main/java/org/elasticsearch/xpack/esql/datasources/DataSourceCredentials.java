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
import org.elasticsearch.xpack.encryption.spi.EncryptionKeyNotYetAvailableException;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Decrypts secret values at the connector boundary. Created once per node by {@code EsqlPlugin} with the node's
 * {@link EncryptionService}; the lazy wrappers in {@code DataSourceModule} call {@link #decryptInPlace} before each connector use.
 *
 * <p>The key cache behind {@link EncryptionService} is filled from this node's own applied cluster state, while the coordinator picked
 * this node as a worker from <em>its</em> applied state. A node that is still applying the state carrying the key (one that just
 * joined, or lags a rotation) therefore reports {@link EncryptionKeyNotYetAvailableException}. That is a node-local SPI signal and not a
 * registered exception, so it is translated here into an {@link ExternalUnavailableException} with
 * {@link ExternalException.Condition#CREDENTIALS_NOT_READY}: the condition rides in the exception metadata, survives the node
 * boundary, and lets the coordinator route the splits to another worker instead of failing the query. {@link #preflight} lets a worker
 * raise it before any data moves.
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
                result.put(entry.getKey(), decryptValue(encrypted));
            } else {
                result.put(entry.getKey(), value);
            }
        }
        return result;
    }

    /**
     * Checks that every secret carried by an external source in {@code plan} can be decrypted on this node, before anything is read.
     * Throws exactly what a connector open would throw later, so a worker that cannot decrypt yet fails fast and empty, not mid-stream.
     */
    public void preflight(PhysicalPlan plan) {
        plan.forEachDown(ExternalSourceExec.class, exec -> decryptInPlace(ExternalSourceResolver.storageConfig(exec.config())));
    }

    /** Decrypt the carrier and deserialize the plaintext back to the original value type. */
    private Object decryptValue(EncryptedData encrypted) {
        final byte[] plaintext;
        try {
            plaintext = encryptionService.decrypt(encrypted);
        } catch (EncryptionKeyNotYetAvailableException e) {
            throw credentialsNotReady(encrypted, e);
        }
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

    /**
     * The typed, wire-safe form of "this node does not hold the key yet". Permanent conditions
     * ({@link org.elasticsearch.xpack.encryption.spi.EncryptionServiceUnavailableException}) are deliberately not translated: nothing
     * is gained by moving them to another node.
     */
    static ExternalUnavailableException credentialsNotReady(EncryptedData encrypted, EncryptionKeyNotYetAvailableException cause) {
        return new ExternalUnavailableException(
            ExternalException.Condition.CREDENTIALS_NOT_READY,
            StoragePath.NONE,
            "key [" + encrypted.keyId() + "]",
            "Retry the query",
            false,
            0L,
            cause
        );
    }
}
