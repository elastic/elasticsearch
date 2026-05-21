/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.cluster.metadata.DataSourceSetting;
import org.elasticsearch.cluster.metadata.DataSourceSettings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.crypto.EncryptedData;
import org.elasticsearch.xpack.core.crypto.EncryptionService;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Master-side encryption step for data-source secret values. Wraps the optional
 * {@link EncryptionService} binding; callers hand it the settings of a PUT and get back what
 * should be stored.
 *
 * <p>Two paths:
 * <ul>
 *   <li><b>service bound</b> — every plaintext-String secret is replaced by its
 *       {@link EncryptedData} ciphertext blob; non-secret values, null-valued secrets, and
 *       already-encrypted carriers pass through unchanged.
 *   <li><b>no service bound</b> — settings pass through unchanged. If any of them is a real
 *       secret being persisted as plaintext, a {@code WARN} naming the data source is logged so
 *       the operator can see exactly which credentials hit the disk in clear text.
 * </ul>
 *
 * <p>The constructor accepts a {@code null} service so callers don't have to guard at construction
 * time; the {@link #apply(String, DataSourceSettings)} method does the right thing in both cases.
 */
public final class DataSourceEncryption {

    private static final Logger logger = LogManager.getLogger(DataSourceEncryption.class);

    @Nullable
    private final EncryptionService service;

    public DataSourceEncryption(@Nullable EncryptionService service) {
        this.service = service;
    }

    /**
     * Apply the encryption step to {@code settings}. Returns the input unchanged when no
     * encryption service is bound; otherwise returns a new {@link DataSourceSettings} with every
     * plaintext secret replaced by its ciphertext carrier. When no service is bound and the input
     * holds at least one real secret, logs a {@code WARN} naming {@code dataSourceName} so the
     * operator can see exactly which credentials are stored as plaintext.
     */
    public DataSourceSettings apply(String dataSourceName, DataSourceSettings settings) {
        if (service == null) {
            if (settings.hasSecrets()) {
                logger.warn(
                    "credentials for data source [{}] are stored as plaintext because no encryption service is available",
                    dataSourceName
                );
            }
            return settings;
        }
        Map<String, DataSourceSetting> result = new HashMap<>(settings.size());
        for (var entry : settings) {
            String key = entry.getKey();
            DataSourceSetting setting = entry.getValue();
            if (needsEncryption(setting)) {
                result.put(key, encryptOne(key, (String) setting.rawValue()));
            } else {
                result.put(key, setting);
            }
        }
        return new DataSourceSettings(result);
    }

    /** Only fresh plaintext String secrets need encrypting; non-secrets, null, and existing ciphertext byte[] pass through. */
    private static boolean needsEncryption(DataSourceSetting setting) {
        return setting.secret() && setting.rawValue() instanceof String;
    }

    /**
     * Encrypt one plaintext into a freshly-built ciphertext-carrying setting. The intermediate
     * UTF-8 byte buffer is zeroed in {@code finally} so this method's own byte-array copy does not
     * linger; the original plaintext String reference still lives in the caller's settings until
     * the cluster-state task completes (narrowing that lifetime is Phase 2).
     */
    private DataSourceSetting encryptOne(String key, String plaintext) {
        byte[] bytes = plaintext.getBytes(StandardCharsets.UTF_8);
        try {
            EncryptedData encrypted = service.encrypt(bytes);
            byte[] blob = serializeCarrier(encrypted, key);
            return new DataSourceSetting(blob, true, DataSourceSetting.EncryptionFormat.V1);
        } finally {
            Arrays.fill(bytes, (byte) 0);
        }
    }

    /** Render the upstream {@link EncryptedData} primitive into its native byte representation. */
    private static byte[] serializeCarrier(EncryptedData encrypted, String key) {
        BytesStreamOutput out = new BytesStreamOutput();
        try {
            encrypted.writeTo(out);
        } catch (IOException e) {
            throw new ElasticsearchStatusException(
                "failed to serialize encrypted value for setting [" + key + "]",
                RestStatus.INTERNAL_SERVER_ERROR,
                e
            );
        }
        return BytesReference.toBytes(out.bytes());
    }
}
