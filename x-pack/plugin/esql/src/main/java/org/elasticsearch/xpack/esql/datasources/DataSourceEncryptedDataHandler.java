/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSettings;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Re-encrypts every stored data-source secret under the active project encryption key on rotation —
 * without it, retiring a key would strand secrets undecryptable. Driven by the rotation coordinator for
 * the {@link DataSourceMetadata} custom; contributed via {@link EsqlEncryptedDataHandlerProvider}. The
 * payload is an opaque {@code writeGenericValue} blob, preserved verbatim — only the wrapping key changes.
 */
public final class DataSourceEncryptedDataHandler implements EncryptedDataHandler<DataSourceMetadata> {

    @Override
    public String customName() {
        return DataSourceMetadata.TYPE;
    }

    /**
     * On destructive reset, wipe only the encrypted credential values while preserving the rest of
     * each data source's configuration (name, type, description, non-secret settings). Users will
     * see {@code null} for credentials in the API and need only re-provision those — not recreate
     * the entire data source from scratch.
     */
    @Override
    public DataSourceMetadata onDestructiveReset(DataSourceMetadata current) {
        if (current == null || current.dataSources().isEmpty()) {
            return current;
        }
        Map<String, DataSource> rebuilt = new HashMap<>(current.dataSources().size());
        for (Map.Entry<String, DataSource> entry : current.dataSources().entrySet()) {
            rebuilt.put(entry.getKey(), wipeSecrets(entry.getValue()));
        }
        return new DataSourceMetadata(rebuilt);
    }

    private static DataSource wipeSecrets(DataSource dataSource) {
        Map<String, DataSourceSetting> rebuilt = new HashMap<>(dataSource.settings().size());
        for (Map.Entry<String, DataSourceSetting> entry : dataSource.settings()) {
            DataSourceSetting setting = entry.getValue();
            rebuilt.put(entry.getKey(), setting.secret() ? new DataSourceSetting(null, true) : setting);
        }
        return new DataSource(dataSource.name(), dataSource.type(), dataSource.description(), rebuilt);
    }

    @Override
    public DataSourceMetadata reEncrypt(DataSourceMetadata current, EncryptionService encryptionService, String activeKeyId) {
        if (current == null || current.dataSources().isEmpty()) {
            return current;
        }
        Map<String, DataSource> rebuilt = new HashMap<>(current.dataSources().size());
        boolean changed = false;
        for (Map.Entry<String, DataSource> entry : current.dataSources().entrySet()) {
            DataSource dataSource = entry.getValue();
            DataSource reEncrypted = reEncrypt(dataSource, encryptionService, activeKeyId);
            rebuilt.put(entry.getKey(), reEncrypted);
            changed |= reEncrypted != dataSource;
        }
        return changed ? new DataSourceMetadata(rebuilt) : current;
    }

    private static DataSource reEncrypt(DataSource dataSource, EncryptionService encryptionService, String activeKeyId) {
        DataSourceSettings settings = dataSource.settings();
        Map<String, DataSourceSetting> rebuilt = new HashMap<>(settings.size());
        boolean changed = false;
        for (Map.Entry<String, DataSourceSetting> entry : settings) {
            DataSourceSetting setting = entry.getValue();
            DataSourceSetting reEncrypted = reEncrypt(setting, encryptionService, activeKeyId);
            rebuilt.put(entry.getKey(), reEncrypted);
            changed |= reEncrypted != setting;
        }
        if (changed == false) {
            return dataSource;
        }
        return new DataSource(dataSource.name(), dataSource.type(), dataSource.description(), rebuilt);
    }

    private static DataSourceSetting reEncrypt(DataSourceSetting setting, EncryptionService encryptionService, String activeKeyId) {
        if (setting.isEncrypted() == false) {
            return setting;
        }
        EncryptedData existing = (EncryptedData) setting.rawValue();
        if (existing.keyId().equals(activeKeyId)) {
            return setting;
        }
        byte[] plaintext = encryptionService.decrypt(existing);
        try {
            return new DataSourceSetting(encryptionService.encrypt(plaintext), true);
        } finally {
            Arrays.fill(plaintext, (byte) 0);
        }
    }
}
