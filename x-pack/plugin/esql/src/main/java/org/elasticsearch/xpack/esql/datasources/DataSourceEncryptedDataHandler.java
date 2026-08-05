/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.core.encryption.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;

import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * Re-keys stored data-source secrets. This handler knows where the {@link EncryptedData} values live inside
 * {@link DataSourceMetadata} and applies the caller-supplied re-keying function to them — on key rotation this keeps
 * secrets decryptable after the previous key is retired; without it, retiring a key would strand secrets
 * undecryptable. Contributed via {@link EsqlEncryptedDataHandlerProvider}. The payload is an opaque
 * {@code writeGenericValue} blob, preserved verbatim — only the wrapping key changes.
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
    public DataSourceMetadata reEncrypt(DataSourceMetadata current, UnaryOperator<EncryptedData> rewrap) {
        if (current.dataSources().isEmpty()) {
            return current;
        }
        Map<String, DataSource> rebuiltSources = new HashMap<>(current.dataSources().size());
        boolean changed = false;
        for (Map.Entry<String, DataSource> sourceEntry : current.dataSources().entrySet()) {
            DataSource dataSource = sourceEntry.getValue();
            Map<String, DataSourceSetting> rebuiltSettings = new HashMap<>(dataSource.settings().size());
            boolean sourceChanged = false;
            for (Map.Entry<String, DataSourceSetting> entry : dataSource.settings()) {
                DataSourceSetting setting = entry.getValue();
                if (setting.isEncrypted()) {
                    EncryptedData existing = (EncryptedData) setting.rawValue();
                    EncryptedData rewrapped = rewrap.apply(existing);
                    if (rewrapped != existing) {
                        setting = new DataSourceSetting(rewrapped, true);
                        sourceChanged = true;
                    }
                }
                rebuiltSettings.put(entry.getKey(), setting);
            }
            rebuiltSources.put(
                sourceEntry.getKey(),
                sourceChanged ? new DataSource(dataSource.name(), dataSource.type(), dataSource.description(), rebuiltSettings) : dataSource
            );
            changed |= sourceChanged;
        }
        return changed ? new DataSourceMetadata(rebuiltSources) : current;
    }
}
