/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorFactoryProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.util.Map;

/**
 * Framework-internal factory that bridges the building-block registries
 * ({@link StorageProviderRegistry} + {@link FormatReaderRegistry}) into the
 * unified {@link ExternalSourceFactory} contract.
 *
 * <p>This is NOT an SPI extension — it is never returned by any DataSourcePlugin.
 * It is created by {@link DataSourceModule} itself and registered as a catch-all
 * fallback entry (key {@code "file"}) in the sourceFactories map.
 */
final class FileSourceFactory implements ExternalSourceFactory {

    private final StorageProviderRegistry storageRegistry;
    private final FormatReaderRegistry formatRegistry;
    private final Settings settings;

    FileSourceFactory(StorageProviderRegistry storageRegistry, FormatReaderRegistry formatRegistry, Settings settings) {
        if (storageRegistry == null) {
            throw new IllegalArgumentException("storageRegistry cannot be null");
        }
        if (formatRegistry == null) {
            throw new IllegalArgumentException("formatRegistry cannot be null");
        }
        this.storageRegistry = storageRegistry;
        this.formatRegistry = formatRegistry;
        this.settings = settings != null ? settings : Settings.EMPTY;
    }

    @Override
    public String type() {
        return "file";
    }

    @Override
    public boolean canHandle(String location) {
        if (location == null) {
            return false;
        }
        try {
            StoragePath path = StoragePath.of(location);
            String scheme = path.scheme();
            String objectName = path.objectName();
            if (objectName == null || objectName.isEmpty()) {
                return false;
            }
            int lastDot = objectName.lastIndexOf('.');
            if (lastDot < 0 || lastDot == objectName.length() - 1) {
                return false;
            }
            if (storageRegistry.hasProvider(scheme) == false) {
                return false;
            }
            String ext = objectName.substring(objectName.lastIndexOf('.'));
            return formatRegistry.hasExtension(ext);
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    @Override
    public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
        try {
            StoragePath storagePath = StoragePath.of(location);
            String scheme = storagePath.scheme();

            StorageProvider provider;
            if (config != null && config.isEmpty() == false) {
                provider = storageRegistry.createProvider(scheme, settings, config);
            } else {
                provider = storageRegistry.provider(storagePath);
            }

            StorageObject storageObject = provider.newObject(storagePath);
            FormatReader reader = formatRegistry.byExtension(storagePath.objectName());
            return reader.metadata(storageObject);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to resolve metadata for [" + location + "]", e);
        }
    }

    @Override
    public SourceOperatorFactoryProvider operatorFactory() {
        return context -> {
            StoragePath path = context.path();
            Map<String, Object> config = context.config();

            StorageProvider storage;
            if (config != null && config.isEmpty() == false) {
                storage = storageRegistry.createProvider(path.scheme(), settings, config);
            } else {
                storage = storageRegistry.provider(path);
            }

            FormatReader format = formatRegistry.byExtension(path.objectName());

            return new AsyncExternalSourceOperatorFactory(
                storage,
                format,
                path,
                context.attributes(),
                context.batchSize(),
                context.maxBufferSize(),
                context.executor(),
                context.fileSet()
            );
        };
    }
}
