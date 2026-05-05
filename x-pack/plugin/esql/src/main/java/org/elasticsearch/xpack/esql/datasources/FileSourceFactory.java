/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorFactoryProvider;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

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
    private final DecompressionCodecRegistry codecRegistry;
    private final Settings settings;
    @Nullable
    private final ExecutorService splitDiscoveryExecutor;

    FileSourceFactory(
        StorageProviderRegistry storageRegistry,
        FormatReaderRegistry formatRegistry,
        DecompressionCodecRegistry codecRegistry,
        Settings settings
    ) {
        this(storageRegistry, formatRegistry, codecRegistry, settings, null);
    }

    FileSourceFactory(
        StorageProviderRegistry storageRegistry,
        FormatReaderRegistry formatRegistry,
        DecompressionCodecRegistry codecRegistry,
        Settings settings,
        @Nullable ExecutorService splitDiscoveryExecutor
    ) {
        Check.notNull(storageRegistry, "storageRegistry cannot be null");
        Check.notNull(formatRegistry, "formatRegistry cannot be null");
        this.storageRegistry = storageRegistry;
        this.formatRegistry = formatRegistry;
        this.codecRegistry = codecRegistry != null ? codecRegistry : new DecompressionCodecRegistry();
        this.settings = settings != null ? settings : Settings.EMPTY;
        this.splitDiscoveryExecutor = splitDiscoveryExecutor;
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
            if (formatRegistry.hasExtension(ext)) {
                return true;
            }
            if (codecRegistry.hasCompressionExtension(ext) && formatRegistry.hasCompressedExtension(objectName)) {
                return true;
            }
            return false;
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
            if (storageObject.exists() == false) {
                throw new IOException("File does not exist: " + location);
            }
            FormatReader reader = resolveFormatReader(storagePath.objectName(), config).withConfig(config);
            return reader.metadata(storageObject);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to resolve metadata for [" + location + "]", e);
        }
    }

    @Override
    public SplitProvider splitProvider() {
        return new FileSplitProvider(
            FileSplitProvider.DEFAULT_TARGET_SPLIT_SIZE,
            codecRegistry,
            storageRegistry,
            formatRegistry,
            settings,
            splitDiscoveryExecutor
        );
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

            FormatReader format = resolveFormatReader(path.objectName(), config).withConfig(config)
                .withPushedFilter(context.pushedFilter())
                .withSchema(context.attributes());
            ErrorPolicy errorPolicy = resolveErrorPolicy(config, format);

            Map<String, Object> partitionValues = Map.of();
            if (context.split() instanceof FileSplit fileSplit) {
                partitionValues = fileSplit.partitionValues();
            }

            List<Expression> pushedExpressions = context.pushedExpressions();
            FilterPushdownSupport pushdownSupport = (pushedExpressions != null && pushedExpressions.isEmpty() == false)
                ? format.filterPushdownSupport()
                : null;

            Closeable onClose = null;
            ConcurrencyBudgetAllocator allocator = storageRegistry.allocatorForScheme(path.scheme().toLowerCase(Locale.ROOT));
            if (allocator != null) {
                QueryBudgetedStorageProvider budgeted = new QueryBudgetedStorageProvider(storage, allocator.register());
                storage = budgeted;
                onClose = budgeted;
            }

            Executor readExecutor = context.fileReadExecutor() != null ? context.fileReadExecutor() : context.executor();
            return AsyncExternalSourceOperatorFactory.builder(
                storage,
                format,
                path,
                context.attributes(),
                context.batchSize(),
                context.maxBufferSize(),
                readExecutor
            )
                .rowLimit(context.rowLimit())
                .fileList(context.fileList())
                .partitionColumnNames(context.partitionColumnNames())
                .partitionValues(partitionValues)
                .sliceQueue(context.sliceQueue())
                .errorPolicy(errorPolicy)
                .parsingParallelism(context.parsingParallelism())
                .parallelism(context.parallelism())
                .pushedExpressions(pushedExpressions)
                .pushdownSupport(pushdownSupport)
                .onClose(onClose)
                .build();
        };
    }

    /** Delegates to {@link ErrorPolicy#fromConfig(Map, ErrorPolicy)} with the format's default
     *  policy as the fallback. Kept here so existing call sites and tests do not have to change. */
    static ErrorPolicy resolveErrorPolicy(Map<String, Object> config, FormatReader format) {
        return ErrorPolicy.fromConfig(config, format.defaultErrorPolicy());
    }

    private FormatReader resolveFormatReader(String objectName, Map<String, Object> config) {
        return FormatNameResolver.resolveReader(config, objectName, formatRegistry);
    }
}
