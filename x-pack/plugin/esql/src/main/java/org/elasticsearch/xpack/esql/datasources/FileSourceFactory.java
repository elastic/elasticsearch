/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractorAware;
import org.elasticsearch.xpack.esql.datasources.spi.ConfigKeyValidator;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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

    static final String CONFIG_FORMAT = "format";

    /**
     * Aggregated set of keys the coordinator-side path claims from a per-query configuration map.
     * Built from each component's own {@code CONFIG_KEYS} set so adding a new coordinator-level
     * configuration consumer requires updating only the consumer's own constant — the union here
     * picks it up automatically. Components contributing today: {@link ErrorPolicy},
     * {@link FileSplitProvider}, {@link PartitionConfig}, the {@link #CONFIG_FORMAT} override read
     * by this class, and the {@link FormatNameResolver#CONFIG_READER} override read by the
     * format-name resolver.
     */
    static final Set<String> COORDINATOR_KEYS;

    /**
     * Coordinator keys deliberately NOT exposed as dataset settings: the {@link #CONFIG_FORMAT} and
     * {@link FormatNameResolver#CONFIG_READER} overrides remain EXTERNAL-only development knobs (a
     * dataset implies its format from the registered resource's extension). Pinned against
     * {@link #COORDINATOR_KEYS} and the dataset key set by {@code FileSourceFactoryValidationTests}
     * so neither can drift: any new coordinator key must either be added to the dataset vocabulary or
     * explicitly listed here.
     */
    static final Set<String> EXTERNAL_ONLY_KEYS = Set.of(CONFIG_FORMAT, FormatNameResolver.CONFIG_READER);

    static {
        Set<String> keys = new HashSet<>();
        keys.add(CONFIG_FORMAT);
        keys.add(FormatNameResolver.CONFIG_READER);
        keys.addAll(ErrorPolicy.CONFIG_KEYS);
        keys.addAll(FileSplitProvider.CONFIG_KEYS);
        keys.addAll(ExternalSourceResolver.CONFIG_KEYS);
        keys.addAll(PartitionConfig.CONFIG_KEYS);
        COORDINATOR_KEYS = Set.copyOf(keys);
    }

    private final StorageProviderRegistry storageRegistry;
    private final FormatReaderRegistry formatRegistry;
    private final DecompressionCodecRegistry codecRegistry;
    private final Settings settings;
    @Nullable
    private final ExecutorService splitDiscoveryExecutor;
    /**
     * Node-level (root) {@link BlockFactory}, threaded into
     * {@link AsyncExternalSourceOperatorFactory.Builder#producerBlockFactory(BlockFactory)} so that
     * producer-thread allocations performed by iterator wrappers ({@link VirtualColumnIterator},
     * {@link SchemaAdaptingIterator}) route through the global request circuit breaker rather than
     * the driver-local breaker. May be {@code null} in tests where the factory falls back to
     * {@link org.elasticsearch.compute.operator.DriverContext#blockFactory()}.
     */
    @Nullable
    private final BlockFactory blockFactory;

    FileSourceFactory(
        StorageProviderRegistry storageRegistry,
        FormatReaderRegistry formatRegistry,
        DecompressionCodecRegistry codecRegistry,
        Settings settings
    ) {
        this(storageRegistry, formatRegistry, codecRegistry, settings, null, null);
    }

    FileSourceFactory(
        StorageProviderRegistry storageRegistry,
        FormatReaderRegistry formatRegistry,
        DecompressionCodecRegistry codecRegistry,
        Settings settings,
        @Nullable ExecutorService splitDiscoveryExecutor
    ) {
        this(storageRegistry, formatRegistry, codecRegistry, settings, splitDiscoveryExecutor, null);
    }

    FileSourceFactory(
        StorageProviderRegistry storageRegistry,
        FormatReaderRegistry formatRegistry,
        DecompressionCodecRegistry codecRegistry,
        Settings settings,
        @Nullable ExecutorService splitDiscoveryExecutor,
        @Nullable BlockFactory blockFactory
    ) {
        Check.notNull(storageRegistry, "storageRegistry cannot be null");
        Check.notNull(formatRegistry, "formatRegistry cannot be null");
        this.storageRegistry = storageRegistry;
        this.formatRegistry = formatRegistry;
        this.codecRegistry = codecRegistry != null ? codecRegistry : new DecompressionCodecRegistry();
        this.settings = settings != null ? settings : Settings.EMPTY;
        this.splitDiscoveryExecutor = splitDiscoveryExecutor;
        this.blockFactory = blockFactory;
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
    public void validateConfig(String location, Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return;
        }
        StoragePath storagePath = StoragePath.of(location);
        Configured<StorageProvider> resolvedStorage = storageRegistry.createProviderTrackingConsumedKeys(
            storagePath.scheme(),
            settings,
            ExternalSourceResolver.storageConfig(config)
        );
        Configured<FormatReader> resolvedReader = resolveFormatReader(storagePath.objectName(), config).withConfigTrackingConsumedKeys(
            config
        );
        ConfigKeyValidator.check(config, List.of(resolvedStorage.consumedKeys(), resolvedReader.consumedKeys(), COORDINATOR_KEYS));
    }

    @Override
    public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
        try {
            // Reject unknown configuration keys via the SPI hook before any provider/reader work.
            // The provider/reader resolutions below hit the same cache keys validateConfig populates,
            // so this is a single source of truth for validation without extra cloud-client construction.
            validateConfig(location, config);
            StoragePath storagePath = StoragePath.of(location);
            String scheme = storagePath.scheme();

            StorageProvider provider;
            FormatReader reader;
            if (config != null && config.isEmpty() == false) {
                provider = storageRegistry.createProviderTrackingConsumedKeys(
                    scheme,
                    settings,
                    ExternalSourceResolver.storageConfig(config)
                ).value();
                reader = resolveFormatReader(storagePath.objectName(), config).withConfigTrackingConsumedKeys(config).value();
            } else {
                provider = storageRegistry.provider(storagePath);
                reader = resolveFormatReader(storagePath.objectName(), config).withConfig(config);
            }

            StorageObject storageObject = provider.newObject(storagePath);
            if (storageObject.exists() == false) {
                throw new IOException("File does not exist: " + location);
            }
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
                storage = storageRegistry.createProvider(path.scheme(), settings, ExternalSourceResolver.storageConfig(config));
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
            // Auto-detect the deferred-extraction signal: the synthetic _rowPosition column in the
            // projection means InsertExternalFieldExtraction injected a paired
            // ExternalFieldExtractExec downstream and expects this source to register a
            // ColumnExtractor per opened file plus emit encoded row references. Only enable it
            // when the resolved reader actually advertises ColumnExtractorAware — without that
            // capability the builder would refuse to set the flag.
            boolean deferredExtraction = format instanceof ColumnExtractorAware
                && context.projectedColumns() != null
                && context.projectedColumns().contains(ColumnExtractor.ROW_POSITION_COLUMN);

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
                .schemaMap(context.schemaMap())
                .partitionColumnNames(context.partitionColumnNames())
                .partitionValues(partitionValues)
                .producerBlockFactory(blockFactory)
                .sliceQueue(context.sliceQueue())
                .errorPolicy(errorPolicy)
                .parsingParallelism(context.parsingParallelism())
                .maxConcurrentOpenSegments(context.maxConcurrentOpenSegments())
                .maxRecordBytes(context.maxRecordBytes())
                .parallelism(context.parallelism())
                .pushedExpressions(pushedExpressions)
                .pushdownSupport(pushdownSupport)
                .onClose(onClose)
                .deferredExtraction(deferredExtraction)
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
