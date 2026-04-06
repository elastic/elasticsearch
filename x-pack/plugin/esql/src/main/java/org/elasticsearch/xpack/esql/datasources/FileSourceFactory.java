/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Settings;
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

import java.io.IOException;
import java.util.List;
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
    private final DecompressionCodecRegistry codecRegistry;
    private final Settings settings;

    FileSourceFactory(
        StorageProviderRegistry storageRegistry,
        FormatReaderRegistry formatRegistry,
        DecompressionCodecRegistry codecRegistry,
        Settings settings
    ) {
        Check.notNull(storageRegistry, "storageRegistry cannot be null");
        Check.notNull(formatRegistry, "formatRegistry cannot be null");
        this.storageRegistry = storageRegistry;
        this.formatRegistry = formatRegistry;
        this.codecRegistry = codecRegistry != null ? codecRegistry : new DecompressionCodecRegistry();
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
            FormatReader reader = resolveFormatReader(storagePath.objectName(), config).withConfig(config);
            return reader.metadata(storageObject);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to resolve metadata for [" + location + "]", e);
        }
    }

    @Override
    public SplitProvider splitProvider() {
        return new FileSplitProvider(FileSplitProvider.DEFAULT_TARGET_SPLIT_SIZE, codecRegistry, storageRegistry, formatRegistry, settings);
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

            return new AsyncExternalSourceOperatorFactory(
                storage,
                format,
                path,
                context.attributes(),
                context.batchSize(),
                context.maxBufferSize(),
                context.rowLimit(),
                context.executor(),
                context.fileList(),
                context.partitionColumnNames(),
                partitionValues,
                context.sliceQueue(),
                errorPolicy,
                context.parsingParallelism(),
                null,
                pushedExpressions,
                pushdownSupport
            );
        };
    }

    static final String CONFIG_FORMAT = "format";
    static final String CONFIG_MAX_ERRORS = "max_errors";
    static final String CONFIG_MAX_ERROR_RATIO = "max_error_ratio";
    static final String CONFIG_ERROR_MODE = "error_mode";

    static ErrorPolicy resolveErrorPolicy(Map<String, Object> config, FormatReader format) {
        if (config == null) {
            return format.defaultErrorPolicy();
        }
        Object maxErrorsValue = config.get(CONFIG_MAX_ERRORS);
        Object maxErrorRatioValue = config.get(CONFIG_MAX_ERROR_RATIO);
        Object errorModeValue = config.get(CONFIG_ERROR_MODE);
        if (maxErrorsValue == null && maxErrorRatioValue == null && errorModeValue == null) {
            return format.defaultErrorPolicy();
        }

        // When only budget keys are set (no explicit mode), default to SKIP_ROW.
        // When only mode is set, budget defaults depend on the mode.
        ErrorPolicy.Mode mode = ErrorPolicy.Mode.SKIP_ROW;
        if (errorModeValue != null) {
            String modeStr = errorModeValue.toString();
            try {
                mode = ErrorPolicy.Mode.parse(modeStr);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid value for [" + CONFIG_ERROR_MODE + "]: [" + errorModeValue + "]", e);
            }
            if (mode == null) {
                throw new IllegalArgumentException("Invalid value for [" + CONFIG_ERROR_MODE + "]: [" + errorModeValue + "]");
            }
        }

        // FAIL_FAST is incompatible with budget settings — it always aborts on the first error.
        if (mode == ErrorPolicy.Mode.FAIL_FAST) {
            if (maxErrorsValue != null || maxErrorRatioValue != null) {
                throw new IllegalArgumentException(
                    "["
                        + CONFIG_MAX_ERRORS
                        + "] and ["
                        + CONFIG_MAX_ERROR_RATIO
                        + "] cannot be used with ["
                        + CONFIG_ERROR_MODE
                        + "="
                        + mode
                        + "]; fail_fast always aborts on the first error"
                );
            }
            return ErrorPolicy.STRICT;
        }

        long maxErrors;
        if (maxErrorsValue != null) {
            try {
                maxErrors = Long.parseLong(maxErrorsValue.toString());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid value for [" + CONFIG_MAX_ERRORS + "]: [" + maxErrorsValue + "]", e);
            }
        } else {
            maxErrors = Long.MAX_VALUE;
        }

        double maxErrorRatio = 0.0;
        if (maxErrorRatioValue != null) {
            try {
                maxErrorRatio = Double.parseDouble(maxErrorRatioValue.toString());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid value for [" + CONFIG_MAX_ERROR_RATIO + "]: [" + maxErrorRatioValue + "]", e);
            }
        }

        boolean logErrors = maxErrors < Long.MAX_VALUE || maxErrorRatio > 0.0;
        return new ErrorPolicy(mode, maxErrors, maxErrorRatio, logErrors);
    }

    private FormatReader resolveFormatReader(String objectName, Map<String, Object> config) {
        if (config != null) {
            Object formatOverride = config.get(CONFIG_FORMAT);
            if (formatOverride != null) {
                String formatName = formatOverride.toString();
                if (formatName.isEmpty() == false) {
                    return formatRegistry.byName(formatName);
                }
            }
        }
        return formatRegistry.byExtension(objectName);
    }
}
