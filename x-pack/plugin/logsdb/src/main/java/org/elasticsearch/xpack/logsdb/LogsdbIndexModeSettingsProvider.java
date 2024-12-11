/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceFieldMapper;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_PATH;
import static org.elasticsearch.xpack.logsdb.LogsDBPlugin.CLUSTER_LOGSDB_ENABLED;

final class LogsdbIndexModeSettingsProvider implements IndexSettingProvider {
    private static final Logger LOGGER = LogManager.getLogger(LogsdbIndexModeSettingsProvider.class);
    private static final String LOGS_PATTERN = "logs-*-*";

    private final SyntheticSourceLicenseService syntheticSourceLicenseService;
    private final SetOnce<CheckedFunction<IndexMetadata, MapperService, IOException>> mapperServiceFactory = new SetOnce<>();
    private final SetOnce<Supplier<IndexVersion>> createdIndexVersion = new SetOnce<>();

    private volatile boolean isLogsdbEnabled;

    LogsdbIndexModeSettingsProvider(SyntheticSourceLicenseService syntheticSourceLicenseService, final Settings settings) {
        this.syntheticSourceLicenseService = syntheticSourceLicenseService;
        this.isLogsdbEnabled = CLUSTER_LOGSDB_ENABLED.get(settings);
    }

    void updateClusterIndexModeLogsdbEnabled(boolean isLogsdbEnabled) {
        this.isLogsdbEnabled = isLogsdbEnabled;
    }

    void init(CheckedFunction<IndexMetadata, MapperService, IOException> factory, Supplier<IndexVersion> indexVersion) {
        mapperServiceFactory.set(factory);
        createdIndexVersion.set(indexVersion);
    }

    private boolean supportFallbackToStoredSource() {
        return mapperServiceFactory.get() != null;
    }

    @Override
    public boolean overrulesTemplateAndRequestSettings() {
        // Indicates that the provider value takes precedence over any user setting.
        return true;
    }

    @Override
    public Settings getAdditionalIndexSettings(
        final String indexName,
        final String dataStreamName,
        IndexMode templateIndexMode,
        final Metadata metadata,
        final Instant resolvedAt,
        Settings settings,
        final List<CompressedXContent> combinedTemplateMappings
    ) {
        Settings.Builder settingsBuilder = null;
        if (isLogsdbEnabled
            && dataStreamName != null
            && resolveIndexMode(settings.get(IndexSettings.MODE.getKey())) == null
            && matchesLogsPattern(dataStreamName)) {
            settingsBuilder = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.getName());
            if (supportFallbackToStoredSource()) {
                settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.getName()).put(settings).build();
            }
        }

        if (supportFallbackToStoredSource()) {
            // This index name is used when validating component and index templates, we should skip this check in that case.
            // (See MetadataIndexTemplateService#validateIndexTemplateV2(...) method)
            boolean isTemplateValidation = "validate-index-name".equals(indexName);
            boolean legacyLicensedUsageOfSyntheticSourceAllowed = isLegacyLicensedUsageOfSyntheticSourceAllowed(
                templateIndexMode,
                indexName,
                dataStreamName
            );
            if (newIndexHasSyntheticSourceUsage(indexName, templateIndexMode, settings, combinedTemplateMappings)
                && syntheticSourceLicenseService.fallbackToStoredSource(
                    isTemplateValidation,
                    legacyLicensedUsageOfSyntheticSourceAllowed
                )) {
                LOGGER.debug("creation of index [{}] with synthetic source without it being allowed", indexName);
                if (settingsBuilder == null) {
                    settingsBuilder = Settings.builder();
                }
                settingsBuilder.put(SourceFieldMapper.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.STORED.toString());
            }
        }
        return settingsBuilder == null ? Settings.EMPTY : settingsBuilder.build();
    }

    private static boolean matchesLogsPattern(final String name) {
        return Regex.simpleMatch(LOGS_PATTERN, name);
    }

    private IndexMode resolveIndexMode(final String mode) {
        return mode != null ? Enum.valueOf(IndexMode.class, mode.toUpperCase(Locale.ROOT)) : null;
    }

    boolean newIndexHasSyntheticSourceUsage(
        String indexName,
        IndexMode templateIndexMode,
        Settings indexTemplateAndCreateRequestSettings,
        List<CompressedXContent> combinedTemplateMappings
    ) {
        if ("validate-index-name".equals(indexName)) {
            // This index name is used when validating component and index templates, we should skip this check in that case.
            // (See MetadataIndexTemplateService#validateIndexTemplateV2(...) method)
            return false;
        }

        try {
            var tmpIndexMetadata = buildIndexMetadataForMapperService(indexName, templateIndexMode, indexTemplateAndCreateRequestSettings);
            var indexMode = tmpIndexMetadata.getIndexMode();
            if (SourceFieldMapper.INDEX_MAPPER_SOURCE_MODE_SETTING.exists(tmpIndexMetadata.getSettings())
                || indexMode == IndexMode.LOGSDB
                || indexMode == IndexMode.TIME_SERIES) {
                // In case when index mode is tsdb or logsdb and only _source.mode mapping attribute is specified, then the default
                // could be wrong. However, it doesn't really matter, because if the _source.mode mapping attribute is set to stored,
                // then configuring the index.mapping.source.mode setting to stored has no effect. Additionally _source.mode can't be set
                // to disabled, because that isn't allowed with logsdb/tsdb. In other words setting index.mapping.source.mode setting to
                // stored when _source.mode mapping attribute is stored is fine as it has no effect, but avoids creating MapperService.
                var sourceMode = SourceFieldMapper.INDEX_MAPPER_SOURCE_MODE_SETTING.get(tmpIndexMetadata.getSettings());
                return sourceMode == SourceFieldMapper.Mode.SYNTHETIC;
            }

            // TODO: remove this when _source.mode attribute has been removed:
            try (var mapperService = mapperServiceFactory.get().apply(tmpIndexMetadata)) {
                // combinedTemplateMappings can be null when creating system indices
                // combinedTemplateMappings can be empty when creating a normal index that doesn't match any template and without mapping.
                if (combinedTemplateMappings == null || combinedTemplateMappings.isEmpty()) {
                    combinedTemplateMappings = List.of(new CompressedXContent("{}"));
                }
                mapperService.merge(MapperService.SINGLE_MAPPING_NAME, combinedTemplateMappings, MapperService.MergeReason.INDEX_TEMPLATE);
                return mapperService.documentMapper().sourceMapper().isSynthetic();
            }
        } catch (AssertionError | Exception e) {
            // In case invalid mappings or setting are provided, then mapper service creation can fail.
            // In that case it is ok to return false here. The index creation will fail anyway later, so no need to fallback to stored
            // source.
            LOGGER.info(() -> Strings.format("unable to create mapper service for index [%s]", indexName), e);
            return false;
        }
    }

    // Create a dummy IndexMetadata instance that can be used to create a MapperService in order to check whether synthetic source is used:
    private IndexMetadata buildIndexMetadataForMapperService(
        String indexName,
        IndexMode templateIndexMode,
        Settings indexTemplateAndCreateRequestSettings
    ) {
        var tmpIndexMetadata = IndexMetadata.builder(indexName);

        int dummyPartitionSize = IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING.get(indexTemplateAndCreateRequestSettings);
        int dummyShards = indexTemplateAndCreateRequestSettings.getAsInt(
            IndexMetadata.SETTING_NUMBER_OF_SHARDS,
            dummyPartitionSize == 1 ? 1 : dummyPartitionSize + 1
        );
        int shardReplicas = indexTemplateAndCreateRequestSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
        var finalResolvedSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, createdIndexVersion.get().get())
            .put(indexTemplateAndCreateRequestSettings)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, dummyShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, shardReplicas)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID());

        if (templateIndexMode == IndexMode.TIME_SERIES) {
            finalResolvedSettings.put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES);
            // Avoid failing because index.routing_path is missing (in case fields are marked as dimension)
            finalResolvedSettings.putList(INDEX_ROUTING_PATH.getKey(), List.of("path"));
        }

        tmpIndexMetadata.settings(finalResolvedSettings);
        return tmpIndexMetadata.build();
    }

    /**
     * The GA-ed use cases in which synthetic source usage is allowed with gold or platinum license.
     */
    private boolean isLegacyLicensedUsageOfSyntheticSourceAllowed(IndexMode templateIndexMode, String indexName, String dataStreamName) {
        if (templateIndexMode == IndexMode.TIME_SERIES) {
            return true;
        }

        // To allow the following patterns: profiling-metrics and profiling-events
        if (dataStreamName != null && dataStreamName.startsWith("profiling-")) {
            return true;
        }
        // To allow the following patterns: .profiling-sq-executables, .profiling-sq-leafframes and .profiling-stacktraces
        if (indexName.startsWith(".profiling-")) {
            return true;
        }
        // To allow the following patterns: metrics-apm.transaction.*, metrics-apm.service_transaction.*, metrics-apm.service_summary.*,
        // metrics-apm.service_destination.*, "metrics-apm.internal-* and metrics-apm.app.*
        if (dataStreamName != null && dataStreamName.startsWith("metrics-apm.")) {
            return true;
        }

        return false;
    }
}
