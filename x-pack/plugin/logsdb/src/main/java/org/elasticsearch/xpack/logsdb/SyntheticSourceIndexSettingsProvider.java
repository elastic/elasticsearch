/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.compress.CompressedXContent;
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
import java.util.function.Supplier;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_PATH;

/**
 * An index setting provider that overwrites the source mode from synthetic to stored if synthetic source isn't allowed to be used.
 */
final class SyntheticSourceIndexSettingsProvider implements IndexSettingProvider {

    private static final Logger LOGGER = LogManager.getLogger(SyntheticSourceIndexSettingsProvider.class);

    private final SyntheticSourceLicenseService syntheticSourceLicenseService;
    private final CheckedFunction<IndexMetadata, MapperService, IOException> mapperServiceFactory;
    private final LogsdbIndexModeSettingsProvider logsdbIndexModeSettingsProvider;
    private final Supplier<IndexVersion> createdIndexVersion;

    SyntheticSourceIndexSettingsProvider(
        SyntheticSourceLicenseService syntheticSourceLicenseService,
        CheckedFunction<IndexMetadata, MapperService, IOException> mapperServiceFactory,
        LogsdbIndexModeSettingsProvider logsdbIndexModeSettingsProvider,
        Supplier<IndexVersion> createdIndexVersion
    ) {
        this.syntheticSourceLicenseService = syntheticSourceLicenseService;
        this.mapperServiceFactory = mapperServiceFactory;
        this.logsdbIndexModeSettingsProvider = logsdbIndexModeSettingsProvider;
        this.createdIndexVersion = createdIndexVersion;
    }

    @Override
    public boolean overrulesTemplateAndRequestSettings() {
        // Indicates that the provider value takes precedence over any user setting.
        return true;
    }

    @Override
    public Settings getAdditionalIndexSettings(
        String indexName,
        String dataStreamName,
        IndexMode templateIndexMode,
        Metadata metadata,
        Instant resolvedAt,
        Settings indexTemplateAndCreateRequestSettings,
        List<CompressedXContent> combinedTemplateMappings
    ) {
        var logsdbSettings = logsdbIndexModeSettingsProvider.getLogsdbModeSetting(dataStreamName, indexTemplateAndCreateRequestSettings);
        if (logsdbSettings != Settings.EMPTY) {
            indexTemplateAndCreateRequestSettings = Settings.builder()
                .put(logsdbSettings)
                .put(indexTemplateAndCreateRequestSettings)
                .build();
        }

        // This index name is used when validating component and index templates, we should skip this check in that case.
        // (See MetadataIndexTemplateService#validateIndexTemplateV2(...) method)
        boolean isTemplateValidation = "validate-index-name".equals(indexName);
        boolean legacyLicensedUsageOfSyntheticSourceAllowed = isLegacyLicensedUsageOfSyntheticSourceAllowed(
            templateIndexMode,
            indexName,
            dataStreamName
        );
        if (newIndexHasSyntheticSourceUsage(indexName, templateIndexMode, indexTemplateAndCreateRequestSettings, combinedTemplateMappings)
            && syntheticSourceLicenseService.fallbackToStoredSource(isTemplateValidation, legacyLicensedUsageOfSyntheticSourceAllowed)) {
            LOGGER.debug("creation of index [{}] with synthetic source without it being allowed", indexName);
            return Settings.builder()
                .put(SourceFieldMapper.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.STORED.toString())
                .build();
        }
        return Settings.EMPTY;
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
            try (var mapperService = mapperServiceFactory.apply(tmpIndexMetadata)) {
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
            .put(IndexMetadata.SETTING_VERSION_CREATED, createdIndexVersion.get())
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
    boolean isLegacyLicensedUsageOfSyntheticSourceAllowed(IndexMode templateIndexMode, String indexName, String dataStreamName) {
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
