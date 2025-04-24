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
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_PATH;
import static org.elasticsearch.xpack.logsdb.LogsDBPlugin.CLUSTER_LOGSDB_ENABLED;

final class LogsdbIndexModeSettingsProvider implements IndexSettingProvider {
    private static final Logger LOGGER = LogManager.getLogger(LogsdbIndexModeSettingsProvider.class);
    static final String LOGS_PATTERN = "logs-*-*";
    private static final Set<String> MAPPING_INCLUDES = Set.of("_doc._source.*", "_doc.properties.host**", "_doc.subobjects");

    private final LogsdbLicenseService licenseService;
    private final SetOnce<CheckedFunction<IndexMetadata, MapperService, IOException>> mapperServiceFactory = new SetOnce<>();
    private final SetOnce<Supplier<IndexVersion>> createdIndexVersion = new SetOnce<>();
    private final SetOnce<Supplier<Version>> minNodeVersion = new SetOnce<>();
    private final SetOnce<Boolean> supportFallbackToStoredSource = new SetOnce<>();
    private final SetOnce<Boolean> supportFallbackLogsdbRouting = new SetOnce<>();

    private volatile boolean isLogsdbEnabled;

    LogsdbIndexModeSettingsProvider(LogsdbLicenseService licenseService, final Settings settings) {
        this.licenseService = licenseService;
        this.isLogsdbEnabled = CLUSTER_LOGSDB_ENABLED.get(settings);
    }

    void updateClusterIndexModeLogsdbEnabled(boolean isLogsdbEnabled) {
        this.isLogsdbEnabled = isLogsdbEnabled;
    }

    void init(
        CheckedFunction<IndexMetadata, MapperService, IOException> factory,
        Supplier<IndexVersion> indexVersion,
        Supplier<Version> minNodeVersion,
        boolean supportFallbackToStoredSource,
        boolean supportFallbackLogsdbRouting
    ) {
        this.mapperServiceFactory.set(factory);
        this.createdIndexVersion.set(indexVersion);
        this.minNodeVersion.set(minNodeVersion);
        this.supportFallbackToStoredSource.set(supportFallbackToStoredSource);
        this.supportFallbackLogsdbRouting.set(supportFallbackLogsdbRouting);
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
        final ProjectMetadata metadata,
        final Instant resolvedAt,
        Settings settings,
        final List<CompressedXContent> combinedTemplateMappings
    ) {
        Settings.Builder settingsBuilder = null;
        boolean isLogsDB = templateIndexMode == IndexMode.LOGSDB;
        // This index name is used when validating component and index templates, we should skip this check in that case.
        // (See MetadataIndexTemplateService#validateIndexTemplateV2(...) method)
        boolean isTemplateValidation = MetadataIndexTemplateService.VALIDATE_INDEX_NAME.equals(indexName);

        // Inject logsdb index mode, based on the logs pattern.
        if (isLogsdbEnabled
            && dataStreamName != null
            && resolveIndexMode(settings.get(IndexSettings.MODE.getKey())) == null
            && matchesLogsPattern(dataStreamName)) {
            settingsBuilder = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.getName());
            settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.getName()).put(settings).build();
            isLogsDB = true;
        }

        MappingHints mappingHints = getMappingHints(indexName, templateIndexMode, settings, combinedTemplateMappings);

        // Inject stored source mode if synthetic source if not available per licence.
        if (mappingHints.hasSyntheticSourceUsage
            && supportFallbackToStoredSource.get()
            && minNodeVersion.get().get().onOrAfter(Version.V_8_17_0)) {
            boolean legacyLicensedUsageOfSyntheticSourceAllowed = isLegacyLicensedUsageOfSyntheticSourceAllowed(
                templateIndexMode,
                indexName,
                dataStreamName
            );
            if (licenseService.fallbackToStoredSource(isTemplateValidation, legacyLicensedUsageOfSyntheticSourceAllowed)) {
                LOGGER.debug("creation of index [{}] with synthetic source without it being allowed", indexName);
                settingsBuilder = getBuilder(settingsBuilder).put(
                    IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(),
                    SourceFieldMapper.Mode.STORED.toString()
                );
            }
        }

        if (isLogsDB && minNodeVersion.get().get().onOrAfter(Version.V_8_18_0)) {
            // Inject sorting on [host.name], in addition to [@timestamp].
            if (mappingHints.sortOnHostName) {
                if (mappingHints.addHostNameField) {
                    // Inject keyword field [host.name] too.
                    settingsBuilder = getBuilder(settingsBuilder).put(IndexSettings.LOGSDB_ADD_HOST_NAME_FIELD.getKey(), true);
                }
                settingsBuilder = getBuilder(settingsBuilder).put(IndexSettings.LOGSDB_SORT_ON_HOST_NAME.getKey(), true);
            }

            // Inject routing path matching sort fields.
            if (settings.getAsBoolean(IndexSettings.LOGSDB_ROUTE_ON_SORT_FIELDS.getKey(), false)) {
                if (supportFallbackLogsdbRouting.get() == false || licenseService.allowLogsdbRoutingOnSortField(isTemplateValidation)) {
                    List<String> sortFields = new ArrayList<>(settings.getAsList(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey()));
                    sortFields.removeIf(s -> s.equals(DataStreamTimestampFieldMapper.DEFAULT_PATH));
                    if (sortFields.size() < 2) {
                        throw new IllegalStateException(
                            String.format(
                                Locale.ROOT,
                                "data stream [%s] in logsdb mode and with [%s] index setting has only %d sort fields "
                                    + "(excluding timestamp), needs at least 2",
                                dataStreamName,
                                IndexSettings.LOGSDB_ROUTE_ON_SORT_FIELDS.getKey(),
                                sortFields.size()
                            )
                        );
                    }
                    if (settings.hasValue(IndexMetadata.INDEX_ROUTING_PATH.getKey())) {
                        List<String> routingPaths = settings.getAsList(IndexMetadata.INDEX_ROUTING_PATH.getKey());
                        if (routingPaths.equals(sortFields) == false) {
                            throw new IllegalStateException(
                                String.format(
                                    Locale.ROOT,
                                    "data stream [%s] in logsdb mode and with [%s] index setting has mismatching sort "
                                        + "and routing fields, [index.routing_path:%s], [index.sort.fields:%s]",
                                    dataStreamName,
                                    IndexSettings.LOGSDB_ROUTE_ON_SORT_FIELDS.getKey(),
                                    routingPaths,
                                    sortFields
                                )
                            );
                        }
                    } else {
                        settingsBuilder = getBuilder(settingsBuilder).putList(INDEX_ROUTING_PATH.getKey(), sortFields);
                    }
                } else {
                    // Routing on sort fields is not allowed, reset the corresponding index setting.
                    LOGGER.debug("creation of index [{}] with logsdb mode and routing on sort fields without it being allowed", indexName);
                    settingsBuilder = getBuilder(settingsBuilder).put(IndexSettings.LOGSDB_ROUTE_ON_SORT_FIELDS.getKey(), false);
                }
            }
        }

        return settingsBuilder == null ? Settings.EMPTY : settingsBuilder.build();
    }

    record MappingHints(boolean hasSyntheticSourceUsage, boolean sortOnHostName, boolean addHostNameField) {
        static MappingHints EMPTY = new MappingHints(false, false, false);
    }

    private static boolean matchesLogsPattern(final String name) {
        return Regex.simpleMatch(LOGS_PATTERN, name);
    }

    private static IndexMode resolveIndexMode(final String mode) {
        return mode != null ? Enum.valueOf(IndexMode.class, mode.toUpperCase(Locale.ROOT)) : null;
    }

    // Returned value needs to be reassigned to the passed arg, to track the created builder.
    private static Settings.Builder getBuilder(Settings.Builder builder) {
        if (builder == null) {
            return Settings.builder();
        }
        return builder;
    }

    MappingHints getMappingHints(
        String indexName,
        IndexMode templateIndexMode,
        Settings indexTemplateAndCreateRequestSettings,
        List<CompressedXContent> combinedTemplateMappings
    ) {
        if (MetadataIndexTemplateService.VALIDATE_INDEX_NAME.equals(indexName)) {
            // This index name is used when validating component and index templates, we should skip this check in that case.
            // (See MetadataIndexTemplateService#validateIndexTemplateV2(...) method)
            return MappingHints.EMPTY;
        }

        try {
            var tmpIndexMetadata = buildIndexMetadataForMapperService(indexName, templateIndexMode, indexTemplateAndCreateRequestSettings);
            var indexMode = tmpIndexMetadata.getIndexMode();
            boolean hasSyntheticSourceUsage = false;
            if (IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.exists(tmpIndexMetadata.getSettings())
                || indexMode == IndexMode.LOGSDB
                || indexMode == IndexMode.TIME_SERIES) {
                // In case when index mode is tsdb or logsdb and only _source.mode mapping attribute is specified, then the default
                // could be wrong. However, it doesn't really matter, because if the _source.mode mapping attribute is set to stored,
                // then configuring the index.mapping.source.mode setting to stored has no effect. Additionally _source.mode can't be set
                // to disabled, because that isn't allowed with logsdb/tsdb. In other words setting index.mapping.source.mode setting to
                // stored when _source.mode mapping attribute is stored is fine as it has no effect, but avoids creating MapperService.
                var sourceMode = IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.get(tmpIndexMetadata.getSettings());
                hasSyntheticSourceUsage = sourceMode == SourceFieldMapper.Mode.SYNTHETIC;
                if (IndexSortConfig.INDEX_SORT_FIELD_SETTING.get(indexTemplateAndCreateRequestSettings).isEmpty() == false) {
                    // Custom sort config, no point for further checks on [host.name] field.
                    return new MappingHints(hasSyntheticSourceUsage, false, false);
                }
                if (IndexSettings.LOGSDB_SORT_ON_HOST_NAME.get(indexTemplateAndCreateRequestSettings)
                    && IndexSettings.LOGSDB_ADD_HOST_NAME_FIELD.get(indexTemplateAndCreateRequestSettings)) {
                    // Settings for adding and sorting on [host.name] are already set, propagate them.
                    return new MappingHints(hasSyntheticSourceUsage, true, true);
                }
            }

            try (var mapperService = mapperServiceFactory.get().apply(tmpIndexMetadata)) {
                // combinedTemplateMappings can be null when creating system indices
                // combinedTemplateMappings can be empty when creating a normal index that doesn't match any template and without mapping.
                if (combinedTemplateMappings == null || combinedTemplateMappings.isEmpty()) {
                    combinedTemplateMappings = List.of(new CompressedXContent("{}"));
                } else {
                    // Filter the mapping to contain only the part this index settings provider is interested in.
                    // This reduces the overhead of loading mappings, since mappings can be very large.
                    // The _doc._source.mode is needed to determine synthetic source usage.
                    // The _doc.properties.host* is needed to determine whether host.name field can be injected.
                    // The _doc.subobjects is needed to determine whether subobjects is enabled.
                    List<CompressedXContent> filteredMappings = new ArrayList<>(combinedTemplateMappings.size());
                    for (CompressedXContent mappingSource : combinedTemplateMappings) {
                        var ref = mappingSource.compressedReference();
                        var map = XContentHelper.convertToMap(ref, true, XContentType.JSON, MAPPING_INCLUDES, Set.of()).v2();
                        filteredMappings.add(new CompressedXContent(map));
                    }
                    combinedTemplateMappings = filteredMappings;
                }
                mapperService.merge(MapperService.SINGLE_MAPPING_NAME, combinedTemplateMappings, MapperService.MergeReason.INDEX_TEMPLATE);
                Mapper hostName = mapperService.mappingLookup().getMapper("host.name");
                hasSyntheticSourceUsage = hasSyntheticSourceUsage || mapperService.documentMapper().sourceMapper().isSynthetic();
                boolean addHostNameField = IndexSettings.LOGSDB_ADD_HOST_NAME_FIELD.get(indexTemplateAndCreateRequestSettings)
                    || (hostName == null
                        && mapperService.mappingLookup().objectMappers().get("host.name") == null
                        && (mapperService.mappingLookup().getMapper("host") == null
                            || mapperService.mappingLookup().getMapping().getRoot().subobjects() == ObjectMapper.Subobjects.DISABLED));
                boolean sortOnHostName = IndexSettings.LOGSDB_SORT_ON_HOST_NAME.get(indexTemplateAndCreateRequestSettings)
                    || addHostNameField
                    || (hostName instanceof NumberFieldMapper nfm && nfm.fieldType().hasDocValues())
                    || (hostName instanceof KeywordFieldMapper kfm && kfm.fieldType().hasDocValues());
                return new MappingHints(hasSyntheticSourceUsage, sortOnHostName, addHostNameField);
            }
        } catch (AssertionError | Exception e) {
            // In case invalid mappings or setting are provided, then mapper service creation can fail.
            // In that case it is ok to return false here. The index creation will fail anyway later, so no need to fallback to stored
            // source.
            LOGGER.warn(() -> Strings.format("unable to create mapper service for index [%s]", indexName), e);
            return MappingHints.EMPTY;
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
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexSettings.INDEX_FAST_REFRESH_SETTING.getKey(), false);  // Avoid warnings for non-system indexes.

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
