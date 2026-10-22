/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.PassThroughObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_PATH;

/**
 * An {@link IndexSettingProvider} implementation that adds the index.time_series.start_time,
 * index.time_series.end_time and index.routing_path index settings to backing indices of
 * data streams in time series index mode.
 */
public class TimeSeriesIndexSettingsProvider implements IndexSettingProvider {

    static final DateFormatter FORMATTER = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;

    private static final Set<String> AFFECTED_SETTINGS = Set.of(
        IndexSettings.TIME_SERIES_START_TIME.getKey(),
        IndexSettings.TIME_SERIES_END_TIME.getKey(),
        INDEX_ROUTING_PATH.getKey()
    );

    private final CheckedFunction<IndexMetadata, MapperService, IOException> mapperServiceFactory;

    TimeSeriesIndexSettingsProvider(CheckedFunction<IndexMetadata, MapperService, IOException> mapperServiceFactory) {
        this.mapperServiceFactory = mapperServiceFactory;
    }

    @Override
    public Set<String> affectedSettings() {
        return AFFECTED_SETTINGS;
    }

    @Override
    public Settings getAdditionalIndexSettings(
        String indexName,
        @Nullable String dataStreamName,
        @Nullable IndexMode templateIndexMode,
        Metadata metadata,
        Instant resolvedAt,
        Settings indexTemplateAndCreateRequestSettings,
        List<CompressedXContent> combinedTemplateMappings
    ) {
        // If this is not a data stream or the index mode is not going to be time_series, we skip
        if (dataStreamName == null || templateIndexMode != IndexMode.TIME_SERIES) {
            return Settings.EMPTY;
        }
        DataStream dataStream = metadata.dataStreams().get(dataStreamName);
        IndexMode dataStreamIndexMode = dataStream == null ? null : dataStream.getIndexMode();
        // If this is the first time series index for this data stream, either because the data stream was
        // just created or because the data stream was not a time series data stream before.
        boolean validFirstTimeSeriesIndex = dataStreamIndexMode == null || dataStreamIndexMode == IndexMode.STANDARD;
        if (validFirstTimeSeriesIndex == false && dataStreamIndexMode != IndexMode.TIME_SERIES) {
            // This is an invalid migration to TSDB, we do not fail here, but we rely on the rollover to reject this.
            return Settings.EMPTY;
        }
        Settings.Builder builder = Settings.builder();
        TimeValue lookAheadTime = DataStreamsPlugin.getLookAheadTime(indexTemplateAndCreateRequestSettings);
        TimeValue lookBackTime = DataStreamsPlugin.LOOK_BACK_TIME.get(indexTemplateAndCreateRequestSettings);
        final Instant start;
        final Instant end;
        // Determine the start and end time either based on resolvedAt or the previous time series backing index.
        if (validFirstTimeSeriesIndex) {
            start = DataStream.getCanonicalTimestampBound(resolvedAt.minusMillis(lookBackTime.getMillis()));
            end = DataStream.getCanonicalTimestampBound(resolvedAt.plusMillis(lookAheadTime.getMillis()));
        } else {
            IndexMetadata currentLatestBackingIndex = metadata.index(dataStream.getWriteIndex());
            if (currentLatestBackingIndex.getSettings().hasValue(IndexSettings.TIME_SERIES_END_TIME.getKey()) == false) {
                throw new IllegalStateException(
                    String.format(
                        Locale.ROOT,
                        "backing index [%s] in tsdb mode doesn't have the [%s] index setting",
                        currentLatestBackingIndex.getIndex().getName(),
                        IndexSettings.TIME_SERIES_END_TIME.getKey()
                    )
                );
            }
            start = IndexSettings.TIME_SERIES_END_TIME.get(currentLatestBackingIndex.getSettings());
            if (start.isAfter(resolvedAt)) {
                end = DataStream.getCanonicalTimestampBound(start.plusMillis(lookAheadTime.getMillis()));
            } else {
                end = DataStream.getCanonicalTimestampBound(resolvedAt.plusMillis(lookAheadTime.getMillis()));
            }
        }
        assert start.isBefore(end) : "data stream backing index's start time is not before end time";
        builder.put(IndexSettings.TIME_SERIES_START_TIME.getKey(), FORMATTER.format(start));
        builder.put(IndexSettings.TIME_SERIES_END_TIME.getKey(), FORMATTER.format(end));

        if (indexTemplateAndCreateRequestSettings.hasValue(IndexMetadata.INDEX_ROUTING_PATH.getKey()) == false
            && combinedTemplateMappings.isEmpty() == false) {
            List<String> routingPaths = findRoutingPaths(indexName, indexTemplateAndCreateRequestSettings, combinedTemplateMappings);
            if (routingPaths.isEmpty() == false) {
                builder.putList(INDEX_ROUTING_PATH.getKey(), routingPaths);
            }
        }
        return builder.build();
    }

    /**
     * Find fields in mapping that are of type keyword and time_series_dimension enabled.
     * Using MapperService here has an overhead, but allows the mappings from template to
     * be merged correctly and fetching the fields without manually parsing the mappings.
     *
     * Alternatively this method can instead parse mappings into map of maps and merge that and
     * iterate over all values to find the field that can serve as routing value. But this requires
     * mapping specific logic to exist here.
     */
    private List<String> findRoutingPaths(String indexName, Settings allSettings, List<CompressedXContent> combinedTemplateMappings) {
        var tmpIndexMetadata = IndexMetadata.builder(indexName);

        int dummyPartitionSize = IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING.get(allSettings);
        int dummyShards = allSettings.getAsInt(
            IndexMetadata.SETTING_NUMBER_OF_SHARDS,
            dummyPartitionSize == 1 ? 1 : dummyPartitionSize + 1
        );
        int shardReplicas = allSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
        var finalResolvedSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(allSettings)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, dummyShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, shardReplicas)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            // Avoid failing because index.routing_path is missing
            .putList(INDEX_ROUTING_PATH.getKey(), List.of("path"))
            .build();

        tmpIndexMetadata.settings(finalResolvedSettings);
        // Create MapperService just to extract keyword dimension fields:
        try (var mapperService = mapperServiceFactory.apply(tmpIndexMetadata.build())) {
            mapperService.merge(MapperService.SINGLE_MAPPING_NAME, combinedTemplateMappings, MapperService.MergeReason.INDEX_TEMPLATE);
            List<String> routingPaths = new ArrayList<>();
            for (var fieldMapper : mapperService.documentMapper().mappers().fieldMappers()) {
                extractPath(routingPaths, fieldMapper);
            }
            for (var objectMapper : mapperService.documentMapper().mappers().objectMappers().values()) {
                if (objectMapper instanceof PassThroughObjectMapper passThroughObjectMapper) {
                    if (passThroughObjectMapper.containsDimensions()) {
                        routingPaths.add(passThroughObjectMapper.fullPath() + ".*");
                    }
                }
            }
            for (var template : mapperService.getAllDynamicTemplates()) {
                if (template.pathMatch().isEmpty()) {
                    continue;
                }

                var templateName = "__dynamic__" + template.name();
                var mappingSnippet = template.mappingForName(templateName, KeywordFieldMapper.CONTENT_TYPE);
                String mappingSnippetType = (String) mappingSnippet.get("type");
                if (mappingSnippetType == null) {
                    continue;
                }

                MappingParserContext parserContext = mapperService.parserContext();
                for (Iterator<String> iterator = template.pathMatch().iterator(); iterator.hasNext();) {
                    var mapper = parserContext.typeParser(mappingSnippetType)
                        .parse(iterator.next(), mappingSnippet, parserContext)
                        .build(MapperBuilderContext.root(false, false));
                    extractPath(routingPaths, mapper);
                    if (iterator.hasNext()) {
                        // Since FieldMapper.parse modifies the Map passed in (removing entries for "type"), that means
                        // that only the first pathMatch passed in gets recognized as a time_series_dimension.
                        // To avoid this, each parsing call uses a new mapping snippet.
                        // Note that a shallow copy of the mappingSnippet map is not enough if there are multi-fields.
                        mappingSnippet = template.mappingForName(templateName, KeywordFieldMapper.CONTENT_TYPE);
                    }
                }
            }
            return routingPaths;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Helper method that adds the name of the mapper to the provided list if it is a keyword dimension field.
     */
    private static void extractPath(List<String> routingPaths, Mapper mapper) {
        if (mapper instanceof KeywordFieldMapper keywordFieldMapper) {
            if (keywordFieldMapper.fieldType().isDimension()) {
                routingPaths.add(mapper.fullPath());
            }
        }
    }

}
