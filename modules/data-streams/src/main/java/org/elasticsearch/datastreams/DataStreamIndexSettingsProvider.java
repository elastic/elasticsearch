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
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.regex.Regex;
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
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.PassThroughObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_DIMENSIONS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_PATH;

/**
 * An {@link IndexSettingProvider} implementation that adds the index.time_series.start_time,
 * index.time_series.end_time and index.routing_path index settings to backing indices of
 * data streams in time series index mode.
 */
public class DataStreamIndexSettingsProvider implements IndexSettingProvider {

    static final DateFormatter FORMATTER = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;

    private final CheckedFunction<IndexMetadata, MapperService, IOException> mapperServiceFactory;

    DataStreamIndexSettingsProvider(CheckedFunction<IndexMetadata, MapperService, IOException> mapperServiceFactory) {
        this.mapperServiceFactory = mapperServiceFactory;
    }

    @Override
    public void provideAdditionalMetadata(
        String indexName,
        @Nullable String dataStreamName,
        @Nullable IndexMode templateIndexMode,
        ProjectMetadata projectMetadata,
        Instant resolvedAt,
        Settings indexTemplateAndCreateRequestSettings,
        List<CompressedXContent> combinedTemplateMappings,
        Settings.Builder additionalSettings,
        BiConsumer<String, Map<String, String>> additionalCustomMetadata
    ) {
        if (dataStreamName != null) {
            DataStream dataStream = projectMetadata.dataStreams().get(dataStreamName);
            // First backing index is created and then data stream is rolled over (in a single cluster state update).
            // So at this point we can't check index_mode==time_series,
            // so checking that index_mode==null|standard and templateIndexMode == TIME_SERIES
            boolean isMigratingToTimeSeries = templateIndexMode == IndexMode.TIME_SERIES;
            boolean migrating = dataStream != null
                && (dataStream.getIndexMode() == null || dataStream.getIndexMode() == IndexMode.STANDARD)
                && isMigratingToTimeSeries;
            IndexMode indexMode;
            if (migrating) {
                indexMode = IndexMode.TIME_SERIES;
            } else if (dataStream != null) {
                indexMode = isMigratingToTimeSeries ? dataStream.getIndexMode() : null;
            } else if (isMigratingToTimeSeries) {
                indexMode = IndexMode.TIME_SERIES;
            } else {
                indexMode = null;
            }
            if (indexMode != null) {
                if (indexMode == IndexMode.TIME_SERIES) {
                    TimeValue lookAheadTime = DataStreamsPlugin.getLookAheadTime(indexTemplateAndCreateRequestSettings);
                    TimeValue lookBackTime = DataStreamsPlugin.LOOK_BACK_TIME.get(indexTemplateAndCreateRequestSettings);
                    final Instant start;
                    final Instant end;
                    if (dataStream == null || migrating) {
                        start = DataStream.getCanonicalTimestampBound(resolvedAt.minusMillis(lookBackTime.getMillis()));
                        end = DataStream.getCanonicalTimestampBound(resolvedAt.plusMillis(lookAheadTime.getMillis()));
                    } else {
                        IndexMetadata currentLatestBackingIndex = projectMetadata.index(dataStream.getWriteIndex());
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
                    additionalSettings.put(IndexSettings.TIME_SERIES_START_TIME.getKey(), FORMATTER.format(start));
                    additionalSettings.put(IndexSettings.TIME_SERIES_END_TIME.getKey(), FORMATTER.format(end));

                    if (indexTemplateAndCreateRequestSettings.hasValue(IndexMetadata.INDEX_ROUTING_PATH.getKey()) == false
                        && combinedTemplateMappings.isEmpty() == false) {
                        List<String> dimensions = new ArrayList<>();
                        boolean matchesAllDimensions = findDimensionFields(
                            indexName,
                            indexTemplateAndCreateRequestSettings,
                            combinedTemplateMappings,
                            dimensions
                        );
                        if (dimensions.isEmpty() == false) {
                            if (matchesAllDimensions) {
                                // Only set index.dimensions if the paths in the dimensions list match all potential dimension fields.
                                // This is not the case e.g. if a dynamic template matches by match_mapping_type instead of path_match
                                additionalSettings.putList(INDEX_DIMENSIONS.getKey(), dimensions);
                            }
                            // always populate index.routing_path, so that routing works for older index versions
                            // this applies to indices created during a rolling upgrade
                            additionalSettings.putList(INDEX_ROUTING_PATH.getKey(), dimensions);
                        }
                    }
                }
            }
        }
    }

    /**
     * This is called when mappings are updated, so that the {@link IndexMetadata#getTimeSeriesDimensions()}
     * and {@link IndexMetadata#INDEX_ROUTING_PATH} settings are updated to match the new mappings.
     * Updates {@link IndexMetadata#getTimeSeriesDimensions} if a new dimension field is added to the mappings,
     * or sets {@link IndexMetadata#INDEX_ROUTING_PATH} if a new dimension field is added that doesn't allow for matching all
     * dimension fields via a wildcard pattern.
     */
    @Override
    public void onUpdateMappings(
        IndexMetadata indexMetadata,
        DocumentMapper documentMapper,
        Settings.Builder additionalSettings,
        BiConsumer<String, Map<String, String>> additionalCustomMetadata
    ) {
        List<String> indexDimensions = indexMetadata.getTimeSeriesDimensions();
        if (indexDimensions.isEmpty()) {
            return;
        }
        assert indexMetadata.getIndexMode() == IndexMode.TIME_SERIES;
        List<String> newIndexDimensions = new ArrayList<>(indexDimensions.size());
        boolean matchesAllDimensions = findDimensionFields(newIndexDimensions, documentMapper);
        if (indexDimensions.size() == newIndexDimensions.size()
            && new HashSet<>(indexDimensions).equals(new HashSet<>(newIndexDimensions))) {
            return;
        }
        if (matchesAllDimensions) {
            additionalSettings.putList(INDEX_DIMENSIONS.getKey(), newIndexDimensions);
        } else {
            // If the new dimensions don't match all potential dimension fields, we need to unset index.dimensions
            // so that index.routing_path is used instead.
            // This can happen if a new dynamic template is added to an existing index that matches by mapping type instead of path_match.
            additionalSettings.putList(INDEX_DIMENSIONS.getKey(), List.of());
        }
    }

    /**
     * Find fields in mapping that are time_series_dimension enabled.
     * Using MapperService here has an overhead, but allows the mappings from template to
     * be merged correctly and fetching the fields without manually parsing the mappings.
     * <p>
     * Alternatively this method can instead parse mappings into map of maps and merge that and
     * iterate over all values to find the field that can serve as routing value. But this requires
     * mapping specific logic to exist here.
     *
     * @param indexName the name of the index for which the dimension fields are being found
     * @param allSettings the settings of the index
     * @param combinedTemplateMappings the combined mappings from index templates
     *                                 (if any) that are applied to the index
     * @param dimensions a list to which the found dimension fields will be added
     * @return true if all potential dimension fields can be matched via the dimensions in the list, false otherwise
     */
    private boolean findDimensionFields(
        String indexName,
        Settings allSettings,
        List<CompressedXContent> combinedTemplateMappings,
        List<String> dimensions
    ) {
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
            DocumentMapper documentMapper = mapperService.documentMapper();
            return findDimensionFields(dimensions, documentMapper);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Finds the dimension fields in the provided document mapper and adds them to the provided list.
     *
     * @param dimensions the list to which the found dimension fields will be added
     * @param documentMapper the document mapper from which to extract the dimension fields
     * @return true if all potential dimension fields can be matched via the dimensions in the list, false otherwise
     */
    private static boolean findDimensionFields(List<String> dimensions, DocumentMapper documentMapper) {
        for (var objectMapper : documentMapper.mappers().objectMappers().values()) {
            if (objectMapper instanceof PassThroughObjectMapper passThroughObjectMapper) {
                if (passThroughObjectMapper.containsDimensions()) {
                    dimensions.add(passThroughObjectMapper.fullPath() + ".*");
                }
            }
        }
        boolean matchesAllDimensions = true;
        for (var template : documentMapper.mapping().getRoot().dynamicTemplates()) {
            if (template.isTimeSeriesDimension() == false) {
                continue;
            }
            if (template.isSimplePathMatch() == false) {
                // If the template is not using a simple path match, the dimensions list can't match all potential dimensions.
                // For example, if the dynamic template matches by mapping type (all strings are mapped as dimensions),
                // the coordinating node can't rely on the dimensions list to match all dimensions.
                // In this case, the index.routing_path setting will be used instead.
                matchesAllDimensions = false;
            }
            if (template.pathMatch().isEmpty() == false) {
                dimensions.addAll(template.pathMatch());
            }
        }

        for (var fieldMapper : documentMapper.mappers().fieldMappers()) {
            extractPath(dimensions, fieldMapper);
        }
        return matchesAllDimensions;
    }

    /**
     * Helper method that adds the name of the mapper to the provided list.
     */
    private static void extractPath(List<String> dimensions, Mapper mapper) {
        if (mapper instanceof FieldMapper fieldMapper && fieldMapper.fieldType().isDimension()) {
            String path = mapper.fullPath();
            // don't add if the path already matches via a wildcard pattern in the list
            // e.g. if "path.*" is already added, "path.foo" should not be added
            if (Regex.simpleMatch(dimensions, path) == false) {
                dimensions.add(path);
            }
        }
    }

}
