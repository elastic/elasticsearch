/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class TimeSeriesMetadataFieldBlockLoaderTests extends MapperServiceTestCase {

    public void testBlockLoaderWithTimeSeriesDimensionsFunction() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "host,cluster")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
            .build();

        String mapping = """
            {
              "_doc": {
                "properties": {
                  "@timestamp": { "type": "date" },
                  "host": { "type": "keyword", "time_series_dimension": true },
                  "cluster": { "type": "keyword", "time_series_dimension": true },
                  "metric": { "type": "long", "time_series_metric": "gauge" },
                  "message": { "type": "keyword" },
                  "status_code": { "type": "integer" },
                  "tags": { "type": "keyword" }
                }
              }
            }
            """;

        MapperService mapperService = createMapperService(settings, mapping);
        SourceFieldMapper sourceMapper = mapperService.documentMapper().sourceMapper();
        assertThat(sourceMapper.enabled(), equalTo(true));

        BlockLoaderFunctionConfig config = new BlockLoaderFunctionConfig.JustFunction(
            BlockLoaderFunctionConfig.Function.TIME_SERIES_DIMENSIONS
        );

        BlockLoader blockLoader = sourceMapper.fieldType().blockLoader(createBlockLoaderContext(mapperService, config));

        assertThat(blockLoader, notNullValue());
        assertThat(blockLoader, instanceOf(TimeSeriesMetadataFieldBlockLoader.class));
    }

    public void testBlockLoaderWithoutTimeSeriesDimensionsFunction() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "host")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
            .build();

        String mapping = """
            {
              "_doc": {
                "properties": {
                  "@timestamp": { "type": "date" },
                  "host": { "type": "keyword", "time_series_dimension": true },
                  "metric": { "type": "long", "time_series_metric": "gauge" },
                  "message": { "type": "keyword" },
                  "status_code": { "type": "integer" },
                  "tags": { "type": "keyword" }
                }
              }
            }
            """;

        MapperService mapperService = createMapperService(settings, mapping);
        SourceFieldMapper sourceMapper = mapperService.documentMapper().sourceMapper();

        // Without TIME_SERIES_DIMENSIONS function, should return SourceFieldBlockLoader
        BlockLoader blockLoader = sourceMapper.fieldType().blockLoader(createBlockLoaderContext(mapperService, null));

        assertThat(blockLoader, notNullValue());
        assertThat(blockLoader, instanceOf(SourceFieldBlockLoader.class));
    }

    public void testBlockLoaderWithTimeSeriesMetricsAndDimensionsFunction() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "host,cluster")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
            .build();

        String mapping = """
            {
              "_doc": {
                "properties": {
                  "@timestamp": { "type": "date" },
                  "host": { "type": "keyword", "time_series_dimension": true },
                  "cluster": { "type": "keyword", "time_series_dimension": true },
                  "cpu": { "type": "double", "time_series_metric": "gauge" },
                  "request_count": { "type": "long", "time_series_metric": "counter" },
                  "message": { "type": "keyword" },
                  "status_code": { "type": "integer" },
                  "tags": { "type": "keyword" }
                }
              }
            }
            """;

        MapperService mapperService = createMapperService(settings, mapping);
        SourceFieldMapper sourceMapper = mapperService.documentMapper().sourceMapper();
        assertThat(sourceMapper.enabled(), equalTo(true));

        BlockLoaderFunctionConfig config = new BlockLoaderFunctionConfig.JustFunction(
            BlockLoaderFunctionConfig.Function.TIME_SERIES_METRICS_AND_DIMENSIONS
        );

        BlockLoader blockLoader = sourceMapper.fieldType().blockLoader(createBlockLoaderContext(mapperService, config));

        assertThat(blockLoader, notNullValue());
        assertThat(blockLoader, instanceOf(TimeSeriesMetadataFieldBlockLoader.class));
    }

    public void testBlockLoaderWithTimeSeriesDimensionsFunctionOnlyLoadsDimensions() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "host")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
            .build();

        String mapping = """
            {
              "_doc": {
                "properties": {
                  "@timestamp": { "type": "date" },
                  "host": { "type": "keyword", "time_series_dimension": true },
                  "env": { "type": "keyword", "time_series_dimension": true },
                  "cpu": { "type": "double", "time_series_metric": "gauge" },
                  "request_count": { "type": "long", "time_series_metric": "counter" },
                  "message": { "type": "keyword" },
                  "status_code": { "type": "integer" }
                }
              }
            }
            """;

        MapperService mapperService = createMapperService(settings, mapping);
        SourceFieldMapper sourceMapper = mapperService.documentMapper().sourceMapper();

        BlockLoaderFunctionConfig config = new BlockLoaderFunctionConfig.JustFunction(
            BlockLoaderFunctionConfig.Function.TIME_SERIES_DIMENSIONS
        );

        BlockLoader blockLoader = sourceMapper.fieldType().blockLoader(createBlockLoaderContext(mapperService, config));

        assertThat(blockLoader, instanceOf(TimeSeriesMetadataFieldBlockLoader.class));

        var storedFieldsSpec = blockLoader.rowStrideStoredFieldSpec();
        Set<String> requiredFields = storedFieldsSpec.requiresSource() ? storedFieldsSpec.sourcePaths() : Set.of();
        assertThat(requiredFields, equalTo(Set.of("host", "env")));
    }

    public void testBlockLoaderWithMetricsAndDimensionsFunctionLoadsBoth() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "host")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
            .build();

        String mapping = """
            {
              "_doc": {
                "properties": {
                  "@timestamp": { "type": "date" },
                  "host": { "type": "keyword", "time_series_dimension": true },
                  "env": { "type": "keyword", "time_series_dimension": true },
                  "cpu": { "type": "double", "time_series_metric": "gauge" },
                  "request_count": { "type": "long", "time_series_metric": "counter" },
                  "message": { "type": "keyword" },
                  "status_code": { "type": "integer" },
                  "tags": { "type": "keyword" }
                }
              }
            }
            """;

        MapperService mapperService = createMapperService(settings, mapping);
        SourceFieldMapper sourceMapper = mapperService.documentMapper().sourceMapper();

        BlockLoaderFunctionConfig config = new BlockLoaderFunctionConfig.JustFunction(
            BlockLoaderFunctionConfig.Function.TIME_SERIES_METRICS_AND_DIMENSIONS
        );

        BlockLoader blockLoader = sourceMapper.fieldType().blockLoader(createBlockLoaderContext(mapperService, config));

        assertThat(blockLoader, instanceOf(TimeSeriesMetadataFieldBlockLoader.class));

        // Should only include dimensions and metrics, not regular fields (message, status_code, tags)
        var storedFieldsSpec = blockLoader.rowStrideStoredFieldSpec();
        Set<String> requiredFields = storedFieldsSpec.requiresSource() ? storedFieldsSpec.sourcePaths() : Set.of();
        assertThat(requiredFields, equalTo(Set.of("host", "env", "cpu", "request_count")));
    }

    private MappedFieldType.BlockLoaderContext createBlockLoaderContext(MapperService mapperService, BlockLoaderFunctionConfig config) {
        return new MappedFieldType.BlockLoaderContext() {
            @Override
            public String indexName() {
                return mapperService.getIndexSettings().getIndex().getName();
            }

            @Override
            public IndexSettings indexSettings() {
                return mapperService.getIndexSettings();
            }

            @Override
            public MappedFieldType.FieldExtractPreference fieldExtractPreference() {
                return MappedFieldType.FieldExtractPreference.NONE;
            }

            @Override
            public SearchLookup lookup() {
                return new SearchLookup(mapperService.mappingLookup().fieldTypesLookup()::get, null, null);
            }

            @Override
            public Set<String> sourcePaths(String name) {
                return mapperService.mappingLookup().sourcePaths(name);
            }

            @Override
            public String parentField(String field) {
                return mapperService.mappingLookup().parentField(field);
            }

            @Override
            public FieldNamesFieldMapper.FieldNamesFieldType fieldNames() {
                return (FieldNamesFieldMapper.FieldNamesFieldType) mapperService.fieldType(FieldNamesFieldMapper.NAME);
            }

            @Override
            public BlockLoaderFunctionConfig blockLoaderFunctionConfig() {
                return config;
            }

            @Override
            public MappingLookup mappingLookup() {
                return mapperService.mappingLookup();
            }
        };
    }
}
