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

        BlockLoaderFunctionConfig config = new BlockLoaderFunctionConfig.TimeSeriesMetadata(false, Set.of());

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

        BlockLoaderFunctionConfig config = new BlockLoaderFunctionConfig.TimeSeriesMetadata(true, Set.of());

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

        BlockLoaderFunctionConfig config = new BlockLoaderFunctionConfig.TimeSeriesMetadata(false, Set.of());

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

        BlockLoaderFunctionConfig config = new BlockLoaderFunctionConfig.TimeSeriesMetadata(true, Set.of());

        BlockLoader blockLoader = sourceMapper.fieldType().blockLoader(createBlockLoaderContext(mapperService, config));

        assertThat(blockLoader, instanceOf(TimeSeriesMetadataFieldBlockLoader.class));
        var storedFieldsSpec = blockLoader.rowStrideStoredFieldSpec();
        Set<String> requiredFields = storedFieldsSpec.requiresSource() ? storedFieldsSpec.sourcePaths() : Set.of();
        assertThat(requiredFields, equalTo(Set.of("host", "env", "cpu", "request_count")));
    }

    public void testBlockLoaderWithExcludedDimensions() throws IOException {
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
                  "region": { "type": "keyword", "time_series_dimension": true },
                  "cpu": { "type": "double", "time_series_metric": "gauge" }
                }
              }
            }
            """;

        MapperService mapperService = createMapperService(settings, mapping);
        SourceFieldMapper sourceMapper = mapperService.documentMapper().sourceMapper();

        BlockLoaderFunctionConfig config = new BlockLoaderFunctionConfig.TimeSeriesMetadata(false, Set.of("host"));

        BlockLoader blockLoader = sourceMapper.fieldType().blockLoader(createBlockLoaderContext(mapperService, config));

        assertThat(blockLoader, instanceOf(TimeSeriesMetadataFieldBlockLoader.class));

        var storedFieldsSpec = blockLoader.rowStrideStoredFieldSpec();
        Set<String> requiredFields = storedFieldsSpec.requiresSource() ? storedFieldsSpec.sourcePaths() : Set.of();
        assertThat("excluded dimension 'host' should not be in required fields", requiredFields, equalTo(Set.of("env", "region")));
    }

    private MappedFieldType.BlockLoaderContext createBlockLoaderContext(MapperService mapperService, BlockLoaderFunctionConfig config) {
        return new TestBlockLoaderContext(mapperService, config);
    }

    private static class TestBlockLoaderContext extends DummyBlockLoaderContext.MapperServiceBlockLoaderContext {
        private final BlockLoaderFunctionConfig config;

        private TestBlockLoaderContext(MapperService mapperService, BlockLoaderFunctionConfig config) {
            super(mapperService);
            this.config = config;
        }

        @Override
        public BlockLoaderFunctionConfig blockLoaderFunctionConfig() {
            return config;
        }
    }
}
