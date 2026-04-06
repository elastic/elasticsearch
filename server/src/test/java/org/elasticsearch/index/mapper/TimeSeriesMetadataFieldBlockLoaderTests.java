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

public class TimeSeriesMetadataFieldBlockLoaderTests extends MapperServiceTestCase {

    private static final Settings TSDB_SETTINGS = Settings.builder()
        .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
        .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "host")
        .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
        .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
        .build();

    private static final String MAPPING = """
        {
          "_doc": {
            "properties": {
              "@timestamp": { "type": "date" },
              "host": { "type": "keyword", "time_series_dimension": true },
              "env": { "type": "keyword", "time_series_dimension": true },
              "region": { "type": "keyword", "time_series_dimension": true },
              "cpu": { "type": "double", "time_series_metric": "gauge" },
              "request_count": { "type": "long", "time_series_metric": "counter" },
              "message": { "type": "keyword" }
            }
          }
        }
        """;

    public void testDimensionsOnly() throws IOException {
        BlockLoader loader = createBlockLoader(new BlockLoaderFunctionConfig.TimeSeriesMetadata(false, Set.of()));
        assertThat(loader, instanceOf(TimeSeriesMetadataFieldBlockLoader.class));
        assertThat(sourcePaths(loader), equalTo(Set.of("host", "env", "region")));
    }

    public void testDimensionsAndMetrics() throws IOException {
        BlockLoader loader = createBlockLoader(new BlockLoaderFunctionConfig.TimeSeriesMetadata(true, Set.of()));
        assertThat(loader, instanceOf(TimeSeriesMetadataFieldBlockLoader.class));
        assertThat(sourcePaths(loader), equalTo(Set.of("host", "env", "region", "cpu", "request_count")));
    }

    public void testExcludedDimensions() throws IOException {
        BlockLoader loader = createBlockLoader(new BlockLoaderFunctionConfig.TimeSeriesMetadata(false, Set.of("host", "region")));
        assertThat(loader, instanceOf(TimeSeriesMetadataFieldBlockLoader.class));
        assertThat(sourcePaths(loader), equalTo(Set.of("env")));
    }

    public void testExcludedDimensionsWithMetrics() throws IOException {
        BlockLoader loader = createBlockLoader(new BlockLoaderFunctionConfig.TimeSeriesMetadata(true, Set.of("env")));
        assertThat(loader, instanceOf(TimeSeriesMetadataFieldBlockLoader.class));
        assertThat(sourcePaths(loader), equalTo(Set.of("host", "region", "cpu", "request_count")));
    }

    public void testNoConfigReturnSourceBlockLoader() throws IOException {
        MapperService mapperService = createMapperService(TSDB_SETTINGS, MAPPING);
        BlockLoader loader = mapperService.documentMapper()
            .sourceMapper()
            .fieldType()
            .blockLoader(new TestBlockLoaderContext(mapperService, null));
        assertThat(loader, instanceOf(SourceFieldBlockLoader.class));
    }

    private BlockLoader createBlockLoader(BlockLoaderFunctionConfig config) throws IOException {
        MapperService mapperService = createMapperService(TSDB_SETTINGS, MAPPING);
        return mapperService.documentMapper().sourceMapper().fieldType().blockLoader(new TestBlockLoaderContext(mapperService, config));
    }

    private static Set<String> sourcePaths(BlockLoader loader) {
        var spec = loader.rowStrideStoredFieldSpec();
        return spec.requiresSource() ? spec.sourcePaths() : Set.of();
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
