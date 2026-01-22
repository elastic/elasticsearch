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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class SourceFieldMapperBlockLoaderTests extends MapperServiceTestCase {

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
                  "metric": { "type": "long", "time_series_metric": "gauge" }
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

        BlockLoader blockLoader = sourceMapper.fieldType()
            .blockLoader(new DummyBlockLoaderContext.MapperServiceBlockLoaderContext(mapperService) {
                @Override
                public BlockLoaderFunctionConfig blockLoaderFunctionConfig() {
                    return config;
                }
            });

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
                  "metric": { "type": "long", "time_series_metric": "gauge" }
                }
              }
            }
            """;

        MapperService mapperService = createMapperService(settings, mapping);
        SourceFieldMapper sourceMapper = mapperService.documentMapper().sourceMapper();

        // Without TIME_SERIES_DIMENSIONS function, should return SourceFieldBlockLoader
        BlockLoader blockLoader = sourceMapper.fieldType()
            .blockLoader(new DummyBlockLoaderContext.MapperServiceBlockLoaderContext(mapperService) {
                @Override
                public BlockLoaderFunctionConfig blockLoaderFunctionConfig() {
                    return null; // No function config
                }
            });

        assertThat(blockLoader, notNullValue());
        assertThat(blockLoader, instanceOf(SourceFieldBlockLoader.class));
    }
}
