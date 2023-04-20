/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.timeseries.support;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesDimensionsLimitIT extends ESIntegTestCase {

    public void testDimensionFieldNameLimit() throws IOException {
        int dimensionFieldLimit = 21;
        final String dimensionFieldName = randomAlphaOfLength(randomIntBetween(513, 1024));
        createTimeSeriesIndex(mapping -> {
            mapping.startObject("routing_field").field("type", "keyword").field("time_series_dimension", true).endObject();
            mapping.startObject(dimensionFieldName).field("type", "keyword").field("time_series_dimension", true).endObject();
        },
            mapping -> mapping.startObject("gauge").field("type", "integer").field("time_series_metric", "gauge").endObject(),
            () -> List.of("routing_field"),
            dimensionFieldLimit
        );
        final Exception ex = expectThrows(
            DocumentParsingException.class,
            () -> client().prepareIndex("test")
                .setSource(
                    "routing_field",
                    randomAlphaOfLength(10),
                    dimensionFieldName,
                    randomAlphaOfLength(1024),
                    "gauge",
                    randomIntBetween(10, 20),
                    "@timestamp",
                    Instant.now().toEpochMilli()
                )
                .get()
        );
        assertThat(
            ex.getCause().getMessage(),
            equalTo(
                "Dimension name must be less than [512] bytes but [" + dimensionFieldName + "] was [" + dimensionFieldName.length() + "]."
            )
        );
    }

    public void testDimensionFieldValueLimit() throws IOException {
        int dimensionFieldLimit = 21;
        createTimeSeriesIndex(
            mapping -> mapping.startObject("field").field("type", "keyword").field("time_series_dimension", true).endObject(),
            mapping -> mapping.startObject("gauge").field("type", "integer").field("time_series_metric", "gauge").endObject(),
            () -> List.of("field"),
            dimensionFieldLimit
        );
        long startTime = Instant.now().toEpochMilli();
        client().prepareIndex("test")
            .setSource("field", randomAlphaOfLength(1024), "gauge", randomIntBetween(10, 20), "@timestamp", startTime)
            .get();
        final Exception ex = expectThrows(
            DocumentParsingException.class,
            () -> client().prepareIndex("test")
                .setSource("field", randomAlphaOfLength(1025), "gauge", randomIntBetween(10, 20), "@timestamp", startTime + 1)
                .get()
        );
        assertThat(ex.getCause().getMessage(), equalTo("Dimension fields must be less than [1024] bytes but was [1025]."));
    }

    public void testTotalNumberOfDimensionFieldsLimit() {
        int dimensionFieldLimit = 21;
        final Exception ex = expectThrows(IllegalArgumentException.class, () -> createTimeSeriesIndex(mapping -> {
            mapping.startObject("routing_field").field("type", "keyword").field("time_series_dimension", true).endObject();
            for (int i = 0; i < dimensionFieldLimit; i++) {
                mapping.startObject(randomAlphaOfLength(10)).field("type", "keyword").field("time_series_dimension", true).endObject();
            }
        },
            mapping -> mapping.startObject("gauge").field("type", "integer").field("time_series_metric", "gauge").endObject(),
            () -> List.of("routing_field"),
            dimensionFieldLimit
        ));

        assertThat(ex.getMessage(), equalTo("Limit of total dimension fields [" + dimensionFieldLimit + "] has been exceeded"));
    }

    public void testTotalNumberOfDimensionFieldsDefaultLimit() {
        int dimensionFieldLimit = 21;
        final Exception ex = expectThrows(IllegalArgumentException.class, () -> createTimeSeriesIndex(mapping -> {
            mapping.startObject("routing_field").field("type", "keyword").field("time_series_dimension", true).endObject();
            for (int i = 0; i < dimensionFieldLimit; i++) {
                mapping.startObject(randomAlphaOfLength(10)).field("type", "keyword").field("time_series_dimension", true).endObject();
            }
        },
            mapping -> mapping.startObject("gauge").field("type", "integer").field("time_series_metric", "gauge").endObject(),
            () -> List.of("routing_field"),
            null // NOTE: using default field limit
        ));

        assertThat(ex.getMessage(), equalTo("Limit of total dimension fields [" + dimensionFieldLimit + "] has been exceeded"));
    }

    public void testTotalDimensionFieldsSizeLuceneLimit() throws IOException {
        int dimensionFieldLimit = 21;
        final List<String> dimensionFieldNames = new ArrayList<>();
        createTimeSeriesIndex(mapping -> {
            for (int i = 0; i < dimensionFieldLimit; i++) {
                String dimensionFieldName = randomAlphaOfLength(512);
                dimensionFieldNames.add(dimensionFieldName);
                mapping.startObject(dimensionFieldName).field("type", "keyword").field("time_series_dimension", true).endObject();
            }
        },
            mapping -> mapping.startObject("gauge").field("type", "integer").field("time_series_metric", "gauge").endObject(),
            () -> List.of(dimensionFieldNames.get(0)),
            dimensionFieldLimit
        );

        final Map<String, Object> source = new HashMap<>();
        source.put("gauge", randomIntBetween(10, 20));
        source.put("@timestamp", Instant.now().toEpochMilli());
        for (int i = 0; i < dimensionFieldLimit; i++) {
            source.put(dimensionFieldNames.get(i), randomAlphaOfLength(1024));
        }
        final IndexResponse indexResponse = client().prepareIndex("test").setSource(source).get();
        assertEquals(RestStatus.CREATED.getStatus(), indexResponse.status().getStatus());
    }

    public void testTotalDimensionFieldsSizeLuceneLimitPlusOne() throws IOException {
        int dimensionFieldLimit = 22;
        final List<String> dimensionFieldNames = new ArrayList<>();
        createTimeSeriesIndex(mapping -> {
            for (int i = 0; i < dimensionFieldLimit; i++) {
                String dimensionFieldName = randomAlphaOfLength(512);
                dimensionFieldNames.add(dimensionFieldName);
                mapping.startObject(dimensionFieldName).field("type", "keyword").field("time_series_dimension", true).endObject();
            }
        },
            mapping -> mapping.startObject("gauge").field("type", "integer").field("time_series_metric", "gauge").endObject(),
            () -> List.of(dimensionFieldNames.get(0)),
            dimensionFieldLimit
        );

        final Map<String, Object> source = new HashMap<>();
        source.put("routing_field", randomAlphaOfLength(1024));
        source.put("gauge", randomIntBetween(10, 20));
        source.put("@timestamp", Instant.now().toEpochMilli());
        for (int i = 0; i < dimensionFieldLimit; i++) {
            source.put(dimensionFieldNames.get(i), randomAlphaOfLength(1024));
        }
        final Exception ex = expectThrows(DocumentParsingException.class, () -> client().prepareIndex("test").setSource(source).get());
        assertEquals("_tsid longer than [32766] bytes [33903].", ex.getCause().getMessage());
    }

    private void createTimeSeriesIndex(
        final CheckedConsumer<XContentBuilder, IOException> dimensions,
        final CheckedConsumer<XContentBuilder, IOException> metrics,
        final Supplier<List<String>> routingPaths,
        final Integer dimensionsFieldLimit
    ) throws IOException {
        XContentBuilder mapping = JsonXContent.contentBuilder();
        mapping.startObject().startObject("properties");
        mapping.startObject("@timestamp").field("type", "date").endObject();
        metrics.accept(mapping);
        dimensions.accept(mapping);
        mapping.endObject().endObject();

        Settings.Builder settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), routingPaths.get())
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2000-01-08T23:40:53.384Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2106-01-08T23:40:53.384Z");

        if (dimensionsFieldLimit != null) {
            settings.put(MapperService.INDEX_MAPPING_DIMENSION_FIELDS_LIMIT_SETTING.getKey(), dimensionsFieldLimit);
        }

        client().admin().indices().prepareCreate("test").setSettings(settings.build()).setMapping(mapping).get();
    }

}
