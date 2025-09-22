/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TransportDownsampleActionTests extends ESTestCase {
    private static final Map<String, Object> METRIC_COUNTER_FIELD = Map.of("type", "long", "time_series_metric", "counter");
    private static final Map<String, Object> METRIC_GAUGE_FIELD = Map.of("type", "double", "time_series_metric", "gauge");
    private static final Map<String, Object> DIMENSION_FIELD = Map.of("type", "keyword", "time_series_dimension", true);
    private static final Map<String, Object> FLOAT_FIELD = Map.of("type", "float", "scaling_factor", 1000.0);
    private static final Map<String, Object> DATE_FIELD = Map.of("type", "date");
    private static final Map<String, Object> FLATTENED_FIELD = Map.of("type", "flattened");
    private static final Map<String, Object> PASSTHROUGH_FIELD = Map.of("type", "passthrough", "time_series_dimension", true);
    private static final Map<String, Object> AGGREGATE_METRIC_DOUBLE_FIELD = Map.of(
        "type",
        "aggregate_metric_double",
        "time_series_metric",
        "gauge",
        "metrics",
        List.of("max", "min", "value_count", "sum"),
        "default_metric",
        "max"
    );

    public void testCopyIndexMetadata() {
        // GIVEN
        final List<String> tiers = List.of(DataTier.DATA_HOT, DataTier.DATA_WARM, DataTier.DATA_COLD, DataTier.DATA_CONTENT);
        final IndexMetadata source = new IndexMetadata.Builder(randomAlphaOfLength(10)).settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 5))
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(1, 3))
                .put(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_UUID_KEY, UUID.randomUUID().toString())
                .put(IndexSettings.DEFAULT_PIPELINE.getKey(), randomAlphaOfLength(15))
                .put(IndexSettings.FINAL_PIPELINE.getKey(), randomAlphaOfLength(11))
                .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), randomBoolean())
                .put(DataTier.TIER_PREFERENCE_SETTING.getKey(), randomFrom(tiers))
                .put(IndexMetadata.SETTING_INDEX_PROVIDED_NAME, randomAlphaOfLength(20))
                .put(IndexMetadata.SETTING_CREATION_DATE, System.currentTimeMillis())
        ).build();
        final IndexMetadata target = new IndexMetadata.Builder(randomAlphaOfLength(10)).settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 6)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 4)
                .put(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_UUID_KEY, UUID.randomUUID().toString())
                .put(IndexSettings.DEFAULT_PIPELINE.getKey(), randomAlphaOfLength(15))
                .put(IndexSettings.FINAL_PIPELINE.getKey(), randomAlphaOfLength(11))
                .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), randomBoolean())
                .put(DataTier.TIER_PREFERENCE_SETTING.getKey(), randomFrom(tiers))
                .put(IndexMetadata.SETTING_CREATION_DATE, System.currentTimeMillis())
        ).build();
        final IndexScopedSettings indexScopedSettings = new IndexScopedSettings(
            Settings.EMPTY,
            IndexScopedSettings.BUILT_IN_INDEX_SETTINGS
        );

        // WHEN
        final Settings indexMetadata = TransportDownsampleAction.copyIndexMetadata(source, target, indexScopedSettings)
            .build()
            .getSettings();

        // THEN
        assertTargetSettings(target, indexMetadata);
        assertSourceSettings(source, indexMetadata);
        assertEquals(IndexMetadata.INDEX_UUID_NA_VALUE, indexMetadata.get(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_UUID_KEY));
        assertFalse(LifecycleSettings.LIFECYCLE_NAME_SETTING.exists(indexMetadata));
    }

    private static void assertSourceSettings(final IndexMetadata indexMetadata, final Settings settings) {
        assertEquals(indexMetadata.getIndex().getName(), settings.get(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_NAME_KEY));
        assertEquals(
            indexMetadata.getSettings().get(DataTier.TIER_PREFERENCE_SETTING.getKey()),
            settings.get(DataTier.TIER_PREFERENCE_SETTING.getKey())
        );
        assertEquals(indexMetadata.getSettings().get(IndexMetadata.LIFECYCLE_NAME), settings.get(IndexMetadata.LIFECYCLE_NAME));
        // NOTE: setting only in source (not forbidden, not to override): expect source setting is used
        assertEquals(
            indexMetadata.getSettings().get(IndexMetadata.SETTING_INDEX_PROVIDED_NAME),
            settings.get(IndexMetadata.SETTING_INDEX_PROVIDED_NAME)
        );
    }

    private static void assertTargetSettings(final IndexMetadata indexMetadata, final Settings settings) {
        assertEquals(
            indexMetadata.getSettings().get(IndexMetadata.SETTING_NUMBER_OF_SHARDS),
            settings.get(IndexMetadata.SETTING_NUMBER_OF_SHARDS)
        );
        assertEquals(
            indexMetadata.getSettings().get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS),
            settings.get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS)
        );
        assertEquals(
            indexMetadata.getSettings().get(IndexSettings.FINAL_PIPELINE.getKey()),
            settings.get(IndexSettings.FINAL_PIPELINE.getKey())
        );
        assertEquals(
            indexMetadata.getSettings().get(IndexSettings.DEFAULT_PIPELINE.getKey()),
            settings.get(IndexSettings.DEFAULT_PIPELINE.getKey())
        );
        assertEquals(
            indexMetadata.getSettings().get(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey()),
            settings.get(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey())
        );
        assertEquals(indexMetadata.getSettings().get(LifecycleSettings.LIFECYCLE_NAME), settings.get(LifecycleSettings.LIFECYCLE_NAME));
        // NOTE: setting both in source and target (not forbidden, not to override): expect target setting is used
        assertEquals(
            indexMetadata.getSettings().get(IndexMetadata.SETTING_CREATION_DATE),
            settings.get(IndexMetadata.SETTING_CREATION_DATE)
        );
    }

    public void testGetSupportedMetrics() {
        TimeSeriesParams.MetricType metricType = TimeSeriesParams.MetricType.GAUGE;
        Map<String, Object> fieldProperties = Map.of(
            "type",
            "aggregate_metric_double",
            "metrics",
            List.of("max", "sum"),
            "default_metric",
            "sum"
        );

        var supported = TransportDownsampleAction.getSupportedMetrics(metricType, fieldProperties);
        assertThat(supported.defaultMetric(), is("sum"));
        assertThat(supported.supportedMetrics(), is(List.of("max", "sum")));

        fieldProperties = Map.of("type", "integer");
        supported = TransportDownsampleAction.getSupportedMetrics(metricType, fieldProperties);
        assertThat(supported.defaultMetric(), is("max"));
        assertThat(supported.supportedMetrics(), is(List.of(metricType.supportedAggs())));
    }

    @SuppressWarnings("unchecked")
    public void testDownsampledMapping() throws IOException {
        TimeseriesFieldTypeHelper helper = new TimeseriesFieldTypeHelper.Builder(null).build("@timestamp");
        String downsampledMappingStr = TransportDownsampleAction.createDownsampleIndexMapping(
            helper,
            new DownsampleConfig(new DateHistogramInterval("5m")),
            Map.of(
                "properties",
                Map.of(
                    "my-dimension",
                    DIMENSION_FIELD,
                    "my-passthrough",
                    PASSTHROUGH_FIELD,
                    "my-multi-field",
                    Map.of("type", "keyword", "fields", Map.of("my-gauge", METRIC_GAUGE_FIELD)),
                    "my-object",
                    Map.of("type", "text", "properties", Map.of("my-float", FLOAT_FIELD, "my-counter", METRIC_COUNTER_FIELD)),
                    "my-flattened",
                    FLATTENED_FIELD,
                    "@timestamp",
                    DATE_FIELD
                )
            )
        );
        Map<String, Object> downsampledMapping = XContentHelper.convertToMap(JsonXContent.jsonXContent, downsampledMappingStr, false);
        assertThat(downsampledMapping.containsKey("properties"), is(true));
        Map<String, Object> properties = (Map<String, Object>) downsampledMapping.get("properties");
        assertThat(properties.get("my-dimension"), equalTo(DIMENSION_FIELD));
        assertThat(properties.get("my-passthrough"), equalTo(PASSTHROUGH_FIELD));
        assertThat(properties.get("my-flattened"), equalTo(FLATTENED_FIELD));
        Map<String, Object> timestamp = (Map<String, Object>) properties.get("@timestamp");
        assertThat(timestamp.get("type"), equalTo("date"));
        assertThat(timestamp.get("meta"), equalTo(Map.of("fixed_interval", "5m", "time_zone", "UTC")));
        Map<String, Object> multiField = (Map<String, Object>) properties.get("my-multi-field");
        assertThat(multiField.get("type"), equalTo("keyword"));
        assertThat(((Map<String, Object>) multiField.get("fields")).get("my-gauge"), equalTo(AGGREGATE_METRIC_DOUBLE_FIELD));
        Map<String, Object> objectField = (Map<String, Object>) properties.get("my-object");
        assertThat(objectField.get("type"), equalTo("text"));
        Map<String, Object> subObjectsProperties = (Map<String, Object>) objectField.get("properties");
        assertThat(subObjectsProperties.get("my-float"), equalTo(FLOAT_FIELD));
        assertThat(subObjectsProperties.get("my-counter"), equalTo(METRIC_COUNTER_FIELD));
    }
}
