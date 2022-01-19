/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.v2;

import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionDateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.HistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.rollup.v2.TransportRollupAction.addRollupSettings;
import static org.elasticsearch.xpack.rollup.v2.TransportRollupAction.getMapping;
import static org.elasticsearch.xpack.rollup.v2.TransportRollupAction.getSettings;
import static org.elasticsearch.xpack.rollup.v2.TransportRollupAction.rebuildRollupConfig;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class TransportRollupActionTests extends RollupTestCase {

    public void testSettingsFilter() {
        Settings settings = Settings.builder()
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), randomBoolean())
            .put(IndexMetadata.SETTING_INDEX_UUID, randomAlphaOfLength(5))
            .put(IndexMetadata.SETTING_HISTORY_UUID, randomAlphaOfLength(5))
            .put(IndexMetadata.SETTING_INDEX_PROVIDED_NAME, randomAlphaOfLength(5))
            .put(IndexMetadata.SETTING_CREATION_DATE, 0L)
            .put(LifecycleSettings.LIFECYCLE_NAME, randomAlphaOfLength(5))
            .put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, randomBoolean())
            .put(IndexMetadata.INDEX_ROLLUP_SOURCE_UUID_KEY, randomAlphaOfLength(5))
            .put(IndexMetadata.INDEX_ROLLUP_SOURCE_NAME_KEY, randomAlphaOfLength(5))
            .put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY, randomAlphaOfLength(5))
            .put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY, randomAlphaOfLength(5))
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), randomAlphaOfLength(5))
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), Instant.ofEpochMilli(1).toString())
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), Instant.ofEpochMilli(DateUtils.MAX_MILLIS_BEFORE_9999 - 1).toString())
            .build();
        IndexMetadata indexMetadata = newIndexMetadata(settings);
        Settings newSettings = getSettings(indexMetadata, null);
        Settings expected = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        assertThat(newSettings, equalTo(expected));
    }

    public void testNullTermsSettings() {
        Settings settings = Settings.builder().build();
        RollupActionGroupConfig groupConfig = new RollupActionGroupConfig(
            new RollupActionDateHistogramGroupConfig.FixedInterval(
                DataStreamTimestampFieldMapper.DEFAULT_PATH,
                ConfigTestHelpers.randomInterval(),
                randomZone().getId()
            ),
            null,
            null
        );
        IndexMetadata indexMetadata = newIndexMetadata(settings);
        Settings newSettings = getSettings(
            indexMetadata,
            new RollupActionConfig(groupConfig, ConfigTestHelpers.randomMetricsConfigs(random()))
        );
        Settings expected = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        assertThat(newSettings, equalTo(expected));
    }

    public void testNotTimestampSettings() {
        Settings settings = Settings.builder().build();
        IndexMetadata indexMetadata = newIndexMetadata(settings);
        Settings newSettings = getSettings(
            indexMetadata,
            new RollupActionConfig(
                ConfigTestHelpers.randomRollupActionGroupConfig(random()),
                ConfigTestHelpers.randomMetricsConfigs(random())
            )
        );
        Settings expected = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        assertThat(newSettings, equalTo(expected));
    }

    public void testStandardIndexSettings() {
        String[] terms = randomArray(1, 5, String[]::new, () -> randomAlphaOfLength(5));
        Settings settings = Settings.builder().build();
        RollupActionGroupConfig groupConfig = new RollupActionGroupConfig(
            new RollupActionDateHistogramGroupConfig.FixedInterval(
                DataStreamTimestampFieldMapper.DEFAULT_PATH,
                ConfigTestHelpers.randomInterval(),
                randomZone().getId()
            ),
            null,
            new TermsGroupConfig(terms)
        );
        IndexMetadata indexMetadata = newIndexMetadata(settings);
        Settings newSettings = getSettings(
            indexMetadata,
            new RollupActionConfig(groupConfig, ConfigTestHelpers.randomMetricsConfigs(random()))
        );
        Settings expected = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name().toLowerCase(Locale.ROOT))
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), terms)
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), Instant.ofEpochMilli(1).toString())
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), Instant.ofEpochMilli(DateUtils.MAX_MILLIS_BEFORE_9999 - 1).toString())
            .build();
        assertThat(newSettings, equalTo(expected));
    }

    public void testTsidSettings() {
        String[] terms = randomArray(1, 5, String[]::new, () -> randomAlphaOfLength(5));
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name().toLowerCase(Locale.ROOT))
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), terms)
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), Instant.ofEpochMilli(1).toString())
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), Instant.ofEpochMilli(DateUtils.MAX_MILLIS_BEFORE_9999 - 1).toString())
            .build();
        RollupActionGroupConfig groupConfig = new RollupActionGroupConfig(
            new RollupActionDateHistogramGroupConfig.FixedInterval(
                DataStreamTimestampFieldMapper.DEFAULT_PATH,
                ConfigTestHelpers.randomInterval(),
                randomZone().getId()
            ),
            null,
            new TermsGroupConfig(TimeSeriesIdFieldMapper.NAME)
        );
        IndexMetadata indexMetadata = newIndexMetadata(settings);
        Settings newSettings = getSettings(
            indexMetadata,
            new RollupActionConfig(groupConfig, ConfigTestHelpers.randomMetricsConfigs(random()))
        );
        Settings expected = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name().toLowerCase(Locale.ROOT))
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), terms)
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), Instant.ofEpochMilli(1).toString())
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), Instant.ofEpochMilli(DateUtils.MAX_MILLIS_BEFORE_9999 - 1).toString())
            .build();
        assertThat(newSettings, equalTo(expected));
    }

    public void testRoutingPathNotInTermsSettings() {
        List<String> routingPath = List.of("exist", "not_exist", "wildcard*");
        String[] terms = { "exist", "wildcard_1", "wildcard_2" };
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name().toLowerCase(Locale.ROOT))
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), routingPath)
            .build();
        RollupActionGroupConfig groupConfig = new RollupActionGroupConfig(
            new RollupActionDateHistogramGroupConfig.FixedInterval(
                DataStreamTimestampFieldMapper.DEFAULT_PATH,
                ConfigTestHelpers.randomInterval(),
                randomZone().getId()
            ),
            null,
            new TermsGroupConfig(terms)
        );
        IndexMetadata indexMetadata = newIndexMetadata(settings);
        Settings newSettings = getSettings(
            indexMetadata,
            new RollupActionConfig(groupConfig, ConfigTestHelpers.randomMetricsConfigs(random()))
        );
        Settings expected = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name().toLowerCase(Locale.ROOT))
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), terms)
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), Instant.ofEpochMilli(1).toString())
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), Instant.ofEpochMilli(DateUtils.MAX_MILLIS_BEFORE_9999 - 1).toString())
            .build();
        assertThat(newSettings, equalTo(expected));
    }

    public void testTermNotInRoutingPathSettings() {
        List<String> routingPath = List.of("exist", "wildcard*");
        String[] terms = { "exist", "not_exist", "wildcard_1", "wildcard_2" };
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name().toLowerCase(Locale.ROOT))
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), routingPath)
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), Instant.ofEpochMilli(1).toString())
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), Instant.ofEpochMilli(DateUtils.MAX_MILLIS_BEFORE_9999 - 1).toString())
            .build();
        RollupActionGroupConfig groupConfig = new RollupActionGroupConfig(
            new RollupActionDateHistogramGroupConfig.FixedInterval(
                DataStreamTimestampFieldMapper.DEFAULT_PATH,
                ConfigTestHelpers.randomInterval(),
                randomZone().getId()
            ),
            null,
            new TermsGroupConfig(terms)
        );
        IndexMetadata indexMetadata = newIndexMetadata(settings);
        Settings newSettings = getSettings(
            indexMetadata,
            new RollupActionConfig(groupConfig, ConfigTestHelpers.randomMetricsConfigs(random()))
        );
        Settings expected = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name().toLowerCase(Locale.ROOT))
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), routingPath)
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), Instant.ofEpochMilli(1).toString())
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), Instant.ofEpochMilli(DateUtils.MAX_MILLIS_BEFORE_9999 - 1).toString())
            .build();
        assertThat(newSettings, equalTo(expected));
    }

    public void testTermsNotMatchWildcardSettings() {
        List<String> routingPath = List.of("exist", "wildcard*");
        String[] terms = { "exist", "no_wildcard" };
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name().toLowerCase(Locale.ROOT))
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), routingPath)
            .build();
        RollupActionGroupConfig groupConfig = new RollupActionGroupConfig(
            new RollupActionDateHistogramGroupConfig.FixedInterval(
                DataStreamTimestampFieldMapper.DEFAULT_PATH,
                ConfigTestHelpers.randomInterval(),
                randomZone().getId()
            ),
            null,
            new TermsGroupConfig(terms)
        );
        IndexMetadata indexMetadata = newIndexMetadata(settings);
        Settings newSettings = getSettings(
            indexMetadata,
            new RollupActionConfig(groupConfig, ConfigTestHelpers.randomMetricsConfigs(random()))
        );
        Settings expected = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name().toLowerCase(Locale.ROOT))
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), terms)
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), Instant.ofEpochMilli(1).toString())
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), Instant.ofEpochMilli(DateUtils.MAX_MILLIS_BEFORE_9999 - 1).toString())
            .build();
        assertThat(newSettings, equalTo(expected));
    }

    public void testGetMapping() throws IOException {
        RollupActionGroupConfig groupConfig = new RollupActionGroupConfig(
            new RollupActionDateHistogramGroupConfig.FixedInterval(
                DataStreamTimestampFieldMapper.DEFAULT_PATH,
                DateHistogramInterval.MINUTE,
                "W-SU"
            ),
            new HistogramGroupConfig(10, "number1", "number2"),
            new TermsGroupConfig(TimeSeriesIdFieldMapper.NAME, "terms1", "terms2")
        );

        List<MetricConfig> metricConfigs = List.of(
            new MetricConfig("metric1", List.of("max", "min", "sum", "value_count")),
            new MetricConfig("metric2", List.of("max", "min")),
            new MetricConfig("metric3", List.of("sum"))
        );

        RollupActionConfig rollupActionConfig = new RollupActionConfig(groupConfig, metricConfigs);
        MappingMetadata mappingMetadata = randomBoolean()
            ? new MappingMetadata("_doc", randomBoolean() ? Map.of(SourceFieldMapper.NAME, Map.of("enabled", "false")) : Map.of())
            : null;
        XContentBuilder mapping = getMapping(rollupActionConfig, mappingMetadata);
        XContentBuilder expected = XContentFactory.jsonBuilder().startObject();
        if (mappingMetadata != null && mappingMetadata.getSourceAsMap().containsKey(SourceFieldMapper.NAME)) {
            expected.field(SourceFieldMapper.NAME, mappingMetadata.getSourceAsMap().get(SourceFieldMapper.NAME));
        }
        expected.startArray("dynamic_templates")
            .startObject()
            .startObject("strings")
            .field("match_mapping_type", "string")
            .startObject("mapping")
            .field("type", "keyword")
            .field("time_series_dimension", "true")
            .endObject()
            .endObject()
            .endObject()
            .endArray()
            .startObject("properties")
            .startObject("@timestamp")
            .field("type", "date")
            .startObject("meta")
            .field("fixed_interval", "1m")
            .field("time_zone", "W-SU")
            .endObject()
            .endObject()
            .startObject("number1")
            .field("type", "double")
            .startObject("meta")
            .field("interval", "10")
            .endObject()
            .endObject()
            .startObject("number2")
            .field("type", "double")
            .startObject("meta")
            .field("interval", "10")
            .endObject()
            .endObject()
            .startObject("terms1")
            .field("type", "keyword")
            .field("time_series_dimension", "true")
            .endObject()
            .startObject("terms2")
            .field("type", "keyword")
            .field("time_series_dimension", "true")
            .endObject()
            .startObject("metric1")
            .field("type", "aggregate_metric_double")
            .startArray("metrics")
            .value("max")
            .value("min")
            .value("sum")
            .value("value_count")
            .endArray()
            .field("default_metric", "value_count")
            .endObject()
            .startObject("metric2")
            .field("type", "aggregate_metric_double")
            .startArray("metrics")
            .value("max")
            .value("min")
            .endArray()
            .field("default_metric", "max")
            .endObject()
            .startObject("metric3")
            .field("type", "aggregate_metric_double")
            .startArray("metrics")
            .value("sum")
            .endArray()
            .field("default_metric", "sum")
            .endObject()
            .endObject()
            .endObject();

        assertThat(Strings.toString(mapping), is(Strings.toString(expected)));
    }

    public void testAddRollupSettings() {
        Settings.Builder settings = Settings.builder();
        if (randomBoolean()) {
            settings.put(LifecycleSettings.LIFECYCLE_NAME, randomAlphaOfLength(5));
        }
        if (randomBoolean()) {
            settings.put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, randomBoolean());
        }
        Settings newSettings = addRollupSettings(settings.build());
        Settings expected = settings.put(IndexMetadata.SETTING_INDEX_HIDDEN, true).build();
        assertThat(newSettings, equalTo(expected));
    }

    public void testNoWildcardRebuild() {
        RollupActionConfig rollupActionConfig = new RollupActionConfig(
            ConfigTestHelpers.randomRollupActionGroupConfig(random()),
            ConfigTestHelpers.randomMetricsConfigs(random())
        );
        RollupActionConfig newRollupActionConfig = rebuildRollupConfig(rollupActionConfig, Map.of());
        assertThat(newRollupActionConfig, equalTo(rollupActionConfig));
    }

    public void testWildcardRebuild() {
        RollupActionGroupConfig groupConfig = new RollupActionGroupConfig(
            ConfigTestHelpers.randomRollupActionDateHistogramGroupConfig(random()),
            new HistogramGroupConfig(10, "number1", "wildcard_num*"),
            new TermsGroupConfig("terms1", "terms2", "wildcard_term*")
        );
        List<MetricConfig> metricConfigs = List.of(
            new MetricConfig("metric1", List.of("max")),
            new MetricConfig("wildcard_metric*", List.of("max"))
        );
        RollupActionConfig rollupActionConfig = new RollupActionConfig(groupConfig, metricConfigs);
        RollupActionConfig newRollupActionConfig = rebuildRollupConfig(
            rollupActionConfig,
            Map.of(
                "wildcard_num_1",
                Map.of("double", createFieldCapabilities()),
                "wildcard_term_1",
                Map.of("keyword", createFieldCapabilities()),
                "wildcard_metric_1",
                Map.of("double", createFieldCapabilities())
            )
        );

        RollupActionConfig expected = new RollupActionConfig(
            new RollupActionGroupConfig(
                groupConfig.getDateHistogram(),
                new HistogramGroupConfig(10, "number1", "wildcard_num_1"),
                new TermsGroupConfig("terms1", "terms2", "wildcard_term_1")
            ),
            List.of(new MetricConfig("metric1", List.of("max")), new MetricConfig("wildcard_metric_1", List.of("max")))
        );
        assertThat(newRollupActionConfig, equalTo(expected));
    }

    public void testNotMatchWildcardRebuild() {
        RollupActionGroupConfig baseGroupConfig = new RollupActionGroupConfig(
            ConfigTestHelpers.randomRollupActionDateHistogramGroupConfig(random()),
            new HistogramGroupConfig(10, "wildcard_num*"),
            new TermsGroupConfig("wildcard_term*")
        );
        List<MetricConfig> baseMetricConfigs = List.of(new MetricConfig("wildcard_metric*", List.of("max")));

        Map<String, Map<String, FieldCapabilities>> fieldCaps = Map.of(
            "wildcard_num_1",
            Map.of("double", createFieldCapabilities()),
            "wildcard_term_1",
            Map.of("keyword", createFieldCapabilities()),
            "wildcard_metric_1",
            Map.of("double", createFieldCapabilities())
        );

        {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> rebuildRollupConfig(
                    new RollupActionConfig(
                        new RollupActionGroupConfig(
                            baseGroupConfig.getDateHistogram(),
                            baseGroupConfig.getHistogram(),
                            new TermsGroupConfig("wildcard_no*")
                        ),
                        baseMetricConfigs
                    ),
                    fieldCaps
                )
            );
            assertThat(e.getMessage(), containsString("Could not find a field match the group terms [wildcard_no*]"));
        }

        {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> rebuildRollupConfig(
                    new RollupActionConfig(
                        new RollupActionGroupConfig(
                            baseGroupConfig.getDateHistogram(),
                            new HistogramGroupConfig(10, "wildcard_no*"),
                            baseGroupConfig.getTerms()
                        ),
                        baseMetricConfigs
                    ),
                    fieldCaps
                )
            );
            assertThat(e.getMessage(), containsString("Could not find a field match the group histograms [wildcard_no*]"));
        }

        {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> rebuildRollupConfig(
                    new RollupActionConfig(baseGroupConfig, List.of(new MetricConfig("wildcard_no*", List.of("max")))),
                    fieldCaps
                )
            );
            assertThat(e.getMessage(), containsString("Could not find a field match the metric fields [wildcard_no*]"));
        }
    }

    private FieldCapabilities createFieldCapabilities() {
        return new FieldCapabilities("field", "type", false, true, true, null, null, null, Collections.emptyMap());
    }
}
