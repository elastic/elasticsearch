/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.downsample;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.stats.MappingVisitor;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.downsample.DownsampleAction;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalTopHits;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.downsample.DownsampleIndexerAction;
import org.elasticsearch.xpack.core.downsample.DownsampleShardIndexerStatus;
import org.elasticsearch.xpack.core.downsample.DownsampleShardPersistentTaskState;
import org.elasticsearch.xpack.core.downsample.DownsampleShardTask;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.index.mapper.TimeSeriesParams.TIME_SERIES_METRIC_PARAM;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.Mockito.mock;

public class DownsampleActionSingleNodeTests extends ESSingleNodeTestCase {

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    public static final String FIELD_TIMESTAMP = "@timestamp";
    public static final String FIELD_DIMENSION_1 = "dimension_kw";
    public static final String FIELD_DIMENSION_2 = "dimension_long";
    public static final String FIELD_DIMENSION_3 = "dimension_flattened";
    public static final String FIELD_DIMENSION_4 = "dimension_kw_multifield";
    public static final String FIELD_NUMERIC_1 = "numeric_1";
    public static final String FIELD_NUMERIC_2 = "numeric_2";
    public static final String FIELD_AGG_METRIC = "agg_metric_1";
    public static final String FIELD_METRIC_LABEL_DOUBLE = "metric_label_double";
    public static final String FIELD_LABEL_DOUBLE = "label_double";
    public static final String FIELD_LABEL_INTEGER = "label_integer";
    public static final String FIELD_LABEL_KEYWORD = "label_keyword";
    public static final String FIELD_LABEL_TEXT = "label_text";
    public static final String FIELD_LABEL_BOOLEAN = "label_boolean";
    public static final String FIELD_LABEL_IPv4_ADDRESS = "label_ipv4_address";
    public static final String FIELD_LABEL_IPv6_ADDRESS = "label_ipv6_address";
    public static final String FIELD_LABEL_DATE = "label_date";
    public static final String FIELD_LABEL_UNMAPPED = "label_unmapped";
    public static final String FIELD_LABEL_KEYWORD_ARRAY = "label_keyword_array";
    public static final String FIELD_LABEL_DOUBLE_ARRAY = "label_double_array";
    public static final String FIELD_LABEL_AGG_METRIC = "label_agg_metric";

    private static final int MAX_DIM_VALUES = 5;
    private static final long MAX_NUM_BUCKETS = 10;
    public static final TimeValue TIMEOUT = new TimeValue(1, TimeUnit.MINUTES);

    private String sourceIndex, downsampleIndex;
    private long startTime;
    private int docCount, numOfShards, numOfReplicas;
    private List<String> dimensionValues;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(
            LocalStateCompositeXPackPlugin.class,
            Downsample.class,
            AggregateMetricMapperPlugin.class,
            DataStreamsPlugin.class,
            IndexLifecycle.class,
            TestTelemetryPlugin.class
        );
    }

    @Before
    public void setup() throws IOException {
        sourceIndex = randomAlphaOfLength(14).toLowerCase(Locale.ROOT);
        downsampleIndex = "downsample-" + sourceIndex;
        startTime = randomLongBetween(946769284000L, 1607470084000L); // random date between 2000-2020
        docCount = randomIntBetween(1000, 9000);
        numOfShards = randomIntBetween(1, 1);
        numOfReplicas = randomIntBetween(0, 0);

        // Values for keyword dimensions
        dimensionValues = new ArrayList<>(MAX_DIM_VALUES);
        for (int j = 0; j < randomIntBetween(1, MAX_DIM_VALUES); j++) {
            dimensionValues.add(randomAlphaOfLength(6));
        }

        /**
         * NOTE: here we map each numeric label field also as a (counter) metric.
         * This is done for testing purposes. There is no easy way to test
         * that labels are collected using the last value. The idea is to
         * check that the value of the label (last value) matches the value
         * of the corresponding metric which uses a last_value metric type.
         */
        Settings.Builder settings = indexSettings(numOfShards, numOfReplicas).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of(FIELD_DIMENSION_1))
            .put(
                IndexSettings.TIME_SERIES_START_TIME.getKey(),
                DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(Instant.ofEpochMilli(startTime).toEpochMilli())
            )
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2106-01-08T23:40:53.384Z");

        if (randomBoolean()) {
            settings.put(IndexMetadata.SETTING_INDEX_HIDDEN, randomBoolean());
        }

        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties");
        mapping.startObject(FIELD_TIMESTAMP).field("type", "date").endObject();

        // Dimensions
        mapping.startObject(FIELD_DIMENSION_1).field("type", "keyword").field("time_series_dimension", true).endObject();
        mapping.startObject(FIELD_DIMENSION_2).field("type", "long").field("time_series_dimension", true).endObject();
        mapping.startObject(FIELD_DIMENSION_3)
            .field("type", "flattened")
            .array("time_series_dimensions", "level1_value", "level1_obj.level2_value")
            .endObject();
        mapping.startObject(FIELD_DIMENSION_4)
            .field("type", "text")
            .startObject("fields")
            .startObject("keyword")
            .field("type", "keyword")
            .field("time_series_dimension", true)
            .endObject()
            .endObject()
            .endObject();

        // Metrics
        mapping.startObject(FIELD_NUMERIC_1).field("type", "long").field("time_series_metric", "gauge").endObject();
        mapping.startObject(FIELD_NUMERIC_2).field("type", "double").field("time_series_metric", "counter").endObject();
        mapping.startObject(FIELD_AGG_METRIC)
            .field("type", "aggregate_metric_double")
            .field("time_series_metric", "gauge")
            .array("metrics", new String[] { "min", "max", "sum", "value_count" })
            .field("default_metric", "value_count")
            .endObject();
        mapping.startObject(FIELD_METRIC_LABEL_DOUBLE)
            .field("type", "double") /* numeric label indexed as a metric */
            .field("time_series_metric", "counter")
            .endObject();

        // Labels
        mapping.startObject(FIELD_LABEL_DOUBLE).field("type", "double").endObject();
        mapping.startObject(FIELD_LABEL_INTEGER).field("type", "integer").endObject();
        mapping.startObject(FIELD_LABEL_KEYWORD).field("type", "keyword").endObject();
        mapping.startObject(FIELD_LABEL_TEXT).field("type", "text").field("store", "true").endObject();
        mapping.startObject(FIELD_LABEL_BOOLEAN).field("type", "boolean").endObject();
        mapping.startObject(FIELD_LABEL_IPv4_ADDRESS).field("type", "ip").endObject();
        mapping.startObject(FIELD_LABEL_IPv6_ADDRESS).field("type", "ip").endObject();
        mapping.startObject(FIELD_LABEL_DATE).field("type", "date").field("format", "date_optional_time").endObject();
        mapping.startObject(FIELD_LABEL_KEYWORD_ARRAY).field("type", "keyword").endObject();
        mapping.startObject(FIELD_LABEL_DOUBLE_ARRAY).field("type", "double").endObject();
        mapping.startObject(FIELD_LABEL_AGG_METRIC)
            .field("type", "aggregate_metric_double")
            .array("metrics", new String[] { "min", "max", "sum", "value_count" })
            .field("default_metric", "value_count")
            .endObject();

        mapping.endObject().endObject().endObject();
        assertAcked(indicesAdmin().prepareCreate(sourceIndex).setSettings(settings.build()).setMapping(mapping).get());
    }

    public void testDownsampleIndex() throws Exception {
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        SourceSupplier sourceSupplier = () -> {
            String ts = randomDateForInterval(config.getInterval());
            double labelDoubleValue = DATE_FORMATTER.parseMillis(ts);
            int labelIntegerValue = randomInt();
            long labelLongValue = randomLong();
            String labelIpv4Address = NetworkAddress.format(randomIp(true));
            String labelIpv6Address = NetworkAddress.format(randomIp(false));
            Date labelDateValue = randomDate();
            int keywordArraySize = randomIntBetween(3, 10);
            String[] keywordArray = new String[keywordArraySize];
            for (int i = 0; i < keywordArraySize; ++i) {
                keywordArray[i] = randomAlphaOfLength(10);
            }
            int doubleArraySize = randomIntBetween(3, 10);
            double[] doubleArray = new double[doubleArraySize];
            for (int i = 0; i < doubleArraySize; ++i) {
                doubleArray[i] = randomDouble();
            }
            return XContentFactory.jsonBuilder()
                .startObject()
                .field(FIELD_TIMESTAMP, ts)
                .field(FIELD_DIMENSION_1, randomFrom(dimensionValues))
                .field(FIELD_DIMENSION_2, randomIntBetween(1, 10))
                .field(FIELD_NUMERIC_1, randomInt())
                .field(FIELD_NUMERIC_2, DATE_FORMATTER.parseMillis(ts))
                .startObject(FIELD_AGG_METRIC)
                .field("min", randomDoubleBetween(-2000, -1001, true))
                .field("max", randomDoubleBetween(-1000, 1000, true))
                .field("sum", randomIntBetween(100, 10000))
                .field("value_count", randomIntBetween(100, 1000))
                .endObject()
                .field(FIELD_LABEL_DOUBLE, labelDoubleValue)
                .field(FIELD_METRIC_LABEL_DOUBLE, labelDoubleValue)
                .field(FIELD_LABEL_INTEGER, labelIntegerValue)
                .field(FIELD_LABEL_KEYWORD, ts)
                .field(FIELD_LABEL_UNMAPPED, randomBoolean() ? labelLongValue : labelDoubleValue)
                .field(FIELD_LABEL_TEXT, ts)
                .field(FIELD_LABEL_BOOLEAN, randomBoolean())
                .field(FIELD_LABEL_IPv4_ADDRESS, labelIpv4Address)
                .field(FIELD_LABEL_IPv6_ADDRESS, labelIpv6Address)
                .field(FIELD_LABEL_DATE, labelDateValue)
                .field(FIELD_LABEL_KEYWORD_ARRAY, keywordArray)
                .field(FIELD_LABEL_DOUBLE_ARRAY, doubleArray)
                .startObject(FIELD_LABEL_AGG_METRIC)
                .field("min", randomDoubleBetween(-2000, -1001, true))
                .field("max", randomDoubleBetween(-1000, 1000, true))
                .field("sum", Double.valueOf(randomIntBetween(100, 10000)))
                .field("value_count", randomIntBetween(100, 1000))
                .endObject()
                .endObject();
        };
        bulkIndex(sourceSupplier);
        prepareSourceIndex(sourceIndex, true);
        downsample(sourceIndex, downsampleIndex, config);
        assertDownsampleIndex(sourceIndex, downsampleIndex, config);
    }

    public void testDownsampleIndexWithFlattenedAndMultiFieldDimensions() throws Exception {
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        SourceSupplier sourceSupplier = () -> {
            String ts = randomDateForInterval(config.getInterval());
            double labelDoubleValue = DATE_FORMATTER.parseMillis(ts);
            return XContentFactory.jsonBuilder()
                .startObject()
                .field(FIELD_TIMESTAMP, ts)
                .field(FIELD_DIMENSION_1, "dim1") // not important for this test
                .startObject(FIELD_DIMENSION_3)
                .field("level1_value", randomFrom(dimensionValues))
                .field("level1_othervalue", randomFrom(dimensionValues))
                .startObject("level1_object")
                .field("level2_value", randomFrom(dimensionValues))
                .field("level2_othervalue", randomFrom(dimensionValues))
                .endObject()
                .endObject()
                .field(FIELD_DIMENSION_4, randomFrom(dimensionValues))
                .field(FIELD_NUMERIC_1, randomInt())
                .field(FIELD_NUMERIC_2, DATE_FORMATTER.parseMillis(ts))
                .startObject(FIELD_AGG_METRIC)
                .field("min", randomDoubleBetween(-2000, -1001, true))
                .field("max", randomDoubleBetween(-1000, 1000, true))
                .field("sum", randomIntBetween(100, 10000))
                .field("value_count", randomIntBetween(100, 1000))
                .endObject()
                .field(FIELD_LABEL_DOUBLE, labelDoubleValue)
                .field(FIELD_METRIC_LABEL_DOUBLE, labelDoubleValue)
                .endObject();
        };
        bulkIndex(sourceSupplier);
        prepareSourceIndex(sourceIndex, true);
        downsample(sourceIndex, downsampleIndex, config);
        assertDownsampleIndex(sourceIndex, downsampleIndex, config);
    }

    public void testDownsampleOfDownsample() throws Exception {
        int intervalMinutes = randomIntBetween(10, 120);
        DownsampleConfig config = new DownsampleConfig(DateHistogramInterval.minutes(intervalMinutes));
        SourceSupplier sourceSupplier = () -> {
            String ts = randomDateForInterval(config.getInterval());
            double labelDoubleValue = DATE_FORMATTER.parseMillis(ts);

            return XContentFactory.jsonBuilder()
                .startObject()
                .field(FIELD_TIMESTAMP, ts)
                .field(FIELD_DIMENSION_1, randomFrom(dimensionValues))
                .field(FIELD_NUMERIC_1, randomInt())
                .field(FIELD_NUMERIC_2, DATE_FORMATTER.parseMillis(ts))
                .startObject(FIELD_AGG_METRIC)
                .field("min", randomDoubleBetween(-2000, -1001, true))
                .field("max", randomDoubleBetween(-1000, 1000, true))
                .field("sum", randomIntBetween(100, 10000))
                .field("value_count", randomIntBetween(100, 1000))
                .endObject()
                .field(FIELD_LABEL_DOUBLE, labelDoubleValue)
                .field(FIELD_METRIC_LABEL_DOUBLE, labelDoubleValue)
                .startObject(FIELD_LABEL_AGG_METRIC)
                .field("min", randomDoubleBetween(-2000, -1001, true))
                .field("max", randomDoubleBetween(-1000, 1000, true))
                .field("sum", Double.valueOf(randomIntBetween(100, 10000)))
                .field("value_count", randomIntBetween(100, 1000))
                .endObject()
                .endObject();
        };
        bulkIndex(sourceSupplier);

        // Downsample the source index
        prepareSourceIndex(sourceIndex, true);
        downsample(sourceIndex, downsampleIndex, config);
        assertDownsampleIndex(sourceIndex, downsampleIndex, config);

        // Downsample the downsample index. The downsampling interval is a multiple of the previous downsampling interval.
        String downsampleIndex2 = downsampleIndex + "-2";
        DownsampleConfig config2 = new DownsampleConfig(DateHistogramInterval.minutes(intervalMinutes * randomIntBetween(2, 50)));
        downsample(downsampleIndex, downsampleIndex2, config2);
        assertDownsampleIndex(downsampleIndex, downsampleIndex2, config2);
    }

    private Date randomDate() {
        int randomYear = randomIntBetween(1970, 2020);
        int randomMonth = randomIntBetween(1, 12);
        int randomDayOfMonth = randomIntBetween(1, 28);
        int randomHour = randomIntBetween(0, 23);
        int randomMinute = randomIntBetween(0, 59);
        int randomSecond = randomIntBetween(0, 59);
        return Date.from(
            ZonedDateTime.of(randomYear, randomMonth, randomDayOfMonth, randomHour, randomMinute, randomSecond, 0, ZoneOffset.UTC)
                .toInstant()
        );
    }

    public void testCopyIndexSettings() throws IOException {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(LifecycleSettings.LIFECYCLE_NAME, randomAlphaOfLength(5))
            .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS_SETTING.getKey(), randomAlphaOfLength(5))
            .put(IndexSettings.LIFECYCLE_PARSE_ORIGINATION_DATE_SETTING.getKey(), randomBoolean());

        final Integer totalFieldsLimit = randomBoolean() ? randomIntBetween(100, 10_000) : null;
        if (totalFieldsLimit != null) {
            settingsBuilder.put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), totalFieldsLimit);
        }
        final Settings settings = settingsBuilder.build();
        logger.info("Updating index [{}] with settings [{}]", sourceIndex, settings);

        var updateSettingsReq = new UpdateSettingsRequest(settings, sourceIndex);
        assertAcked(indicesAdmin().updateSettings(updateSettingsReq).actionGet());

        DownsampleConfig config = new DownsampleConfig(randomInterval());
        SourceSupplier sourceSupplier = () -> {
            String ts = randomDateForInterval(config.getInterval());
            return XContentFactory.jsonBuilder()
                .startObject()
                .field(FIELD_TIMESTAMP, ts)
                .field(FIELD_DIMENSION_1, randomFrom(dimensionValues))
                .field(FIELD_NUMERIC_1, randomInt())
                .endObject();
        };
        bulkIndex(sourceSupplier);
        prepareSourceIndex(sourceIndex, true);
        downsample(sourceIndex, downsampleIndex, config);

        GetIndexResponse indexSettingsResp = indicesAdmin().prepareGetIndex().addIndices(sourceIndex, downsampleIndex).get();
        assertDownsampleIndexSettings(sourceIndex, downsampleIndex, indexSettingsResp);
        for (String key : settings.keySet()) {
            if (LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey().equals(key)) {
                assertNull(indexSettingsResp.getSetting(downsampleIndex, key));
            } else {
                assertEquals(settings.get(key), indexSettingsResp.getSetting(downsampleIndex, key));
            }
        }
    }

    public void testNullSourceIndexName() {
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        ActionRequestValidationException exception = expectThrows(
            ActionRequestValidationException.class,
            () -> downsample(null, downsampleIndex, config)
        );
        assertThat(exception.getMessage(), containsString("source index is missing"));
    }

    public void testNullDownsampleIndexName() {
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        ActionRequestValidationException exception = expectThrows(
            ActionRequestValidationException.class,
            () -> downsample(sourceIndex, null, config)
        );
        assertThat(exception.getMessage(), containsString("target index name is missing"));
    }

    public void testNullDownsampleConfig() {
        ActionRequestValidationException exception = expectThrows(
            ActionRequestValidationException.class,
            () -> downsample(sourceIndex, downsampleIndex, null)
        );
        assertThat(exception.getMessage(), containsString("downsample configuration is missing"));
    }

    public void testDownsampleSparseMetrics() throws Exception {
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        SourceSupplier sourceSupplier = () -> {
            XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field(FIELD_TIMESTAMP, randomDateForInterval(config.getInterval()))
                .field(FIELD_DIMENSION_1, randomFrom(dimensionValues));
            if (randomBoolean()) {
                builder.field(FIELD_NUMERIC_1, randomInt());
            }
            if (randomBoolean()) {
                builder.field(FIELD_NUMERIC_2, randomDouble());
            }
            return builder.endObject();
        };
        bulkIndex(sourceSupplier);
        prepareSourceIndex(sourceIndex, true);
        downsample(sourceIndex, downsampleIndex, config);
        assertDownsampleIndex(sourceIndex, downsampleIndex, config);
    }

    public void testCannotDownsampleToExistingIndex() throws Exception {
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        prepareSourceIndex(sourceIndex, true);

        // Create an empty index with the same name as the downsample index
        assertAcked(indicesAdmin().prepareCreate(downsampleIndex).setSettings(indexSettings(1, 0)).get());
        ResourceAlreadyExistsException exception = expectThrows(
            ResourceAlreadyExistsException.class,
            () -> downsample(sourceIndex, downsampleIndex, config)
        );
        assertThat(exception.getMessage(), containsString(downsampleIndex));
    }

    public void testDownsampleEmptyIndex() throws Exception {
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        // Source index has been created in the setup() method
        prepareSourceIndex(sourceIndex, true);
        downsample(sourceIndex, downsampleIndex, config);
        assertDownsampleIndex(sourceIndex, downsampleIndex, config);
    }

    public void testDownsampleIndexWithNoMetrics() throws Exception {
        // Create a source index that contains no metric fields in its mapping
        String sourceIndex = "no-metrics-idx-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        indicesAdmin().prepareCreate(sourceIndex)
            .setSettings(
                indexSettings(numOfShards, numOfReplicas).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                    .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of(FIELD_DIMENSION_1))
                    .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), Instant.ofEpochMilli(startTime).toString())
                    .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2106-01-08T23:40:53.384Z")
            )
            .setMapping(
                FIELD_TIMESTAMP,
                "type=date",
                FIELD_DIMENSION_1,
                "type=keyword,time_series_dimension=true",
                FIELD_DIMENSION_2,
                "type=long,time_series_dimension=true"
            )
            .get();

        DownsampleConfig config = new DownsampleConfig(randomInterval());
        prepareSourceIndex(sourceIndex, true);
        downsample(sourceIndex, downsampleIndex, config);
        assertDownsampleIndex(sourceIndex, downsampleIndex, config);
    }

    public void testCannotDownsampleWriteableIndex() {
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        // Source index has been created in the setup() method and is empty and still writable
        Exception exception = expectThrows(ElasticsearchException.class, () -> downsample(sourceIndex, downsampleIndex, config));
        assertThat(exception.getMessage(), containsString("Downsample requires setting [index.blocks.write = true] for index"));
    }

    public void testCannotDownsampleMissingIndex() {
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        IndexNotFoundException exception = expectThrows(
            IndexNotFoundException.class,
            () -> downsample("missing-index", downsampleIndex, config)
        );
        assertEquals("missing-index", exception.getIndex().getName());
        assertThat(exception.getMessage(), containsString("no such index [missing-index]"));
    }

    public void testCannotDownsampleWhileOtherDownsampleInProgress() throws Exception {
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field(FIELD_TIMESTAMP, randomDateForInterval(config.getInterval()))
            .field(FIELD_DIMENSION_1, randomAlphaOfLength(1))
            .field(FIELD_NUMERIC_1, randomDouble())
            .endObject();
        bulkIndex(sourceSupplier);
        prepareSourceIndex(sourceIndex, true);
        var downsampleListener = new ActionListener<AcknowledgedResponse>() {
            boolean success;

            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                if (acknowledgedResponse.isAcknowledged()) {
                    success = true;
                } else {
                    fail("Failed to receive downsample acknowledgement");
                }
            }

            @Override
            public void onFailure(Exception e) {
                fail("Downsample failed: " + e.getMessage());
            }
        };
        client().execute(
            DownsampleAction.INSTANCE,
            new DownsampleAction.Request(sourceIndex, downsampleIndex, TIMEOUT, config),
            downsampleListener
        );
        assertBusy(() -> {
            try {
                assertEquals(indicesAdmin().prepareGetIndex().addIndices(downsampleIndex).get().getIndices().length, 1);
            } catch (IndexNotFoundException e) {
                fail("downsample index has not been created");
            }
        });

        assertBusy(() -> {
            try {
                client().execute(DownsampleAction.INSTANCE, new DownsampleAction.Request(sourceIndex, downsampleIndex, TIMEOUT, config));
            } catch (ElasticsearchException e) {
                fail("transient failure due to overlapping downsample operations");
            }
        });

        // We must wait until the in-progress downsample ends, otherwise data will not be cleaned up
        assertBusy(() -> assertTrue("In progress downsample did not complete", downsampleListener.success), 60, TimeUnit.SECONDS);
    }

    public void testDownsampleDatastream() throws Exception {
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        String dataStreamName = createDataStream();

        final Instant now = Instant.now();
        SourceSupplier sourceSupplier = () -> {
            String ts = randomDateForRange(now.minusSeconds(60 * 60).toEpochMilli(), now.plusSeconds(60 * 29).toEpochMilli());
            return XContentFactory.jsonBuilder()
                .startObject()
                .field(FIELD_TIMESTAMP, ts)
                .field(FIELD_DIMENSION_1, randomFrom(dimensionValues))
                .field(FIELD_NUMERIC_1, randomInt())
                .field(FIELD_NUMERIC_2, DATE_FORMATTER.parseMillis(ts))
                .endObject();
        };
        bulkIndex(dataStreamName, sourceSupplier, docCount);

        String sourceIndex = rollover(dataStreamName).getOldIndex();
        prepareSourceIndex(sourceIndex, true);
        String downsampleIndex = "downsample-" + sourceIndex;
        downsample(sourceIndex, downsampleIndex, config);
        assertDownsampleIndex(sourceIndex, downsampleIndex, config);

        var r = client().execute(
            GetDataStreamAction.INSTANCE,
            new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { dataStreamName })
        ).get();
        assertEquals(1, r.getDataStreams().size());
        List<Index> indices = r.getDataStreams().get(0).getDataStream().getIndices();
        // Assert that the downsample index has not been added to the data stream
        assertTrue(indices.stream().filter(i -> i.getName().equals(downsampleIndex)).toList().isEmpty());
        // Assert that the source index is still a member of the data stream
        assertFalse(indices.stream().filter(i -> i.getName().equals(sourceIndex)).toList().isEmpty());
    }

    public void testCancelDownsampleIndexer() throws IOException {
        // create downsample config and index documents into source index
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field(FIELD_TIMESTAMP, randomDateForInterval(config.getInterval()))
            .field(FIELD_DIMENSION_1, randomAlphaOfLength(1))
            .field(FIELD_NUMERIC_1, randomDouble())
            .endObject();
        bulkIndex(sourceSupplier);
        prepareSourceIndex(sourceIndex, true);

        IndicesService indexServices = getInstanceFromNode(IndicesService.class);
        Index srcIndex = resolveIndex(sourceIndex);
        IndexService indexService = indexServices.indexServiceSafe(srcIndex);
        int shardNum = randomIntBetween(0, numOfShards - 1);
        IndexShard shard = indexService.getShard(shardNum);

        DownsampleShardTask task = new DownsampleShardTask(
            randomLong(),
            "rollup",
            "action",
            TaskId.EMPTY_TASK_ID,
            downsampleIndex,
            indexService.getIndexSettings().getTimestampBounds().startTime(),
            indexService.getIndexSettings().getTimestampBounds().endTime(),
            config,
            emptyMap(),
            shard.shardId()
        );
        task.testInit(mock(PersistentTasksService.class), mock(TaskManager.class), randomAlphaOfLength(5), randomIntBetween(1, 5));
        TaskCancelHelper.cancel(task, "test cancel");

        // re-use source index as temp index for test
        DownsampleShardIndexer indexer = new DownsampleShardIndexer(
            task,
            client(),
            indexService,
            getInstanceFromNode(DownsampleMetrics.class),
            shard.shardId(),
            downsampleIndex,
            config,
            new String[] { FIELD_NUMERIC_1, FIELD_NUMERIC_2 },
            new String[] {},
            new String[] { FIELD_DIMENSION_1, FIELD_DIMENSION_2 },
            new DownsampleShardPersistentTaskState(DownsampleShardIndexerStatus.INITIALIZED, null)
        );

        DownsampleShardIndexerException exception = expectThrows(DownsampleShardIndexerException.class, () -> indexer.execute());
        assertThat(exception.getCause().getMessage(), equalTo("Shard [" + sourceIndex + "][" + shardNum + "] downsample cancelled"));
    }

    public void testDownsampleBulkFailed() throws IOException {
        // create downsample config and index documents into source index
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field(FIELD_TIMESTAMP, randomDateForInterval(config.getInterval()))
            .field(FIELD_DIMENSION_1, randomAlphaOfLength(1))
            .field(FIELD_NUMERIC_1, randomDouble())
            .endObject();
        bulkIndex(sourceSupplier);
        prepareSourceIndex(sourceIndex, true);

        IndicesService indexServices = getInstanceFromNode(IndicesService.class);
        Index srcIndex = resolveIndex(sourceIndex);
        IndexService indexService = indexServices.indexServiceSafe(srcIndex);
        int shardNum = randomIntBetween(0, numOfShards - 1);
        IndexShard shard = indexService.getShard(shardNum);
        DownsampleShardTask task = new DownsampleShardTask(
            randomLong(),
            "rollup",
            "action",
            TaskId.EMPTY_TASK_ID,
            downsampleIndex,
            indexService.getIndexSettings().getTimestampBounds().startTime(),
            indexService.getIndexSettings().getTimestampBounds().endTime(),
            config,
            emptyMap(),
            shard.shardId()
        );
        task.testInit(mock(PersistentTasksService.class), mock(TaskManager.class), randomAlphaOfLength(5), randomIntBetween(1, 5));

        // re-use source index as temp index for test
        DownsampleShardIndexer indexer = new DownsampleShardIndexer(
            task,
            client(),
            indexService,
            getInstanceFromNode(DownsampleMetrics.class),
            shard.shardId(),
            downsampleIndex,
            config,
            new String[] { FIELD_NUMERIC_1, FIELD_NUMERIC_2 },
            new String[] {},
            new String[] { FIELD_DIMENSION_1, FIELD_DIMENSION_2 },
            new DownsampleShardPersistentTaskState(DownsampleShardIndexerStatus.INITIALIZED, null)
        );

        // block downsample index
        assertAcked(
            indicesAdmin().preparePutTemplate(downsampleIndex)
                .setPatterns(List.of(downsampleIndex))
                .setSettings(Settings.builder().put("index.blocks.write", "true").build())
        );

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, indexer::execute);
        assertThat(
            exception.getMessage(),
            equalTo(
                "Downsampling task ["
                    + task.getPersistentTaskId()
                    + "] on shard "
                    + shard.shardId()
                    + " failed indexing ["
                    + task.getNumFailed()
                    + "]"
            )
        );
    }

    public void testTooManyBytesInFlight() throws IOException {
        // create downsample config and index documents into source index
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field(FIELD_TIMESTAMP, randomDateForInterval(config.getInterval()))
            .field(FIELD_DIMENSION_1, randomAlphaOfLength(1))
            .field(FIELD_NUMERIC_1, randomDouble())
            .endObject();
        bulkIndex(sourceSupplier);
        prepareSourceIndex(sourceIndex, true);

        IndicesService indexServices = getInstanceFromNode(IndicesService.class);
        Index srcIndex = resolveIndex(sourceIndex);
        IndexService indexService = indexServices.indexServiceSafe(srcIndex);
        int shardNum = randomIntBetween(0, numOfShards - 1);
        IndexShard shard = indexService.getShard(shardNum);
        DownsampleShardTask task = new DownsampleShardTask(
            randomLong(),
            "rollup",
            "action",
            TaskId.EMPTY_TASK_ID,
            downsampleIndex,
            indexService.getIndexSettings().getTimestampBounds().startTime(),
            indexService.getIndexSettings().getTimestampBounds().endTime(),
            config,
            emptyMap(),
            shard.shardId()
        );
        task.testInit(mock(PersistentTasksService.class), mock(TaskManager.class), randomAlphaOfLength(5), randomIntBetween(1, 5));

        // re-use source index as temp index for test
        DownsampleShardIndexer indexer = new DownsampleShardIndexer(
            task,
            client(),
            indexService,
            getInstanceFromNode(DownsampleMetrics.class),
            shard.shardId(),
            downsampleIndex,
            config,
            new String[] { FIELD_NUMERIC_1, FIELD_NUMERIC_2 },
            new String[] {},
            new String[] { FIELD_DIMENSION_1, FIELD_DIMENSION_2 },
            new DownsampleShardPersistentTaskState(DownsampleShardIndexerStatus.INITIALIZED, null)
        );
        /*
         * Here we set the batch size and the total bytes in flight size to tiny numbers so that we are guaranteed to trigger the bulk
         * processor to reject some calls to add(), so that we can make sure DownsampleShardIndexer keeps trying until success.
         */
        indexer.downsampleMaxBytesInFlight = ByteSizeValue.ofBytes(1024);
        indexer.downsampleBulkSize = ByteSizeValue.ofBytes(512);
        indexer.execute();
    }

    public void testDownsampleStats() throws Exception {
        final PersistentTasksService persistentTasksService = mock(PersistentTasksService.class);
        final DownsampleConfig config = new DownsampleConfig(randomInterval());
        final SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field(FIELD_TIMESTAMP, randomDateForInterval(config.getInterval()))
            .field(FIELD_DIMENSION_1, randomAlphaOfLength(1))
            .field(FIELD_NUMERIC_1, randomDouble())
            .endObject();
        bulkIndex(sourceSupplier);
        prepareSourceIndex(sourceIndex, true);

        final IndicesService indexServices = getInstanceFromNode(IndicesService.class);
        final Index resolvedSourceIndex = resolveIndex(sourceIndex);
        final IndexService indexService = indexServices.indexServiceSafe(resolvedSourceIndex);
        for (int shardNum = 0; shardNum < numOfShards; shardNum++) {
            final IndexShard shard = indexService.getShard(shardNum);
            final DownsampleShardTask task = new DownsampleShardTask(
                randomLong(),
                "rollup",
                "action",
                TaskId.EMPTY_TASK_ID,
                downsampleIndex,
                indexService.getIndexSettings().getTimestampBounds().startTime(),
                indexService.getIndexSettings().getTimestampBounds().endTime(),
                config,
                emptyMap(),
                shard.shardId()
            );
            task.testInit(persistentTasksService, mock(TaskManager.class), randomAlphaOfLength(5), randomIntBetween(1, 5));

            final DownsampleShardIndexer indexer = new DownsampleShardIndexer(
                task,
                client(),
                indexService,
                getInstanceFromNode(DownsampleMetrics.class),
                shard.shardId(),
                downsampleIndex,
                config,
                new String[] { FIELD_NUMERIC_1, FIELD_NUMERIC_2 },
                new String[] {},
                new String[] { FIELD_DIMENSION_1, FIELD_DIMENSION_2 },
                new DownsampleShardPersistentTaskState(DownsampleShardIndexerStatus.INITIALIZED, null)
            );

            assertEquals(0.0F, task.getDocsProcessedPercentage(), 0.001);
            assertEquals(0L, task.getDownsampleBulkInfo().totalBulkCount());
            assertEquals(0L, task.getDownsampleBulkInfo().bulkTookSumMillis());
            assertEquals(0L, task.getDownsampleBulkInfo().bulkIngestSumMillis());
            assertEquals(DownsampleShardIndexerStatus.INITIALIZED, task.getDownsampleShardIndexerStatus());

            final DownsampleIndexerAction.ShardDownsampleResponse executeResponse = indexer.execute();

            assertDownsampleIndexer(indexService, shardNum, task, executeResponse, task.getTotalShardDocCount());
        }
    }

    public void testResumeDownsample() throws IOException {
        // create downsample config and index documents into source index
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field(FIELD_TIMESTAMP, randomDateForInterval(config.getInterval()))
            .field(FIELD_DIMENSION_1, randomBoolean() ? "dim1" : "dim2")
            .field(FIELD_NUMERIC_1, randomDouble())
            .endObject();
        bulkIndex(sourceSupplier);
        prepareSourceIndex(sourceIndex, true);

        IndicesService indexServices = getInstanceFromNode(IndicesService.class);
        Index srcIndex = resolveIndex(sourceIndex);
        IndexService indexService = indexServices.indexServiceSafe(srcIndex);
        int shardNum = randomIntBetween(0, numOfShards - 1);
        IndexShard shard = indexService.getShard(shardNum);

        DownsampleShardTask task = new DownsampleShardTask(
            randomLong(),
            "rollup",
            "action",
            TaskId.EMPTY_TASK_ID,
            downsampleIndex,
            indexService.getIndexSettings().getTimestampBounds().startTime(),
            indexService.getIndexSettings().getTimestampBounds().endTime(),
            config,
            emptyMap(),
            shard.shardId()
        );
        task.testInit(mock(PersistentTasksService.class), mock(TaskManager.class), randomAlphaOfLength(5), randomIntBetween(1, 5));

        DownsampleShardIndexer indexer = new DownsampleShardIndexer(
            task,
            client(),
            indexService,
            getInstanceFromNode(DownsampleMetrics.class),
            shard.shardId(),
            downsampleIndex,
            config,
            new String[] { FIELD_NUMERIC_1, FIELD_NUMERIC_2 },
            new String[] {},
            new String[] { FIELD_DIMENSION_1, FIELD_DIMENSION_2 },
            new DownsampleShardPersistentTaskState(
                DownsampleShardIndexerStatus.STARTED,
                new BytesRef(
                    new byte[] {
                        0x01,
                        0x0C,
                        0x64,
                        0x69,
                        0x6d,
                        0x65,
                        0x6E,
                        0x73,
                        0x69,
                        0x6F,
                        0x6E,
                        0x5F,
                        0x6B,
                        0x77,
                        0x73,
                        0x04,
                        0x64,
                        0x69,
                        0x6D,
                        0x31 }
                )
            )
        );

        final DownsampleIndexerAction.ShardDownsampleResponse response2 = indexer.execute();

        assertDownsampleIndexer(indexService, shardNum, task, response2, task.getTotalShardDocCount());
    }

    public void testResumeDownsamplePartial() throws IOException {
        // create downsample config and index documents into source index
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field(FIELD_TIMESTAMP, randomDateForInterval(config.getInterval()))
            .field(FIELD_DIMENSION_1, randomBoolean() ? "dim1" : "dim2")
            .field(FIELD_NUMERIC_1, randomDouble())
            .endObject();
        bulkIndex(sourceSupplier);
        prepareSourceIndex(sourceIndex, true);

        IndicesService indexServices = getInstanceFromNode(IndicesService.class);
        Index srcIndex = resolveIndex(sourceIndex);
        IndexService indexService = indexServices.indexServiceSafe(srcIndex);
        int shardNum = randomIntBetween(0, numOfShards - 1);
        IndexShard shard = indexService.getShard(shardNum);

        DownsampleShardTask task = new DownsampleShardTask(
            randomLong(),
            "rollup",
            "action",
            TaskId.EMPTY_TASK_ID,
            downsampleIndex,
            indexService.getIndexSettings().getTimestampBounds().startTime(),
            indexService.getIndexSettings().getTimestampBounds().endTime(),
            config,
            emptyMap(),
            shard.shardId()
        );
        task.testInit(mock(PersistentTasksService.class), mock(TaskManager.class), randomAlphaOfLength(5), randomIntBetween(1, 5));

        DownsampleShardIndexer indexer = new DownsampleShardIndexer(
            task,
            client(),
            indexService,
            getInstanceFromNode(DownsampleMetrics.class),
            shard.shardId(),
            downsampleIndex,
            config,
            new String[] { FIELD_NUMERIC_1, FIELD_NUMERIC_2 },
            new String[] {},
            new String[] { FIELD_DIMENSION_1 },
            new DownsampleShardPersistentTaskState(
                DownsampleShardIndexerStatus.STARTED,
                // NOTE: there is just one dimension with two possible values, this needs to be one of the two possible tsid values.
                new BytesRef(
                    new byte[] {
                        0x24,
                        0x42,
                        (byte) 0xe4,
                        (byte) 0x9f,
                        (byte) 0xe2,
                        (byte) 0xde,
                        (byte) 0xbb,
                        (byte) 0xf8,
                        (byte) 0xfc,
                        0x7d,
                        0x1a,
                        (byte) 0xb1,
                        0x27,
                        (byte) 0x85,
                        (byte) 0xc2,
                        (byte) 0x8e,
                        0x3a,
                        (byte) 0xae,
                        0x38,
                        0x6c,
                        (byte) 0xf6,
                        (byte) 0xae,
                        0x0f,
                        0x4f,
                        0x44,
                        (byte) 0xf1,
                        0x73,
                        0x02,
                        (byte) 0x90,
                        0x1d,
                        0x79,
                        (byte) 0xf8,
                        0x0d,
                        (byte) 0xc2,
                        0x7e,
                        (byte) 0x91,
                        0x15 }
                )
            )
        );

        final DownsampleIndexerAction.ShardDownsampleResponse response2 = indexer.execute();
        long dim2DocCount = SearchResponseUtils.getTotalHitsValue(
            client().prepareSearch(sourceIndex).setQuery(new TermQueryBuilder(FIELD_DIMENSION_1, "dim1")).setSize(10_000)
        );
        assertDownsampleIndexer(indexService, shardNum, task, response2, dim2DocCount);
    }

    private static void assertDownsampleIndexer(
        final IndexService indexService,
        int shardNum,
        final DownsampleShardTask task,
        final DownsampleIndexerAction.ShardDownsampleResponse response,
        long totalShardDocCount
    ) {
        assertEquals(response.getNumIndexed(), task.getNumIndexed());
        assertEquals(task.getNumReceived(), totalShardDocCount);
        assertEquals(indexService.getShard(shardNum).docStats().getCount(), task.getTotalShardDocCount());
        assertEquals(100.0D * task.getNumReceived() / task.getTotalShardDocCount(), task.getDocsProcessedPercentage(), 0.001);
        assertTrue(task.getDownsampleBulkInfo().bulkTookSumMillis() >= 0);
        assertEquals(task.getDownsampleBulkInfo().bulkIngestSumMillis(), task.getDownsampleBulkInfo().maxBulkIngestMillis());
        assertEquals(task.getDownsampleBulkInfo().bulkIngestSumMillis(), task.getDownsampleBulkInfo().minBulkIngestMillis());
        assertTrue(task.getDownsampleBulkInfo().bulkTookSumMillis() >= 0);
        assertEquals(task.getDownsampleBulkInfo().bulkTookSumMillis(), task.getDownsampleBulkInfo().maxBulkTookMillis());
        assertEquals(task.getDownsampleBulkInfo().bulkTookSumMillis(), task.getDownsampleBulkInfo().minBulkTookMillis());
        assertEquals(1L, task.getDownsampleBulkInfo().totalBulkCount());
        assertEquals(indexService.getIndexSettings().getTimestampBounds().startTime(), task.getIndexStartTimeMillis());
        assertEquals(indexService.getIndexSettings().getTimestampBounds().endTime(), task.getIndexEndTimeMillis());
        assertEquals(DownsampleShardIndexerStatus.COMPLETED, task.getDownsampleShardIndexerStatus());
        assertEquals(task.getNumSent(), task.getNumIndexed());
        assertEquals(task.getNumIndexed(), task.getLastBeforeBulkInfo().numberOfActions());
        assertTrue(task.getLastBeforeBulkInfo().estimatedSizeInBytes() > 0);
        assertFalse(task.getLastAfterBulkInfo().hasFailures());
        assertEquals(RestStatus.OK.getStatus(), task.getLastAfterBulkInfo().restStatusCode());
        assertTrue(task.getLastAfterBulkInfo().lastTookInMillis() >= 0);
        assertTrue(indexService.getIndexSettings().getTimestampBounds().startTime() <= task.getLastIndexingTimestamp());
        assertTrue(indexService.getIndexSettings().getTimestampBounds().startTime() <= task.getLastSourceTimestamp());
        assertTrue(indexService.getIndexSettings().getTimestampBounds().startTime() <= task.getLastTargetTimestamp());
        assertTrue(indexService.getIndexSettings().getTimestampBounds().endTime() >= task.getLastIndexingTimestamp());
        assertTrue(indexService.getIndexSettings().getTimestampBounds().endTime() >= task.getLastSourceTimestamp());
        assertTrue(indexService.getIndexSettings().getTimestampBounds().endTime() >= task.getLastTargetTimestamp());
    }

    private DateHistogramInterval randomInterval() {
        return ConfigTestHelpers.randomInterval();
    }

    private String randomDateForInterval(DateHistogramInterval interval) {
        long endTime = startTime + MAX_NUM_BUCKETS * interval.estimateMillis();
        return randomDateForRange(startTime, endTime);
    }

    private String randomDateForRange(long start, long end) {
        return DATE_FORMATTER.formatMillis(randomLongBetween(start, end));
    }

    private void bulkIndex(SourceSupplier sourceSupplier) throws IOException {
        bulkIndex(sourceIndex, sourceSupplier, docCount);
    }

    private void bulkIndex(final String indexName, final SourceSupplier sourceSupplier, int docCount) throws IOException {
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < docCount; i++) {
            IndexRequest indexRequest = new IndexRequest(indexName).opType(DocWriteRequest.OpType.CREATE);
            XContentBuilder source = sourceSupplier.get();
            indexRequest.source(source);
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        int duplicates = 0;
        for (BulkItemResponse response : bulkResponse.getItems()) {
            if (response.isFailed()) {
                if (response.getFailure().getCause() instanceof VersionConflictEngineException) {
                    // A duplicate event was created by random generator. We should not fail for this
                    // reason.
                    logger.debug("We tried to insert a duplicate: [{}]", response.getFailureMessage());
                    duplicates++;
                } else {
                    fail("Failed to index data: " + bulkResponse.buildFailureMessage());
                }
            }
        }
        int docsIndexed = docCount - duplicates;
        logger.info("Indexed [{}] documents. Dropped [{}] duplicates.", docsIndexed, duplicates);
        assertHitCount(client().prepareSearch(indexName).setSize(0), docsIndexed);
    }

    private void prepareSourceIndex(final String sourceIndex, boolean blockWrite) {
        // Set the source index to read-only state
        assertAcked(
            indicesAdmin().prepareUpdateSettings(sourceIndex)
                .setSettings(Settings.builder().put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), blockWrite).build())
        );
    }

    private void downsample(String sourceIndex, String downsampleIndex, DownsampleConfig config) {
        assertAcked(
            client().execute(DownsampleAction.INSTANCE, new DownsampleAction.Request(sourceIndex, downsampleIndex, TIMEOUT, config))
        );
    }

    private RolloverResponse rollover(String dataStreamName) throws ExecutionException, InterruptedException {
        RolloverResponse response = indicesAdmin().rolloverIndex(new RolloverRequest(dataStreamName, null)).get();
        assertAcked(response);
        return response;
    }

    private InternalAggregations aggregate(final String index, AggregationBuilder aggregationBuilder) {
        var resp = client().prepareSearch(index).setSize(0).addAggregation(aggregationBuilder).get();
        try {
            return resp.getAggregations();
        } finally {
            resp.decRef();
        }
    }

    @SuppressWarnings("unchecked")
    private void assertDownsampleIndex(String sourceIndex, String downsampleIndex, DownsampleConfig config) throws Exception {
        // Retrieve field information for the metric fields
        final GetMappingsResponse getMappingsResponse = indicesAdmin().prepareGetMappings(sourceIndex).get();
        final Map<String, Object> sourceIndexMappings = getMappingsResponse.mappings()
            .entrySet()
            .stream()
            .filter(entry -> sourceIndex.equals(entry.getKey()))
            .findFirst()
            .map(mappingMetadata -> mappingMetadata.getValue().sourceAsMap())
            .orElseThrow(() -> new IllegalArgumentException("No mapping found for downsample source index [" + sourceIndex + "]"));

        final IndexMetadata indexMetadata = clusterAdmin().prepareState().get().getState().getMetadata().index(sourceIndex);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final MapperService mapperService = indicesService.createIndexMapperServiceForValidation(indexMetadata);
        final CompressedXContent sourceIndexCompressedXContent = new CompressedXContent(sourceIndexMappings);
        mapperService.merge(MapperService.SINGLE_MAPPING_NAME, sourceIndexCompressedXContent, MapperService.MergeReason.INDEX_TEMPLATE);
        TimeseriesFieldTypeHelper helper = new TimeseriesFieldTypeHelper.Builder(mapperService).build(config.getTimestampField());

        Map<String, TimeSeriesParams.MetricType> metricFields = new HashMap<>();
        Map<String, String> labelFields = new HashMap<>();
        MappingVisitor.visitMapping(sourceIndexMappings, (field, fieldMapping) -> {
            if (helper.isTimeSeriesMetric(field, fieldMapping)) {
                metricFields.put(field, TimeSeriesParams.MetricType.fromString(fieldMapping.get(TIME_SERIES_METRIC_PARAM).toString()));
            } else if (helper.isTimeSeriesLabel(field, fieldMapping)) {
                labelFields.put(field, fieldMapping.get("type").toString());
            }
        });

        assertDownsampleIndexAggregations(sourceIndex, downsampleIndex, config, metricFields, labelFields);

        GetIndexResponse indexSettingsResp = indicesAdmin().prepareGetIndex().addIndices(sourceIndex, downsampleIndex).get();
        assertDownsampleIndexSettings(sourceIndex, downsampleIndex, indexSettingsResp);

        Map<String, Map<String, Object>> mappings = (Map<String, Map<String, Object>>) indexSettingsResp.getMappings()
            .get(downsampleIndex)
            .getSourceAsMap()
            .get("properties");

        assertFieldMappings(config, metricFields, mappings);

        GetMappingsResponse indexMappings = indicesAdmin().getMappings(new GetMappingsRequest().indices(downsampleIndex, sourceIndex))
            .actionGet();
        Map<String, String> downsampleIndexProperties = (Map<String, String>) indexMappings.mappings()
            .get(downsampleIndex)
            .sourceAsMap()
            .get("properties");
        Map<String, String> sourceIndexCloneProperties = (Map<String, String>) indexMappings.mappings()
            .get(sourceIndex)
            .sourceAsMap()
            .get("properties");
        List<Map.Entry<String, String>> labelFieldDownsampleIndexCloneProperties = (downsampleIndexProperties.entrySet()
            .stream()
            .filter(entry -> labelFields.containsKey(entry.getKey()))
            .toList());
        List<Map.Entry<String, String>> labelFieldSourceIndexProperties = (sourceIndexCloneProperties.entrySet()
            .stream()
            .filter(entry -> labelFields.containsKey(entry.getKey()))
            .toList());
        assertEquals(labelFieldDownsampleIndexCloneProperties, labelFieldSourceIndexProperties);

        // Check that metrics get collected as expected.
        final TestTelemetryPlugin plugin = getInstanceFromNode(PluginsService.class).filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();

        final List<Measurement> latencyShardMetrics = plugin.getLongHistogramMeasurement(DownsampleMetrics.LATENCY_SHARD);
        assertFalse(latencyShardMetrics.isEmpty());
        for (Measurement measurement : latencyShardMetrics) {
            assertTrue(measurement.value().toString(), measurement.value().longValue() >= 0 && measurement.value().longValue() < 1000_000);
            assertEquals(1, measurement.attributes().size());
            assertThat(measurement.attributes().get("status"), Matchers.in(List.of("success", "failed", "missing_docs")));
        }
        List<Measurement> shardActionMeasurements = plugin.getLongCounterMeasurement(DownsampleMetrics.ACTIONS_SHARD);
        assertThat(shardActionMeasurements.size(), greaterThanOrEqualTo(1));
        assertThat(shardActionMeasurements.get(0).getLong(), equalTo(1L));
        assertThat(shardActionMeasurements.get(0).attributes().get("status"), Matchers.in(List.of("success", "failed", "missing_docs")));

        // Total latency and counters are recorded after reindex and force-merge complete.
        assertBusy(() -> {
            final List<Measurement> latencyTotalMetrics = plugin.getLongHistogramMeasurement(DownsampleMetrics.LATENCY_TOTAL);
            assertFalse(latencyTotalMetrics.isEmpty());
            for (Measurement measurement : latencyTotalMetrics) {
                assertTrue(
                    measurement.value().toString(),
                    measurement.value().longValue() >= 0 && measurement.value().longValue() < 1000_000
                );
                assertEquals(1, measurement.attributes().size());
                assertThat(measurement.attributes().get("status"), Matchers.in(List.of("success", "invalid_configuration", "failed")));
            }

            List<Measurement> actionMeasurements = plugin.getLongCounterMeasurement(DownsampleMetrics.ACTIONS);
            assertThat(actionMeasurements.size(), greaterThanOrEqualTo(1));
            assertThat(actionMeasurements.get(0).getLong(), equalTo(1L));
            assertThat(
                actionMeasurements.get(0).attributes().get("status"),
                Matchers.in(List.of("success", "invalid_configuration", "failed"))
            );
        }, 10, TimeUnit.SECONDS);
    }

    private void assertDownsampleIndexAggregations(
        String sourceIndex,
        String downsampleIndex,
        DownsampleConfig config,
        Map<String, TimeSeriesParams.MetricType> metricFields,
        Map<String, String> labelFields
    ) {
        final AggregationBuilder aggregations = buildAggregations(config, metricFields, labelFields, config.getTimestampField());
        List<InternalAggregation> origList = aggregate(sourceIndex, aggregations).asList();
        List<InternalAggregation> downsampleList = aggregate(downsampleIndex, aggregations).asList();
        assertEquals(origList.size(), downsampleList.size());
        for (int i = 0; i < origList.size(); i++) {
            assertEquals(origList.get(i).getName(), downsampleList.get(i).getName());
        }

        StringTerms originalTsIdTermsAggregation = (StringTerms) origList.get(0);
        StringTerms downsampleTsIdTermsAggregation = (StringTerms) downsampleList.get(0);
        originalTsIdTermsAggregation.getBuckets().forEach(originalBucket -> {

            StringTerms.Bucket downsampleBucket = downsampleTsIdTermsAggregation.getBucketByKey(originalBucket.getKeyAsString());
            assertEquals(originalBucket.getAggregations().asList().size(), downsampleBucket.getAggregations().asList().size());

            InternalDateHistogram originalDateHistogram = (InternalDateHistogram) originalBucket.getAggregations().asList().get(0);
            InternalDateHistogram downsamoleDateHistogram = (InternalDateHistogram) downsampleBucket.getAggregations().asList().get(0);
            List<InternalDateHistogram.Bucket> originalDateHistogramBuckets = originalDateHistogram.getBuckets();
            List<InternalDateHistogram.Bucket> downsampleDateHistogramBuckets = downsamoleDateHistogram.getBuckets();
            assertEquals(originalDateHistogramBuckets.size(), downsampleDateHistogramBuckets.size());
            assertEquals(
                originalDateHistogramBuckets.stream().map(InternalDateHistogram.Bucket::getKeyAsString).collect(Collectors.toList()),
                downsampleDateHistogramBuckets.stream().map(InternalDateHistogram.Bucket::getKeyAsString).collect(Collectors.toList())
            );

            for (int i = 0; i < originalDateHistogramBuckets.size(); ++i) {
                InternalDateHistogram.Bucket originalDateHistogramBucket = originalDateHistogramBuckets.get(i);
                InternalDateHistogram.Bucket downsampleDateHistogramBucket = downsampleDateHistogramBuckets.get(i);
                assertEquals(originalDateHistogramBucket.getKeyAsString(), downsampleDateHistogramBucket.getKeyAsString());

                InternalAggregations originalAggregations = originalDateHistogramBucket.getAggregations();
                InternalAggregations downsampleAggregations = downsampleDateHistogramBucket.getAggregations();
                assertEquals(originalAggregations.asList().size(), downsampleAggregations.asList().size());

                List<InternalAggregation> nonTopHitsOriginalAggregations = originalAggregations.asList()
                    .stream()
                    .filter(agg -> agg.getType().equals("top_hits") == false)
                    .toList();
                List<InternalAggregation> nonTopHitsDownsampleAggregations = downsampleAggregations.asList()
                    .stream()
                    .filter(agg -> agg.getType().equals("top_hits") == false)
                    .toList();
                assertEquals(nonTopHitsOriginalAggregations, nonTopHitsDownsampleAggregations);

                List<InternalAggregation> topHitsOriginalAggregations = originalAggregations.asList()
                    .stream()
                    .filter(agg -> agg.getType().equals("top_hits"))
                    .toList();
                List<InternalAggregation> topHitsDownsampleAggregations = downsampleAggregations.asList()
                    .stream()
                    .filter(agg -> agg.getType().equals("top_hits"))
                    .toList();
                assertEquals(topHitsOriginalAggregations.size(), topHitsDownsampleAggregations.size());

                for (int j = 0; j < topHitsDownsampleAggregations.size(); ++j) {
                    InternalTopHits originalTopHits = (InternalTopHits) topHitsOriginalAggregations.get(j);
                    InternalTopHits downsampleTopHits = (InternalTopHits) topHitsDownsampleAggregations.get(j);
                    SearchHit[] originalHits = originalTopHits.getHits().getHits();
                    SearchHit[] downsampleHits = downsampleTopHits.getHits().getHits();
                    assertEquals(originalHits.length, downsampleHits.length);

                    for (int k = 0; k < originalHits.length; ++k) {
                        SearchHit originalHit = originalHits[k];
                        SearchHit downsampleHit = downsampleHits[k];

                        Map<String, DocumentField> originalHitDocumentFields = originalHit.getDocumentFields();
                        Map<String, DocumentField> downsampleHitDocumentFields = downsampleHit.getDocumentFields();
                        List<DocumentField> originalFields = originalHitDocumentFields.values().stream().toList();
                        List<DocumentField> downsampleFields = downsampleHitDocumentFields.values().stream().toList();
                        List<Object> originalFieldsList = originalFields.stream().flatMap(x -> x.getValues().stream()).toList();
                        List<Object> downsampelFieldsList = downsampleFields.stream().flatMap(x -> x.getValues().stream()).toList();
                        if (originalFieldsList.isEmpty() == false && downsampelFieldsList.isEmpty() == false) {
                            // NOTE: here we take advantage of the fact that a label field is indexed also as a metric of type
                            // `counter`. This way we can actually check that the label value stored in the downsample index
                            // is the last value (which is what we store for a metric of type counter) by comparing the metric
                            // field value to the label field value.
                            originalFieldsList.forEach(
                                field -> assertTrue(
                                    "Field [" + field + "] is not included in the downsample fields: " + downsampelFieldsList,
                                    downsampelFieldsList.contains(field)
                                )
                            );
                            downsampelFieldsList.forEach(
                                field -> assertTrue(
                                    "Field [" + field + "] is not included in the source fields: " + originalFieldsList,
                                    originalFieldsList.contains(field)
                                )
                            );
                            String labelName = originalHit.getDocumentFields().values().stream().findFirst().get().getName();
                            Object originalLabelValue = originalHit.getDocumentFields().values().stream().findFirst().get().getValue();
                            Object downsampleLabelValue = downsampleHit.getDocumentFields().values().stream().findFirst().get().getValue();
                            Optional<InternalAggregation> labelAsMetric = topHitsOriginalAggregations.stream()
                                .filter(agg -> agg.getName().equals("metric_" + downsampleTopHits.getName()))
                                .findFirst();
                            // NOTE: this check is possible only if the label can be indexed as a metric (the label is a numeric field)
                            if (labelAsMetric.isPresent()) {
                                double metricValue = ((InternalTopHits) labelAsMetric.get()).getHits().getHits()[0].field(
                                    "metric_" + labelName
                                ).getValue();
                                assertEquals(metricValue, downsampleLabelValue);
                                assertEquals(metricValue, originalLabelValue);
                            }
                        }
                    }
                }
            }
        });
    }

    @SuppressWarnings("unchecked")
    private void assertFieldMappings(
        DownsampleConfig config,
        Map<String, TimeSeriesParams.MetricType> metricFields,
        Map<String, Map<String, Object>> mappings
    ) {
        // Assert field mappings
        assertEquals(DateFieldMapper.CONTENT_TYPE, mappings.get(config.getTimestampField()).get("type"));
        Map<String, Object> dateTimeMeta = (Map<String, Object>) mappings.get(config.getTimestampField()).get("meta");
        assertEquals(config.getTimeZone(), dateTimeMeta.get("time_zone"));
        assertEquals(config.getInterval().toString(), dateTimeMeta.get(config.getIntervalType()));

        metricFields.forEach((field, metricType) -> {
            switch (metricType) {
                case COUNTER -> assertEquals("double", mappings.get(field).get("type"));
                case GAUGE -> assertEquals("aggregate_metric_double", mappings.get(field).get("type"));
                default -> fail("Unsupported field type");
            }
            assertEquals(metricType.toString(), mappings.get(field).get("time_series_metric"));
        });
    }

    private void assertDownsampleIndexSettings(String sourceIndex, String downsampleIndex, GetIndexResponse indexSettingsResp) {
        Settings sourceSettings = indexSettingsResp.settings().get(sourceIndex);
        Settings downsampleSettings = indexSettingsResp.settings().get(downsampleIndex);

        // Assert downsample metadata are set in index settings
        assertEquals("success", downsampleSettings.get(IndexMetadata.INDEX_DOWNSAMPLE_STATUS_KEY));

        assertNotNull(sourceSettings.get(IndexMetadata.SETTING_INDEX_UUID));
        assertNotNull(downsampleSettings.get(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_UUID_KEY));
        assertEquals(
            sourceSettings.get(IndexMetadata.SETTING_INDEX_UUID),
            downsampleSettings.get(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_UUID_KEY)
        );

        assertEquals(sourceIndex, downsampleSettings.get(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_NAME_KEY));
        assertEquals(sourceSettings.get(IndexSettings.MODE.getKey()), downsampleSettings.get(IndexSettings.MODE.getKey()));

        if (Strings.hasText(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_NAME.get(sourceSettings))) {
            // if the source is a downsample index itself, we're in the "downsample of downsample" test case and both indices should have
            // the same ORIGIN configured
            assertEquals(
                IndexMetadata.INDEX_DOWNSAMPLE_ORIGIN_NAME.get(sourceSettings),
                IndexMetadata.INDEX_DOWNSAMPLE_ORIGIN_NAME.get(downsampleSettings)
            );
            assertEquals(
                IndexMetadata.INDEX_DOWNSAMPLE_ORIGIN_UUID.get(sourceSettings),
                IndexMetadata.INDEX_DOWNSAMPLE_ORIGIN_UUID.get(downsampleSettings)
            );
        }
        assertNotNull(sourceSettings.get(IndexSettings.TIME_SERIES_START_TIME.getKey()));
        assertNotNull(downsampleSettings.get(IndexSettings.TIME_SERIES_START_TIME.getKey()));
        assertEquals(
            sourceSettings.get(IndexSettings.TIME_SERIES_START_TIME.getKey()),
            downsampleSettings.get(IndexSettings.TIME_SERIES_START_TIME.getKey())
        );

        assertNotNull(sourceSettings.get(IndexSettings.TIME_SERIES_END_TIME.getKey()));
        assertNotNull(downsampleSettings.get(IndexSettings.TIME_SERIES_END_TIME.getKey()));
        assertEquals(
            sourceSettings.get(IndexSettings.TIME_SERIES_END_TIME.getKey()),
            downsampleSettings.get(IndexSettings.TIME_SERIES_END_TIME.getKey())
        );
        assertNotNull(sourceSettings.get(IndexMetadata.INDEX_ROUTING_PATH.getKey()));
        assertNotNull(downsampleSettings.get(IndexMetadata.INDEX_ROUTING_PATH.getKey()));
        assertEquals(
            sourceSettings.get(IndexMetadata.INDEX_ROUTING_PATH.getKey()),
            downsampleSettings.get(IndexMetadata.INDEX_ROUTING_PATH.getKey())
        );

        assertNotNull(sourceSettings.get(IndexMetadata.SETTING_NUMBER_OF_SHARDS));
        assertNotNull(downsampleSettings.get(IndexMetadata.SETTING_NUMBER_OF_SHARDS));
        assertEquals(
            sourceSettings.get(IndexMetadata.SETTING_NUMBER_OF_SHARDS),
            downsampleSettings.get(IndexMetadata.SETTING_NUMBER_OF_SHARDS)
        );

        assertNotNull(sourceSettings.get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS));
        assertNotNull(downsampleSettings.get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS));
        assertEquals(
            sourceSettings.get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS),
            downsampleSettings.get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS)
        );

        assertNull(downsampleSettings.get(LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey()));
        assertEquals("true", downsampleSettings.get(IndexMetadata.SETTING_BLOCKS_WRITE));
        assertEquals(sourceSettings.get(IndexMetadata.SETTING_INDEX_HIDDEN), downsampleSettings.get(IndexMetadata.SETTING_INDEX_HIDDEN));

        if (sourceSettings.keySet().contains(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey())) {
            assertNotNull(indexSettingsResp.getSetting(downsampleIndex, MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey()));
        }

        if (sourceSettings.keySet().contains(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey())
            && downsampleSettings.keySet().contains(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey())) {
            assertEquals(
                sourceSettings.get(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey()),
                downsampleSettings.get(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey())
            );
        } else {
            assertFalse(sourceSettings.keySet().contains(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey()));
            assertFalse(downsampleSettings.keySet().contains(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey()));
        }
    }

    private AggregationBuilder buildAggregations(
        final DownsampleConfig config,
        final Map<String, TimeSeriesParams.MetricType> metrics,
        final Map<String, String> labels,
        final String timestampField
    ) {

        final TermsAggregationBuilder tsidAggregation = new TermsAggregationBuilder("tsid").field(TimeSeriesIdFieldMapper.NAME)
            .size(10_000);
        final DateHistogramAggregationBuilder dateHistogramAggregation = new DateHistogramAggregationBuilder("timestamp").field(
            config.getTimestampField()
        ).fixedInterval(config.getInterval()).minDocCount(1);
        if (config.getTimeZone() != null) {
            dateHistogramAggregation.timeZone(ZoneId.of(config.getTimeZone()));
        }
        tsidAggregation.subAggregation(dateHistogramAggregation);

        metrics.forEach((fieldName, metricType) -> {
            for (final String supportedAggregation : metricType.supportedAggs()) {
                switch (supportedAggregation) {
                    case "min" -> dateHistogramAggregation.subAggregation(
                        new MinAggregationBuilder(fieldName + "_" + supportedAggregation).field(fieldName)
                    );
                    case "max" -> dateHistogramAggregation.subAggregation(
                        new MaxAggregationBuilder(fieldName + "_" + supportedAggregation).field(fieldName)
                    );
                    case "last_value" -> dateHistogramAggregation.subAggregation(
                        new TopHitsAggregationBuilder(fieldName + "_" + supportedAggregation).sort(
                            SortBuilders.fieldSort(timestampField).order(SortOrder.DESC)
                        ).size(1).fetchField(fieldName)
                    );
                    case "sum" -> dateHistogramAggregation.subAggregation(
                        new SumAggregationBuilder(fieldName + "_" + supportedAggregation).field(fieldName)
                    );
                    case "value_count" -> dateHistogramAggregation.subAggregation(
                        new ValueCountAggregationBuilder(fieldName + "_" + supportedAggregation).field(fieldName)
                    );
                    default -> throw new IllegalArgumentException("Unsupported metric type [" + supportedAggregation + "]");
                }
            }
        });

        labels.forEach((fieldName, type) -> {
            dateHistogramAggregation.subAggregation(
                new TopHitsAggregationBuilder(fieldName + "_last_value").sort(SortBuilders.fieldSort(timestampField).order(SortOrder.DESC))
                    .size(1)
                    .fetchField(fieldName)
            );
        });

        return tsidAggregation;
    }

    @FunctionalInterface
    public interface SourceSupplier {
        XContentBuilder get() throws IOException;
    }

    private String createDataStream() throws Exception {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.getDefault());
        Template indexTemplate = new Template(
            indexSettings(numOfShards, numOfReplicas).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of(FIELD_DIMENSION_1))
                .build(),
            new CompressedXContent("""
                {
                    "properties": {
                        "@timestamp" : {
                            "type": "date"
                        },
                        "dimension_kw": {
                            "type": "keyword",
                            "time_series_dimension": true
                        },
                        "dimension_long": {
                            "type": "long",
                            "time_series_dimension": true
                        },
                        "numeric_1": {
                            "type": "long",
                            "time_series_metric": "gauge"
                        },
                        "numeric_2": {
                            "type": "double",
                            "time_series_metric": "counter"
                        }
                    }
                }
                """),
            null
        );

        ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStreamName + "*"))
            .template(indexTemplate)
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
            .build();
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(
            dataStreamName + "_template"
        ).indexTemplate(template);
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet());
        assertAcked(
            client().execute(
                CreateDataStreamAction.INSTANCE,
                new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStreamName)
            ).get()
        );
        return dataStreamName;
    }

    public void testConcurrentDownsample() throws Exception {
        final DownsampleConfig config = new DownsampleConfig(randomInterval());
        SourceSupplier sourceSupplier = () -> {
            String ts = randomDateForInterval(config.getInterval());
            double labelDoubleValue = DATE_FORMATTER.parseMillis(ts);
            int labelIntegerValue = randomInt();
            long labelLongValue = randomLong();
            String labelIpv4Address = NetworkAddress.format(randomIp(true));
            String labelIpv6Address = NetworkAddress.format(randomIp(false));
            Date labelDateValue = randomDate();
            int keywordArraySize = randomIntBetween(2, 5);
            String[] keywordArray = new String[keywordArraySize];
            for (int i = 0; i < keywordArraySize; ++i) {
                keywordArray[i] = randomAlphaOfLength(10);
            }
            int doubleArraySize = randomIntBetween(3, 10);
            double[] doubleArray = new double[doubleArraySize];
            for (int i = 0; i < doubleArraySize; ++i) {
                doubleArray[i] = randomDouble();
            }
            return XContentFactory.jsonBuilder()
                .startObject()
                .field(FIELD_TIMESTAMP, ts)
                .field(FIELD_DIMENSION_1, randomFrom(dimensionValues))
                .field(FIELD_DIMENSION_2, randomIntBetween(1, 10))
                .field(FIELD_NUMERIC_1, randomInt())
                .field(FIELD_NUMERIC_2, DATE_FORMATTER.parseMillis(ts))
                .startObject(FIELD_AGG_METRIC)
                .field("min", randomDoubleBetween(-2000, -1001, true))
                .field("max", randomDoubleBetween(-1000, 1000, true))
                .field("sum", randomIntBetween(100, 10000))
                .field("value_count", randomIntBetween(100, 1000))
                .endObject()
                .field(FIELD_LABEL_DOUBLE, labelDoubleValue)
                .field(FIELD_METRIC_LABEL_DOUBLE, labelDoubleValue)
                .field(FIELD_LABEL_INTEGER, labelIntegerValue)
                .field(FIELD_LABEL_KEYWORD, ts)
                .field(FIELD_LABEL_UNMAPPED, randomBoolean() ? labelLongValue : labelDoubleValue)
                .field(FIELD_LABEL_TEXT, ts)
                .field(FIELD_LABEL_BOOLEAN, randomBoolean())
                .field(FIELD_LABEL_IPv4_ADDRESS, labelIpv4Address)
                .field(FIELD_LABEL_IPv6_ADDRESS, labelIpv6Address)
                .field(FIELD_LABEL_DATE, labelDateValue)
                .field(FIELD_LABEL_KEYWORD_ARRAY, keywordArray)
                .field(FIELD_LABEL_DOUBLE_ARRAY, doubleArray)
                .startObject(FIELD_LABEL_AGG_METRIC)
                .field("min", randomDoubleBetween(-2000, -1001, true))
                .field("max", randomDoubleBetween(-1000, 1000, true))
                .field("sum", Double.valueOf(randomIntBetween(100, 10000)))
                .field("value_count", randomIntBetween(100, 1000))
                .endObject()
                .endObject();
        };
        bulkIndex(sourceIndex, sourceSupplier, 512);
        prepareSourceIndex(sourceIndex, true);

        int n = randomIntBetween(3, 6);
        final CountDownLatch downsampleComplete = new CountDownLatch(n);
        final List<String> targets = new ArrayList<>();
        final List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            final String targetIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
            targets.add(targetIndex);
            threads.add(new Thread(() -> {
                downsample(sourceIndex, targetIndex, config);
                downsampleComplete.countDown();
            }));
        }
        for (int i = 0; i < n; i++) {
            threads.get(i).start();
        }

        assertTrue(downsampleComplete.await(30, TimeUnit.SECONDS));

        for (int i = 0; i < n; i++) {
            assertDownsampleIndex(sourceIndex, targets.get(i), config);
        }
    }

    public void testDuplicateDownsampleRequest() throws Exception {
        final DownsampleConfig config = new DownsampleConfig(randomInterval());
        SourceSupplier sourceSupplier = () -> {
            String ts = randomDateForInterval(config.getInterval());
            double labelDoubleValue = DATE_FORMATTER.parseMillis(ts);
            int labelIntegerValue = randomInt();
            long labelLongValue = randomLong();
            String labelIpv4Address = NetworkAddress.format(randomIp(true));
            String labelIpv6Address = NetworkAddress.format(randomIp(false));
            Date labelDateValue = randomDate();
            int keywordArraySize = randomIntBetween(2, 5);
            String[] keywordArray = new String[keywordArraySize];
            for (int i = 0; i < keywordArraySize; ++i) {
                keywordArray[i] = randomAlphaOfLength(10);
            }
            int doubleArraySize = randomIntBetween(3, 10);
            double[] doubleArray = new double[doubleArraySize];
            for (int i = 0; i < doubleArraySize; ++i) {
                doubleArray[i] = randomDouble();
            }
            return XContentFactory.jsonBuilder()
                .startObject()
                .field(FIELD_TIMESTAMP, ts)
                .field(FIELD_DIMENSION_1, randomFrom(dimensionValues))
                .field(FIELD_DIMENSION_2, randomIntBetween(1, 10))
                .field(FIELD_NUMERIC_1, randomInt())
                .field(FIELD_NUMERIC_2, DATE_FORMATTER.parseMillis(ts))
                .startObject(FIELD_AGG_METRIC)
                .field("min", randomDoubleBetween(-2000, -1001, true))
                .field("max", randomDoubleBetween(-1000, 1000, true))
                .field("sum", randomIntBetween(100, 10000))
                .field("value_count", randomIntBetween(100, 1000))
                .endObject()
                .field(FIELD_LABEL_DOUBLE, labelDoubleValue)
                .field(FIELD_METRIC_LABEL_DOUBLE, labelDoubleValue)
                .field(FIELD_LABEL_INTEGER, labelIntegerValue)
                .field(FIELD_LABEL_KEYWORD, ts)
                .field(FIELD_LABEL_UNMAPPED, randomBoolean() ? labelLongValue : labelDoubleValue)
                .field(FIELD_LABEL_TEXT, ts)
                .field(FIELD_LABEL_BOOLEAN, randomBoolean())
                .field(FIELD_LABEL_IPv4_ADDRESS, labelIpv4Address)
                .field(FIELD_LABEL_IPv6_ADDRESS, labelIpv6Address)
                .field(FIELD_LABEL_DATE, labelDateValue)
                .field(FIELD_LABEL_KEYWORD_ARRAY, keywordArray)
                .field(FIELD_LABEL_DOUBLE_ARRAY, doubleArray)
                .startObject(FIELD_LABEL_AGG_METRIC)
                .field("min", randomDoubleBetween(-2000, -1001, true))
                .field("max", randomDoubleBetween(-1000, 1000, true))
                .field("sum", Double.valueOf(randomIntBetween(100, 10000)))
                .field("value_count", randomIntBetween(100, 1000))
                .endObject()
                .endObject();
        };
        bulkIndex(sourceIndex, sourceSupplier, 512);
        prepareSourceIndex(sourceIndex, true);

        final CountDownLatch downsampleComplete = new CountDownLatch(2);
        final String targetIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        AtomicBoolean firstFailed = new AtomicBoolean(false);
        AtomicBoolean secondFailed = new AtomicBoolean(false);
        // NOTE: we expect one thread to run the downsample operation and the other one to fail
        new Thread(() -> {
            try {
                downsample(sourceIndex, targetIndex, config);
            } catch (ResourceAlreadyExistsException e) {
                firstFailed.set(true);
            } finally {
                downsampleComplete.countDown();
            }

        }).start();
        new Thread(() -> {
            try {
                downsample(sourceIndex, targetIndex, config);
            } catch (ResourceAlreadyExistsException e) {
                secondFailed.set(true);
            } finally {
                downsampleComplete.countDown();
            }
        }).start();

        assertTrue(downsampleComplete.await(30, TimeUnit.SECONDS));
        assertFalse(firstFailed.get() ^ secondFailed.get());
        assertDownsampleIndex(sourceIndex, targetIndex, config);
    }
}
