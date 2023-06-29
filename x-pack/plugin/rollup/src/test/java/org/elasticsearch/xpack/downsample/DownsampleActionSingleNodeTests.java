/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.downsample;

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
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.unit.ByteSizeValue;
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
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalTopHits;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.downsample.DownsampleAction;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.elasticsearch.xpack.core.rollup.action.RollupShardTask;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.elasticsearch.xpack.rollup.Rollup;
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
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.index.mapper.TimeSeriesParams.TIME_SERIES_METRIC_PARAM;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DownsampleActionSingleNodeTests extends ESSingleNodeTestCase {

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    public static final String FIELD_TIMESTAMP = "@timestamp";
    public static final String FIELD_DIMENSION_1 = "dimension_kw";
    public static final String FIELD_DIMENSION_2 = "dimension_long";
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

    private String sourceIndex, rollupIndex;
    private long startTime;
    private int docCount, numOfShards, numOfReplicas;
    private List<String> dimensionValues;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(
            LocalStateCompositeXPackPlugin.class,
            Rollup.class,
            AggregateMetricMapperPlugin.class,
            DataStreamsPlugin.class,
            IndexLifecycle.class
        );
    }

    @Before
    public void setup() throws IOException {
        sourceIndex = randomAlphaOfLength(14).toLowerCase(Locale.ROOT);
        rollupIndex = "rollup-" + sourceIndex;
        startTime = randomLongBetween(946769284000L, 1607470084000L); // random date between 2000-2020
        docCount = randomIntBetween(10, 9000);
        numOfShards = randomIntBetween(1, 4);
        numOfReplicas = randomIntBetween(0, 3);

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

    public void testRollupIndex() throws IOException {
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
        prepareSourceIndex(sourceIndex);
        rollup(sourceIndex, rollupIndex, config);
        assertRollupIndex(sourceIndex, rollupIndex, config);
    }

    public void testRollupOfRollups() throws IOException {
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
        prepareSourceIndex(sourceIndex);
        rollup(sourceIndex, rollupIndex, config);
        assertRollupIndex(sourceIndex, rollupIndex, config);

        // Downsample the rollup index. The downsampling interval is a multiple of the previous downsampling interval.
        String rollupIndex2 = rollupIndex + "-2";
        DownsampleConfig config2 = new DownsampleConfig(DateHistogramInterval.minutes(intervalMinutes * randomIntBetween(2, 50)));
        rollup(rollupIndex, rollupIndex2, config2);
        assertRollupIndex(sourceIndex, rollupIndex2, config2);
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
        prepareSourceIndex(sourceIndex);
        rollup(sourceIndex, rollupIndex, config);

        GetIndexResponse indexSettingsResp = indicesAdmin().prepareGetIndex().addIndices(sourceIndex, rollupIndex).get();
        assertRollupIndexSettings(sourceIndex, rollupIndex, indexSettingsResp);
        for (String key : settings.keySet()) {
            assertEquals(settings.get(key), indexSettingsResp.getSetting(rollupIndex, key));
        }
    }

    public void testNullSourceIndexName() {
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        ActionRequestValidationException exception = expectThrows(
            ActionRequestValidationException.class,
            () -> rollup(null, rollupIndex, config)
        );
        assertThat(exception.getMessage(), containsString("source index is missing"));
    }

    public void testNullRollupIndexName() {
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        ActionRequestValidationException exception = expectThrows(
            ActionRequestValidationException.class,
            () -> rollup(sourceIndex, null, config)
        );
        assertThat(exception.getMessage(), containsString("target index name is missing"));
    }

    public void testNullRollupConfig() {
        ActionRequestValidationException exception = expectThrows(
            ActionRequestValidationException.class,
            () -> rollup(sourceIndex, rollupIndex, null)
        );
        assertThat(exception.getMessage(), containsString("downsample configuration is missing"));
    }

    public void testRollupSparseMetrics() throws IOException {
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
        prepareSourceIndex(sourceIndex);
        rollup(sourceIndex, rollupIndex, config);
        assertRollupIndex(sourceIndex, rollupIndex, config);
    }

    public void testCannotRollupToExistingIndex() throws Exception {
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        prepareSourceIndex(sourceIndex);

        // Create an empty index with the same name as the rollup index
        assertAcked(indicesAdmin().prepareCreate(rollupIndex).setSettings(indexSettings(1, 0)).get());
        ResourceAlreadyExistsException exception = expectThrows(
            ResourceAlreadyExistsException.class,
            () -> rollup(sourceIndex, rollupIndex, config)
        );
        assertThat(exception.getMessage(), containsString(rollupIndex));
    }

    public void testRollupEmptyIndex() throws IOException {
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        // Source index has been created in the setup() method
        prepareSourceIndex(sourceIndex);
        rollup(sourceIndex, rollupIndex, config);
        assertRollupIndex(sourceIndex, rollupIndex, config);
    }

    public void testRollupIndexWithNoMetrics() throws IOException {
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
        prepareSourceIndex(sourceIndex);
        rollup(sourceIndex, rollupIndex, config);
        assertRollupIndex(sourceIndex, rollupIndex, config);
    }

    public void testCannotRollupWriteableIndex() {
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        // Source index has been created in the setup() method and is empty and still writable
        Exception exception = expectThrows(ElasticsearchException.class, () -> rollup(sourceIndex, rollupIndex, config));
        assertThat(exception.getMessage(), containsString("Rollup requires setting [index.blocks.write = true] for index"));
    }

    public void testCannotRollupMissingIndex() {
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        IndexNotFoundException exception = expectThrows(IndexNotFoundException.class, () -> rollup("missing-index", rollupIndex, config));
        assertEquals("missing-index", exception.getIndex().getName());
        assertThat(exception.getMessage(), containsString("no such index [missing-index]"));
    }

    public void testCannotRollupWhileOtherRollupInProgress() throws Exception {
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field(FIELD_TIMESTAMP, randomDateForInterval(config.getInterval()))
            .field(FIELD_DIMENSION_1, randomAlphaOfLength(1))
            .field(FIELD_NUMERIC_1, randomDouble())
            .endObject();
        bulkIndex(sourceSupplier);
        prepareSourceIndex(sourceIndex);
        var rollupListener = new ActionListener<AcknowledgedResponse>() {
            boolean success;

            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                if (acknowledgedResponse.isAcknowledged()) {
                    success = true;
                } else {
                    fail("Failed to receive rollup acknowledgement");
                }
            }

            @Override
            public void onFailure(Exception e) {
                fail("Rollup failed: " + e.getMessage());
            }
        };
        client().execute(DownsampleAction.INSTANCE, new DownsampleAction.Request(sourceIndex, rollupIndex, config), rollupListener);
        assertBusy(() -> {
            try {
                assertEquals(indicesAdmin().prepareGetIndex().addIndices(rollupIndex).get().getIndices().length, 1);
            } catch (IndexNotFoundException e) {
                fail("rollup index has not been created");
            }
        });
        ResourceAlreadyExistsException exception = expectThrows(
            ResourceAlreadyExistsException.class,
            () -> rollup(sourceIndex, rollupIndex, config)
        );
        assertThat(exception.getMessage(), containsString(rollupIndex));
        // We must wait until the in-progress rollup ends, otherwise data will not be cleaned up
        assertBusy(() -> assertTrue("In progress rollup did not complete", rollupListener.success), 60, TimeUnit.SECONDS);
    }

    public void testRollupDatastream() throws Exception {
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        String dataStreamName = createDataStream();

        final Instant now = Instant.now();
        SourceSupplier sourceSupplier = () -> {
            String ts = randomDateForRange(now.minusSeconds(60 * 60).toEpochMilli(), now.plusSeconds(60 * 60).toEpochMilli());
            return XContentFactory.jsonBuilder()
                .startObject()
                .field(FIELD_TIMESTAMP, ts)
                .field(FIELD_DIMENSION_1, randomFrom(dimensionValues))
                .field(FIELD_NUMERIC_1, randomInt())
                .field(FIELD_NUMERIC_2, DATE_FORMATTER.parseMillis(ts))
                .endObject();
        };
        bulkIndex(dataStreamName, sourceSupplier);

        String sourceIndex = rollover(dataStreamName).getOldIndex();
        prepareSourceIndex(sourceIndex);
        String rollupIndex = "rollup-" + sourceIndex;
        rollup(sourceIndex, rollupIndex, config);
        assertRollupIndex(sourceIndex, rollupIndex, config);

        var r = client().execute(GetDataStreamAction.INSTANCE, new GetDataStreamAction.Request(new String[] { dataStreamName })).get();
        assertEquals(1, r.getDataStreams().size());
        List<Index> indices = r.getDataStreams().get(0).getDataStream().getIndices();
        // Assert that the rollup index has not been added to the data stream
        assertTrue(indices.stream().filter(i -> i.getName().equals(rollupIndex)).toList().isEmpty());
        // Assert that the source index is still a member of the data stream
        assertFalse(indices.stream().filter(i -> i.getName().equals(sourceIndex)).toList().isEmpty());
    }

    public void testCancelRollupIndexer() throws IOException {
        // create rollup config and index documents into source index
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field(FIELD_TIMESTAMP, randomDateForInterval(config.getInterval()))
            .field(FIELD_DIMENSION_1, randomAlphaOfLength(1))
            .field(FIELD_NUMERIC_1, randomDouble())
            .endObject();
        bulkIndex(sourceSupplier);
        prepareSourceIndex(sourceIndex);

        IndicesService indexServices = getInstanceFromNode(IndicesService.class);
        Index srcIndex = resolveIndex(sourceIndex);
        IndexService indexService = indexServices.indexServiceSafe(srcIndex);
        int shardNum = randomIntBetween(0, numOfShards - 1);
        IndexShard shard = indexService.getShard(shardNum);
        RollupShardTask task = new RollupShardTask(
            randomLong(),
            "rollup",
            "action",
            TaskId.EMPTY_TASK_ID,
            rollupIndex,
            config,
            emptyMap(),
            shard.shardId()
        );
        TaskCancelHelper.cancel(task, "test cancel");

        // re-use source index as temp index for test
        RollupShardIndexer indexer = new RollupShardIndexer(
            task,
            client(),
            indexService,
            shard.shardId(),
            rollupIndex,
            config,
            new String[] { FIELD_NUMERIC_1, FIELD_NUMERIC_2 },
            new String[] {}
        );

        TaskCancelledException exception = expectThrows(TaskCancelledException.class, () -> indexer.execute());
        assertThat(exception.getMessage(), equalTo("Shard [" + sourceIndex + "][" + shardNum + "] rollup cancelled"));
    }

    public void testRollupBulkFailed() throws IOException {
        // create rollup config and index documents into source index
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field(FIELD_TIMESTAMP, randomDateForInterval(config.getInterval()))
            .field(FIELD_DIMENSION_1, randomAlphaOfLength(1))
            .field(FIELD_NUMERIC_1, randomDouble())
            .endObject();
        bulkIndex(sourceSupplier);
        prepareSourceIndex(sourceIndex);

        // block rollup index
        assertAcked(
            indicesAdmin().preparePutTemplate(rollupIndex)
                .setPatterns(List.of(rollupIndex))
                .setSettings(Settings.builder().put("index.blocks.write", "true").build())
                .get()
        );

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> rollup(sourceIndex, rollupIndex, config));
        assertThat(exception.getMessage(), equalTo("Unable to rollup index [" + sourceIndex + "]"));
    }

    public void testTooManyBytesInFlight() throws IOException {
        // create rollup config and index documents into source index
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field(FIELD_TIMESTAMP, randomDateForInterval(config.getInterval()))
            .field(FIELD_DIMENSION_1, randomAlphaOfLength(1))
            .field(FIELD_NUMERIC_1, randomDouble())
            .endObject();
        bulkIndex(sourceSupplier);
        prepareSourceIndex(sourceIndex);

        IndicesService indexServices = getInstanceFromNode(IndicesService.class);
        Index srcIndex = resolveIndex(sourceIndex);
        IndexService indexService = indexServices.indexServiceSafe(srcIndex);
        int shardNum = randomIntBetween(0, numOfShards - 1);
        IndexShard shard = indexService.getShard(shardNum);
        RollupShardTask task = new RollupShardTask(
            randomLong(),
            "rollup",
            "action",
            TaskId.EMPTY_TASK_ID,
            rollupIndex,
            config,
            emptyMap(),
            shard.shardId()
        );

        // re-use source index as temp index for test
        RollupShardIndexer indexer = new RollupShardIndexer(
            task,
            client(),
            indexService,
            shard.shardId(),
            rollupIndex,
            config,
            new String[] { FIELD_NUMERIC_1, FIELD_NUMERIC_2 },
            new String[] {}
        );
        /*
         * Here we set the batch size and the total bytes in flight size to tiny numbers so that we are guaranteed to trigger the bulk
         * processor to reject some calls to add(), so that we can make sure RollupShardIndexer keeps trying until success.
         */
        indexer.rollupMaxBytesInFlight = ByteSizeValue.ofBytes(1024);
        indexer.rollupBulkSize = ByteSizeValue.ofBytes(512);
        indexer.execute();
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
        bulkIndex(sourceIndex, sourceSupplier);
    }

    private void bulkIndex(String indexName, SourceSupplier sourceSupplier) throws IOException {
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
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), docsIndexed);
    }

    private void prepareSourceIndex(String sourceIndex) {
        // Set the source index to read-only state
        assertAcked(
            indicesAdmin().prepareUpdateSettings(sourceIndex)
                .setSettings(Settings.builder().put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true).build())
                .get()
        );
    }

    private void rollup(String sourceIndex, String rollupIndex, DownsampleConfig config) {
        assertAcked(
            client().execute(DownsampleAction.INSTANCE, new DownsampleAction.Request(sourceIndex, rollupIndex, config)).actionGet()
        );
    }

    private RolloverResponse rollover(String dataStreamName) throws ExecutionException, InterruptedException {
        RolloverResponse response = indicesAdmin().rolloverIndex(new RolloverRequest(dataStreamName, null)).get();
        assertAcked(response);
        return response;
    }

    private Aggregations aggregate(final String index, AggregationBuilder aggregationBuilder) {
        return client().prepareSearch(index).addAggregation(aggregationBuilder).get().getAggregations();
    }

    @SuppressWarnings("unchecked")
    private void assertRollupIndex(String sourceIndex, String rollupIndex, DownsampleConfig config) throws IOException {
        // Retrieve field information for the metric fields
        final GetMappingsResponse getMappingsResponse = indicesAdmin().prepareGetMappings(sourceIndex).get();
        final Map<String, Object> sourceIndexMappings = getMappingsResponse.mappings()
            .entrySet()
            .stream()
            .filter(entry -> sourceIndex.equals(entry.getKey()))
            .findFirst()
            .map(mappingMetadata -> mappingMetadata.getValue().sourceAsMap())
            .orElseThrow(() -> new IllegalArgumentException("No mapping found for rollup source index [" + sourceIndex + "]"));

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

        assertRollupIndexAggregations(sourceIndex, rollupIndex, config, metricFields, labelFields);

        GetIndexResponse indexSettingsResp = indicesAdmin().prepareGetIndex().addIndices(sourceIndex, rollupIndex).get();
        assertRollupIndexSettings(sourceIndex, rollupIndex, indexSettingsResp);

        Map<String, Map<String, Object>> mappings = (Map<String, Map<String, Object>>) indexSettingsResp.getMappings()
            .get(rollupIndex)
            .getSourceAsMap()
            .get("properties");

        assertFieldMappings(config, metricFields, mappings);

        GetMappingsResponse indexMappings = indicesAdmin().getMappings(new GetMappingsRequest().indices(rollupIndex, sourceIndex))
            .actionGet();
        Map<String, String> rollupIndexProperties = (Map<String, String>) indexMappings.mappings()
            .get(rollupIndex)
            .sourceAsMap()
            .get("properties");
        Map<String, String> sourceIndexCloneProperties = (Map<String, String>) indexMappings.mappings()
            .get(sourceIndex)
            .sourceAsMap()
            .get("properties");
        List<Map.Entry<String, String>> labelFieldRollupIndexCloneProperties = (rollupIndexProperties.entrySet()
            .stream()
            .filter(entry -> labelFields.containsKey(entry.getKey()))
            .toList());
        List<Map.Entry<String, String>> labelFieldSourceIndexProperties = (sourceIndexCloneProperties.entrySet()
            .stream()
            .filter(entry -> labelFields.containsKey(entry.getKey()))
            .toList());
        assertEquals(labelFieldRollupIndexCloneProperties, labelFieldSourceIndexProperties);
    }

    private void assertRollupIndexAggregations(
        String sourceIndex,
        String rollupIndex,
        DownsampleConfig config,
        Map<String, TimeSeriesParams.MetricType> metricFields,
        Map<String, String> labelFields
    ) {
        final AggregationBuilder aggregations = buildAggregations(config, metricFields, labelFields, config.getTimestampField());
        Aggregations origResp = aggregate(sourceIndex, aggregations);
        Aggregations rollupResp = aggregate(rollupIndex, aggregations);
        assertEquals(origResp.asMap().keySet(), rollupResp.asMap().keySet());

        StringTerms originalTsIdTermsAggregation = (StringTerms) origResp.getAsMap().values().stream().toList().get(0);
        StringTerms rollupTsIdTermsAggregation = (StringTerms) rollupResp.getAsMap().values().stream().toList().get(0);
        originalTsIdTermsAggregation.getBuckets().forEach(originalBucket -> {

            StringTerms.Bucket rollupBucket = rollupTsIdTermsAggregation.getBucketByKey(originalBucket.getKeyAsString());
            assertEquals(originalBucket.getAggregations().asList().size(), rollupBucket.getAggregations().asList().size());

            InternalDateHistogram originalDateHistogram = (InternalDateHistogram) originalBucket.getAggregations().asList().get(0);
            InternalDateHistogram rollupDateHistogram = (InternalDateHistogram) rollupBucket.getAggregations().asList().get(0);
            List<InternalDateHistogram.Bucket> originalDateHistogramBuckets = originalDateHistogram.getBuckets();
            List<InternalDateHistogram.Bucket> rollupDateHistogramBuckets = rollupDateHistogram.getBuckets();
            assertEquals(originalDateHistogramBuckets.size(), rollupDateHistogramBuckets.size());
            assertEquals(
                originalDateHistogramBuckets.stream().map(InternalDateHistogram.Bucket::getKeyAsString).collect(Collectors.toList()),
                rollupDateHistogramBuckets.stream().map(InternalDateHistogram.Bucket::getKeyAsString).collect(Collectors.toList())
            );

            for (int i = 0; i < originalDateHistogramBuckets.size(); ++i) {
                InternalDateHistogram.Bucket originalDateHistogramBucket = originalDateHistogramBuckets.get(i);
                InternalDateHistogram.Bucket rollupDateHistogramBucket = rollupDateHistogramBuckets.get(i);
                assertEquals(originalDateHistogramBucket.getKeyAsString(), rollupDateHistogramBucket.getKeyAsString());

                Aggregations originalAggregations = originalDateHistogramBucket.getAggregations();
                Aggregations rollupAggregations = rollupDateHistogramBucket.getAggregations();
                assertEquals(originalAggregations.asList().size(), rollupAggregations.asList().size());

                List<Aggregation> nonTopHitsOriginalAggregations = originalAggregations.asList()
                    .stream()
                    .filter(agg -> agg.getType().equals("top_hits") == false)
                    .toList();
                List<Aggregation> nonTopHitsRollupAggregations = rollupAggregations.asList()
                    .stream()
                    .filter(agg -> agg.getType().equals("top_hits") == false)
                    .toList();
                assertEquals(nonTopHitsOriginalAggregations, nonTopHitsRollupAggregations);

                List<Aggregation> topHitsOriginalAggregations = originalAggregations.asList()
                    .stream()
                    .filter(agg -> agg.getType().equals("top_hits"))
                    .toList();
                List<Aggregation> topHitsRollupAggregations = rollupAggregations.asList()
                    .stream()
                    .filter(agg -> agg.getType().equals("top_hits"))
                    .toList();
                assertEquals(topHitsRollupAggregations.size(), topHitsRollupAggregations.size());

                for (int j = 0; j < topHitsRollupAggregations.size(); ++j) {
                    InternalTopHits originalTopHits = (InternalTopHits) topHitsOriginalAggregations.get(j);
                    InternalTopHits rollupTopHits = (InternalTopHits) topHitsRollupAggregations.get(j);
                    SearchHit[] originalHits = originalTopHits.getHits().getHits();
                    SearchHit[] rollupHits = rollupTopHits.getHits().getHits();
                    assertEquals(originalHits.length, rollupHits.length);

                    for (int k = 0; k < originalHits.length; ++k) {
                        SearchHit originalHit = originalHits[k];
                        SearchHit rollupHit = rollupHits[k];

                        Map<String, DocumentField> originalHitDocumentFields = originalHit.getDocumentFields();
                        Map<String, DocumentField> rollupHitDocumentFields = rollupHit.getDocumentFields();
                        List<DocumentField> originalFields = originalHitDocumentFields.values().stream().toList();
                        List<DocumentField> rollupFields = rollupHitDocumentFields.values().stream().toList();
                        List<Object> originalFieldsList = originalFields.stream().flatMap(x -> x.getValues().stream()).toList();
                        List<Object> rollupFieldsList = rollupFields.stream().flatMap(x -> x.getValues().stream()).toList();
                        if (originalFieldsList.isEmpty() == false && rollupFieldsList.isEmpty() == false) {
                            // NOTE: here we take advantage of the fact that a label field is indexed also as a metric of type
                            // `counter`. This way we can actually check that the label value stored in the rollup index
                            // is the last value (which is what we store for a metric of type counter) by comparing the metric
                            // field value to the label field value.
                            originalFieldsList.forEach(
                                field -> assertTrue(
                                    "Field [" + field + "] is not included in the rollup fields: " + rollupFieldsList,
                                    rollupFieldsList.contains(field)
                                )
                            );
                            rollupFieldsList.forEach(
                                field -> assertTrue(
                                    "Field [" + field + "] is not included in the source fields: " + originalFieldsList,
                                    originalFieldsList.contains(field)
                                )
                            );
                            Object originalLabelValue = originalHit.getDocumentFields().values().stream().toList().get(0).getValue();
                            Object rollupLabelValue = rollupHit.getDocumentFields().values().stream().toList().get(0).getValue();
                            Optional<Aggregation> labelAsMetric = nonTopHitsOriginalAggregations.stream()
                                .filter(agg -> agg.getName().equals("metric_" + rollupTopHits.getName()))
                                .findFirst();
                            // NOTE: this check is possible only if the label can be indexed as a metric (the label is a numeric field)
                            if (labelAsMetric.isPresent()) {
                                double metricValue = ((Max) labelAsMetric.get()).value();
                                assertEquals(metricValue, rollupLabelValue);
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

    private void assertRollupIndexSettings(String sourceIndex, String rollupIndex, GetIndexResponse indexSettingsResp) {
        Settings sourceSettings = indexSettingsResp.settings().get(sourceIndex);
        Settings rollupSettings = indexSettingsResp.settings().get(rollupIndex);

        // Assert rollup metadata are set in index settings
        assertEquals("success", rollupSettings.get(IndexMetadata.INDEX_DOWNSAMPLE_STATUS_KEY));

        assertNotNull(sourceSettings.get(IndexMetadata.SETTING_INDEX_UUID));
        assertNotNull(rollupSettings.get(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_UUID_KEY));
        assertEquals(
            sourceSettings.get(IndexMetadata.SETTING_INDEX_UUID),
            rollupSettings.get(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_UUID_KEY)
        );

        assertEquals(sourceIndex, rollupSettings.get(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_NAME_KEY));
        assertEquals(sourceSettings.get(IndexSettings.MODE.getKey()), rollupSettings.get(IndexSettings.MODE.getKey()));

        assertNotNull(sourceSettings.get(IndexSettings.TIME_SERIES_START_TIME.getKey()));
        assertNotNull(rollupSettings.get(IndexSettings.TIME_SERIES_START_TIME.getKey()));
        assertEquals(
            sourceSettings.get(IndexSettings.TIME_SERIES_START_TIME.getKey()),
            rollupSettings.get(IndexSettings.TIME_SERIES_START_TIME.getKey())
        );

        assertNotNull(sourceSettings.get(IndexSettings.TIME_SERIES_END_TIME.getKey()));
        assertNotNull(rollupSettings.get(IndexSettings.TIME_SERIES_END_TIME.getKey()));
        assertEquals(
            sourceSettings.get(IndexSettings.TIME_SERIES_END_TIME.getKey()),
            rollupSettings.get(IndexSettings.TIME_SERIES_END_TIME.getKey())
        );
        assertNotNull(sourceSettings.get(IndexMetadata.INDEX_ROUTING_PATH.getKey()));
        assertNotNull(rollupSettings.get(IndexMetadata.INDEX_ROUTING_PATH.getKey()));
        assertEquals(
            sourceSettings.get(IndexMetadata.INDEX_ROUTING_PATH.getKey()),
            rollupSettings.get(IndexMetadata.INDEX_ROUTING_PATH.getKey())
        );

        assertNotNull(sourceSettings.get(IndexMetadata.SETTING_NUMBER_OF_SHARDS));
        assertNotNull(rollupSettings.get(IndexMetadata.SETTING_NUMBER_OF_SHARDS));
        assertEquals(
            sourceSettings.get(IndexMetadata.SETTING_NUMBER_OF_SHARDS),
            rollupSettings.get(IndexMetadata.SETTING_NUMBER_OF_SHARDS)
        );

        assertNotNull(sourceSettings.get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS));
        assertNotNull(rollupSettings.get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS));
        assertEquals(
            sourceSettings.get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS),
            rollupSettings.get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS)
        );

        assertEquals("true", rollupSettings.get(IndexMetadata.SETTING_BLOCKS_WRITE));
        assertEquals(sourceSettings.get(IndexMetadata.SETTING_INDEX_HIDDEN), rollupSettings.get(IndexMetadata.SETTING_INDEX_HIDDEN));

        if (sourceSettings.keySet().contains(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey())) {
            assertNotNull(indexSettingsResp.getSetting(rollupIndex, MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey()));
        }

        if (sourceSettings.keySet().contains(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey())
            && rollupSettings.keySet().contains(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey())) {
            assertEquals(
                sourceSettings.get(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey()),
                rollupSettings.get(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey())
            );
        } else {
            assertFalse(sourceSettings.keySet().contains(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey()));
            assertFalse(rollupSettings.keySet().contains(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey()));
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

        ComposableIndexTemplate template = new ComposableIndexTemplate(
            List.of(dataStreamName + "*"),
            indexTemplate,
            null,
            null,
            null,
            null,
            new ComposableIndexTemplate.DataStreamTemplate(false, false),
            null
        );
        PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request(dataStreamName + "_template")
            .indexTemplate(template);
        assertAcked(client().execute(PutComposableIndexTemplateAction.INSTANCE, request).actionGet());
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, new CreateDataStreamAction.Request(dataStreamName)).get());
        return dataStreamName;
    }

    public void testConcurrentRollup() throws IOException, InterruptedException {
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
        docCount = 512; // Hard code to have 512 documents in the source index, otherwise running this test take too long.
        bulkIndex(sourceIndex, sourceSupplier);
        prepareSourceIndex(sourceIndex);

        int n = randomIntBetween(3, 6);
        final CountDownLatch rollupComplete = new CountDownLatch(n);
        final List<String> targets = new ArrayList<>();
        final List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            final String targetIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
            targets.add(targetIndex);
            threads.add(new Thread(() -> {
                rollup(sourceIndex, targetIndex, config);
                rollupComplete.countDown();
            }));
        }
        for (int i = 0; i < n; i++) {
            threads.get(i).start();
        }

        assertTrue(rollupComplete.await(30, TimeUnit.SECONDS));

        for (int i = 0; i < n; i++) {
            assertRollupIndex(sourceIndex, targets.get(i), config);
        }
    }
}
