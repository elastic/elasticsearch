/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration.continuous;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transform.transforms.SettingsConfig;
import org.elasticsearch.client.transform.transforms.SyncConfig;
import org.elasticsearch.client.transform.transforms.TimeSyncConfig;
import org.elasticsearch.client.transform.transforms.TransformConfig;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

public abstract class ContinuousTestCase extends ESRestTestCase {

    public static final TimeValue SYNC_DELAY = new TimeValue(1, TimeUnit.SECONDS);
    public static final int METRIC_TREND = 5000;
    public static final String CONTINUOUS_EVENTS_SOURCE_INDEX = "test-transform-continuous-events";
    public static final String INGEST_PIPELINE = "transform-ingest";
    public static final String MAX_RUN_FIELD = "run.max";
    public static final String INGEST_RUN_FIELD = "run_ingest";
    // mixture of fields to choose from, indexed and runtime
    public static final Set<String> METRIC_FIELDS = Set.of("metric", "metric-rt-2x");
    public static final Set<String> METRIC_TIMESTAMP_FIELDS = Set.of("metric-timestamp", "metric-timestamp-5m-earlier");
    public static final Set<String> TERMS_FIELDS = Set.of("event", "event-upper");
    public static final Set<String> TIMESTAMP_FIELDS = Set.of("timestamp", "timestamp-at-runtime");
    public static final Set<String> OTHER_TIMESTAMP_FIELDS = Set.of("some-timestamp", "some-timestamp-10m-earlier");
    public static final DateTimeFormatter STRICT_DATE_OPTIONAL_TIME_PRINTER_NANOS = new DateTimeFormatterBuilder().parseCaseInsensitive()
        .append(ISO_LOCAL_DATE)
        .appendLiteral('T')
        .appendValue(HOUR_OF_DAY, 2)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2)
        .appendFraction(NANO_OF_SECOND, 3, 9, true)
        .appendOffsetId()
        .toFormatter(Locale.ROOT);

    /**
     * Get the name of the transform/test
     *
     * @return name of the transform(used for start/stop)
     */
    public abstract String getName();

    /**
     * Create the transform configuration for the test.
     *
     * @return the transform configuration
     */
    public abstract TransformConfig createConfig();

    /**
     * Test results after 1 iteration in the test runner.
     *
     * @param iteration the current iteration
     * @param modifiedEvents set of events modified in the current iteration
     */
    public abstract void testIteration(int iteration, Set<String> modifiedEvents) throws IOException;

    protected TransformConfig.Builder addCommonBuilderParameters(TransformConfig.Builder builder) {
        return builder.setSyncConfig(getSyncConfig())
            .setSettings(addCommonSetings(new SettingsConfig.Builder()).build())
            .setFrequency(SYNC_DELAY);
    }

    protected AggregatorFactories.Builder addCommonAggregations(AggregatorFactories.Builder builder) {
        builder.addAggregator(AggregationBuilders.max(MAX_RUN_FIELD).field("run"))
            .addAggregator(AggregationBuilders.count("count").field("run"))
            .addAggregator(AggregationBuilders.max("time.max").field("timestamp"));
        return builder;
    }

    protected SettingsConfig.Builder addCommonSetings(SettingsConfig.Builder builder) {
        // enforce paging, to see we run through all of the options
        builder.setMaxPageSearchSize(10);
        return builder;
    }

    protected SearchResponse search(SearchRequest searchRequest) throws IOException {
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            return restClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            logger.error("Search failed with an exception.", e);
            throw e;
        }
    }

    @Override
    protected Settings restClientSettings() {
        final String token = "Basic "
            + Base64.getEncoder().encodeToString(("x_pack_rest_user:x-pack-test-password").getBytes(StandardCharsets.UTF_8));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private static class TestRestHighLevelClient extends RestHighLevelClient {
        private static final List<NamedXContentRegistry.Entry> X_CONTENT_ENTRIES = new SearchModule(Settings.EMPTY, Collections.emptyList())
            .getNamedXContents();

        TestRestHighLevelClient() {
            super(client(), restClient -> {}, X_CONTENT_ENTRIES);
        }
    }

    private SyncConfig getSyncConfig() {
        return TimeSyncConfig.builder().setField("timestamp").setDelay(SYNC_DELAY).build();
    }
}
