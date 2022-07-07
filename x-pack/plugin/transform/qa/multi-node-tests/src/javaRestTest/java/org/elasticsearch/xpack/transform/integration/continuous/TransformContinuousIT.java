/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration.continuous;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.client.transform.DeleteTransformRequest;
import org.elasticsearch.client.transform.GetTransformRequest;
import org.elasticsearch.client.transform.GetTransformResponse;
import org.elasticsearch.client.transform.GetTransformStatsRequest;
import org.elasticsearch.client.transform.GetTransformStatsResponse;
import org.elasticsearch.client.transform.PutTransformRequest;
import org.elasticsearch.client.transform.StartTransformRequest;
import org.elasticsearch.client.transform.StartTransformResponse;
import org.elasticsearch.client.transform.StopTransformRequest;
import org.elasticsearch.client.transform.StopTransformResponse;
import org.elasticsearch.client.transform.transforms.TransformConfig;
import org.elasticsearch.client.transform.transforms.TransformStats;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;

/**
 * Test runner for testing continuous transforms, testing
 *
 *  - continuous mode with several checkpoints created
 *  - correctness of results
 *  - optimizations (minimal necessary writes)
 *  - permutations of features (index settings, aggs, data types, index or data stream)
 *
 *  All test cases are executed with one runner in parallel to save runtime, indexing would otherwise
 *  cause overlong runtime.
 *
 *  In a nutshell the test works like this:
 *
 *   - create a base index with randomized settings
 *   - create test data including missing values
 *   - create 1 transform per test case
 *     - the transform config has common settings:
 *       - sync config for continuous mode
 *       - page size 10 to trigger paging
 *       - count field to test how many buckets
 *       - max run field to check what was the highest run field, see below for more details
 *       - a test ingest pipeline
 *    - execute 10 rounds ("run"):
 *      - set run = #round
 *      - update the ingest pipeline to set run.ingest = run
 *      - shuffle test data
 *      - create a random number of documents:
 *        - randomly draw from the 1st half of the test data to create documents
 *        - add a run field, so we know which run the data point has been created
 *      - start all transforms and wait until it processed the data
 *      - stop transforms
 *      - run the test
 *        - aggregate data on source index and compare it with the cont index
 *        - using "run.max" its possible to check the highest run from the source
 *        - using "run.ingest" its possible to check when the transform re-creates the document,
 *          to check that optimizations worked
 *      - repeat
 */
@SuppressWarnings("removal")
public class TransformContinuousIT extends ESRestTestCase {

    private List<ContinuousTestCase> transformTestCases = new ArrayList<>();

    @Before
    public void setClusterSettings() throws IOException {
        // Make sure we never retry on failure to speed up the test
        // Set logging level to trace
        // see: https://github.com/elastic/elasticsearch/issues/45562
        Request addFailureRetrySetting = new Request("PUT", "/_cluster/settings");
        addFailureRetrySetting.setJsonEntity(
            "{\"persistent\": {\"xpack.transform.num_transform_failure_retries\": \""
                + 0
                + "\","
                + "\"logger.org.elasticsearch.action.bulk\": \"info\","
                + // reduces bulk failure spam
                "\"logger.org.elasticsearch.xpack.core.indexing.AsyncTwoPhaseIndexer\": \"debug\","
                + "\"logger.org.elasticsearch.xpack.transform\": \"debug\"}}"
        );
        client().performRequest(addFailureRetrySetting);
    }

    @Before
    public void registerTestCases() {
        addTestCaseIfNotDisabled(new TermsGroupByIT());
        addTestCaseIfNotDisabled(new TermsOnDateGroupByIT());
        addTestCaseIfNotDisabled(new DateHistogramGroupByIT());
        addTestCaseIfNotDisabled(new DateHistogramGroupByOtherTimeFieldIT());
        addTestCaseIfNotDisabled(new HistogramGroupByIT());
        addTestCaseIfNotDisabled(new LatestContinuousIT());
    }

    @Before
    public void createPipelines() throws IOException {
        createOrUpdatePipeline(ContinuousTestCase.INGEST_RUN_FIELD, 0);
    }

    @After
    public void removeAllTransforms() throws IOException {
        for (TransformConfig config : getTransforms().getTransformConfigurations()) {
            deleteTransform(config.getId(), true);
        }
    }

    @After
    public void removePipelines() throws IOException {
        deletePipeline(ContinuousTestCase.INGEST_PIPELINE);
    }

    public void testContinousEvents() throws Exception {
        String sourceIndexName = ContinuousTestCase.CONTINUOUS_EVENTS_SOURCE_INDEX;
        DecimalFormat numberFormat = new DecimalFormat("000", new DecimalFormatSymbols(Locale.ROOT));
        String dateType = randomBoolean() ? "date_nanos" : "date";
        boolean isDataStream = randomBoolean();
        int runs = 10;

        // generate event id's to group on
        List<String> events = new ArrayList<>();
        events.add(null);
        for (int i = 0; i < 100; i++) {
            events.add("event_" + numberFormat.format(i));
        }

        // generate metric buckets to group on by histogram
        List<Integer> metric_bucket = new ArrayList<>();
        metric_bucket.add(null);
        for (int i = 0; i < 100; i++) {
            metric_bucket.add(Integer.valueOf(i * 100));
        }

        // generate locations to group on by geo location
        List<Tuple<Integer, Integer>> locations = new ArrayList<>();
        locations.add(null);
        for (int i = 0; i < 20; i++) {
            for (int j = 0; j < 20; j++) {
                locations.add(new Tuple<>(i * 9 - 90, j * 18 - 180));
            }
        }

        // generate date id's to group on
        List<String> dates = new ArrayList<>();
        dates.add(null);
        for (int i = 0; i < 100; i++) {
            dates.add(
                // create a random date between 1/1/2001 and 1/1/2006
                ContinuousTestCase.STRICT_DATE_OPTIONAL_TIME_PRINTER_NANOS.withZone(ZoneId.of("UTC"))
                    .format(Instant.ofEpochMilli(randomLongBetween(978307200000L, 1136073600000L)))
            );
        }

        putIndex(sourceIndexName, dateType, isDataStream);
        // create all transforms to test
        createTransforms();

        for (int run = 0; run < runs; run++) {
            Instant runDate = Instant.now();

            createOrUpdatePipeline(ContinuousTestCase.INGEST_RUN_FIELD, run);

            // shuffle the list to draw randomly from the first x entries (that way we do not update all entities in 1 run)
            Collections.shuffle(events, random());
            Collections.shuffle(metric_bucket, random());
            Collections.shuffle(locations, random());
            Collections.shuffle(dates, random());

            final StringBuilder source = new StringBuilder();
            BulkRequest bulkRequest = new BulkRequest(sourceIndexName);

            int numDocs = randomIntBetween(1000, 20000);
            Set<String> modifiedEvents = new HashSet<>();
            for (int numDoc = 0; numDoc < numDocs; numDoc++) {
                source.append("{");

                String event = events.get((numDoc + randomIntBetween(0, 50)) % 50);
                modifiedEvents.add(event);
                if (event != null) {
                    source.append("\"event\":\"").append(event).append("\",");
                }

                Integer metric = metric_bucket.get((numDoc + randomIntBetween(0, 50)) % 50);
                if (metric != null) {
                    // randomize, but ensure it falls into the same bucket
                    int randomizedMetric = run * ContinuousTestCase.METRIC_TREND + metric + randomIntBetween(0, 99);
                    source.append("\"metric\":").append(randomizedMetric).append(",");
                }

                Tuple<Integer, Integer> location = locations.get((numDoc + randomIntBetween(0, 200)) % 200);
                if (location != null) {
                    // randomize within the same bucket
                    int randomizedLat = location.v1() + randomIntBetween(0, 9);
                    int randomizedLon = location.v2() + randomIntBetween(0, 17);
                    source.append("\"location\":\"").append(randomizedLat + "," + randomizedLon).append("\",");
                }

                String date = dates.get((numDoc + randomIntBetween(0, 50)) % 50);
                if (date != null) {
                    source.append("\"some-timestamp\":\"").append(date).append("\",");
                }

                // simulate a different timestamp that is off from the timestamp used for sync, so it can fall into the previous bucket
                String metricDateString = ContinuousTestCase.STRICT_DATE_OPTIONAL_TIME_PRINTER_NANOS.withZone(ZoneId.of("UTC"))
                    .format(runDate.minusSeconds(randomIntBetween(0, 2)).plusNanos(randomIntBetween(0, 999999)));
                source.append("\"metric-timestamp\":\"").append(metricDateString).append("\",");

                String dateString = ContinuousTestCase.STRICT_DATE_OPTIONAL_TIME_PRINTER_NANOS.withZone(ZoneId.of("UTC"))
                    .format(runDate.plusNanos(randomIntBetween(0, 999999)));

                source.append("\"timestamp\":\"").append(dateString).append("\",");
                // for data streams
                source.append("\"@timestamp\":\"").append(dateString).append("\",");
                source.append("\"run\":").append(run);
                source.append("}");

                bulkRequest.add(new IndexRequest().create(true).source(source.toString(), XContentType.JSON));
                source.setLength(0);
                if (numDoc % 100 == 0) {
                    bulkIndex(bulkRequest);
                    bulkRequest = new BulkRequest(sourceIndexName);
                }
            }
            if (source.length() > 0) {
                bulkIndex(bulkRequest);
            }
            refreshIndex(sourceIndexName);

            // start all transforms, wait until the processed all data and stop them
            startTransforms();

            waitUntilTransformsProcessedNewData(ContinuousTestCase.SYNC_DELAY, run);
            stopTransforms();

            // test the output
            for (ContinuousTestCase testCase : transformTestCases) {
                try {
                    testCase.testIteration(run, modifiedEvents);
                } catch (AssertionError testFailure) {
                    throw new AssertionError(
                        "Error in test case ["
                            + testCase.getName()
                            + "]."
                            + "If you want to mute the test, please mute ["
                            + testCase.getClass().getName()
                            + "] only, but _not_ ["
                            + this.getClass().getName()
                            + "] as a whole.",
                        testFailure
                    );
                }
            }
        }
    }

    /**
     * Create the transform source index with randomized settings to increase test coverage, for example
     * index sorting, triggers query optimizations.
     */
    private void putIndex(String indexName, String dateType, boolean isDataStream) throws IOException {
        List<String> sortedFields = Collections.emptyList();

        // create mapping and settings
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.startObject("settings").startObject("index");
                builder.field("number_of_shards", randomIntBetween(1, 5));
                if (randomBoolean()) {
                    builder.field("codec", "best_compression");
                }
                if (randomBoolean()) {
                    sortedFields = new ArrayList<>(
                        // note: no index sort for geo_point
                        randomUnique(() -> randomFrom("event", "metric", "run", "timestamp"), randomIntBetween(1, 3))
                    );
                    Collections.shuffle(sortedFields, random());
                    List<String> sortOrders = randomList(sortedFields.size(), sortedFields.size(), () -> randomFrom("asc", "desc"));

                    builder.field("sort.field", sortedFields);
                    builder.field("sort.order", sortOrders);
                    if (randomBoolean()) {
                        builder.field(
                            "sort.missing",
                            randomList(sortedFields.size(), sortedFields.size(), () -> randomFrom("_last", "_first"))
                        );
                    }
                }
                builder.endObject().endObject();
                builder.startObject("mappings").startObject("properties");
                builder.startObject("timestamp").field("type", dateType);
                if (dateType.equals("date_nanos")) {
                    builder.field("format", "strict_date_optional_time_nanos");
                }
                builder.endObject();

                // gh#72741 : index sort does not support unsigned_long
                final String metricType = sortedFields.contains("metric")
                    ? randomFrom("integer", "long")
                    : randomFrom("integer", "long", "unsigned_long");

                builder.startObject("event")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("metric")
                    .field("type", metricType)
                    .endObject()
                    .startObject("location")
                    .field("type", "geo_point")
                    .endObject()
                    .startObject("run")
                    .field("type", "integer")
                    .endObject()
                    .startObject("metric-timestamp")
                    .field("type", dateType)
                    .endObject()
                    .startObject("some-timestamp")
                    .field("type", dateType)
                    .endObject();

                builder.endObject(); // properties

                // add some runtime fields
                builder.startObject("runtime");

                builder.startObject("metric-rt-2x")
                    .field("type", "long")
                    .startObject("script")
                    .field("source", "if (params._source.metric != null) {emit(params._source.metric * 2)}")
                    .endObject()
                    .endObject()
                    .startObject("event-upper")
                    .field("type", "keyword")
                    .startObject("script")
                    .field("source", "if (params._source.event != null) {emit(params._source.event.toUpperCase())}")
                    .endObject()
                    .endObject()
                    .startObject("timestamp-at-runtime")
                    .field("type", "date")
                    .startObject("script")
                    .field("source", "emit(parse(params._source.get('timestamp')))")
                    .endObject()
                    .endObject()
                    .startObject("metric-timestamp-5m-earlier")
                    .field("type", "date")
                    .startObject("script")
                    .field(
                        "source",
                        "if (doc['metric-timestamp'].size()!=0) {emit(doc['metric-timestamp'].value"
                            + ".minus(5, ChronoUnit.MINUTES).toInstant().toEpochMilli())}"
                    )
                    .endObject()
                    .endObject()
                    .startObject("some-timestamp-10m-earlier")
                    .field("type", "date")
                    .startObject("script")
                    .field(
                        "source",
                        "if (doc['some-timestamp'].size()!=0) {emit(doc['some-timestamp'].value"
                            + ".minus(10, ChronoUnit.MINUTES).toInstant().toEpochMilli())}"
                    )
                    .endObject()
                    .endObject();

                // random overlay of existing field, only if its not part of sorted fields
                if (sortedFields.contains("metric") == false && randomBoolean()) {
                    if (randomBoolean()) {
                        builder.startObject("metric").field("type", "long").endObject();
                    } else {
                        builder.startObject("metric")
                            .field("type", "long")
                            .startObject("script")
                            .field("source", "if (params._source.metric != null) {emit(params._source.metric * 3)}")
                            .endObject()
                            .endObject();
                    }
                }

                builder.endObject(); // runtime
                builder.endObject(); // mappings
            }
            builder.endObject();
            String indexSettingsAndMappings = Strings.toString(builder);
            logger.info("Creating source index with: {}", indexSettingsAndMappings);
            if (isDataStream) {
                Request createCompositeTemplate = new Request("PUT", "_index_template/" + indexName + "_template");
                createCompositeTemplate.setJsonEntity(
                    "{\n"
                        + "  \"index_patterns\": [ \""
                        + indexName
                        + "\" ],\n"
                        + "  \"data_stream\": {\n"
                        + "  },\n"
                        + "  \"template\": \n"
                        + indexSettingsAndMappings
                        + "}"
                );
                client().performRequest(createCompositeTemplate);
                client().performRequest(new Request("PUT", "_data_stream/" + indexName));
            } else {
                final StringEntity entity = new StringEntity(indexSettingsAndMappings, ContentType.APPLICATION_JSON);
                Request req = new Request("PUT", indexName);
                req.setEntity(entity);
                client().performRequest(req);
            }
        }
    }

    private void createTransforms() throws IOException {
        for (ContinuousTestCase testCase : transformTestCases) {
            assertTrue(putTransform(testCase.createConfig()).isAcknowledged());
        }
    }

    private void startTransforms() throws IOException {
        for (ContinuousTestCase testCase : transformTestCases) {
            assertTrue(startTransform(testCase.getName()).isAcknowledged());
        }
    }

    private void stopTransforms() throws IOException {
        for (ContinuousTestCase testCase : transformTestCases) {
            assertTrue(stopTransform(testCase.getName(), true, null, false).isAcknowledged());
        }
    }

    private void createOrUpdatePipeline(String field, int run) throws IOException {
        XContentBuilder pipeline = jsonBuilder().startObject()
            .startArray("processors")
            .startObject()
            .startObject("set")
            .field("field", field)
            .field("value", run)
            .endObject()
            .endObject()
            .endArray()
            .endObject();

        assertTrue(
            putPipeline(new PutPipelineRequest(ContinuousTestCase.INGEST_PIPELINE, BytesReference.bytes(pipeline), XContentType.JSON))
                .isAcknowledged()
        );

    }

    private GetTransformStatsResponse getTransformStats(String id) throws IOException {
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            return restClient.transform().getTransformStats(new GetTransformStatsRequest(id), RequestOptions.DEFAULT);
        }
    }

    private void waitUntilTransformsProcessedNewData(TimeValue delay, int iteration) throws Exception {
        Instant waitUntil = Instant.now().plusMillis(delay.getMillis());
        logger.info(
            "wait until transform reaches timestamp_millis: {} (takes into account the delay: {}) iteration: {}",
            ContinuousTestCase.STRICT_DATE_OPTIONAL_TIME_PRINTER_NANOS.withZone(ZoneId.of("UTC")).format(waitUntil),
            delay,
            iteration
        );
        for (ContinuousTestCase testCase : transformTestCases) {
            assertBusy(() -> {
                TransformStats stats = getTransformStats(testCase.getName()).getTransformsStats().get(0);
                assertThat(
                    "transform ["
                        + testCase.getName()
                        + "] does not progress, state: "
                        + stats.getState()
                        + ", reason: "
                        + stats.getReason(),
                    stats.getCheckpointingInfo().getLastSearchTime(),
                    greaterThan(waitUntil)
                );
            }, 30, TimeUnit.SECONDS);
        }
    }

    private void addTestCaseIfNotDisabled(ContinuousTestCase testCaseInstance) {
        for (Annotation annotation : testCaseInstance.getClass().getAnnotations()) {
            if (annotation.annotationType().isAssignableFrom(AwaitsFix.class)) {
                logger.warn(
                    "Skipping test case: [{}], because it is disabled, see [{}]",
                    testCaseInstance.getName(),
                    ((AwaitsFix) annotation).bugUrl()
                );
                return;
            }
        }
        transformTestCases.add(testCaseInstance);
    }

    private void bulkIndex(BulkRequest bulkRequest) throws IOException {
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            BulkResponse response = restClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            assertThat(response.buildFailureMessage(), response.hasFailures(), is(false));
        }
    }

    private void refreshIndex(String index) throws IOException {
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            restClient.indices().refresh(new RefreshRequest(index), RequestOptions.DEFAULT);
        }
    }

    private AcknowledgedResponse putTransform(TransformConfig config) throws IOException {
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            return restClient.transform().putTransform(new PutTransformRequest(config), RequestOptions.DEFAULT);

        }
    }

    private org.elasticsearch.action.support.master.AcknowledgedResponse putPipeline(PutPipelineRequest pipeline) throws IOException {
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            return restClient.ingest().putPipeline(pipeline, RequestOptions.DEFAULT);

        }
    }

    private org.elasticsearch.action.support.master.AcknowledgedResponse deletePipeline(String id) throws IOException {
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            return restClient.ingest().deletePipeline(new DeletePipelineRequest(id), RequestOptions.DEFAULT);

        }
    }

    private GetTransformResponse getTransforms() throws IOException {
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            return restClient.transform().getTransform(GetTransformRequest.getAllTransformRequest(), RequestOptions.DEFAULT);
        }
    }

    private StartTransformResponse startTransform(String id) throws IOException {
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            return restClient.transform().startTransform(new StartTransformRequest(id), RequestOptions.DEFAULT);

        }
    }

    private StopTransformResponse stopTransform(String id, boolean waitForCompletion, TimeValue timeout, boolean waitForCheckpoint)
        throws IOException {
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            return restClient.transform()
                .stopTransform(new StopTransformRequest(id, waitForCompletion, timeout, waitForCheckpoint), RequestOptions.DEFAULT);

        }
    }

    private AcknowledgedResponse deleteTransform(String id, boolean force) throws IOException {
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            DeleteTransformRequest deleteRequest = new DeleteTransformRequest(id);
            deleteRequest.setForce(force);
            return restClient.transform().deleteTransform(deleteRequest, RequestOptions.DEFAULT);

        }
    }

    @Override
    protected Settings restClientSettings() {
        final String token = "Basic "
            + Base64.getEncoder().encodeToString(("x_pack_rest_user:x-pack-test-password").getBytes(StandardCharsets.UTF_8));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private static class TestRestHighLevelClient extends RestHighLevelClient {
        private static final List<NamedXContentRegistry.Entry> X_CONTENT_ENTRIES = new SearchModule(
            Settings.EMPTY,
            false,
            Collections.emptyList()
        ).getNamedXContents();

        TestRestHighLevelClient() {
            super(client(), restClient -> {}, X_CONTENT_ENTRIES);
        }
    }
}
