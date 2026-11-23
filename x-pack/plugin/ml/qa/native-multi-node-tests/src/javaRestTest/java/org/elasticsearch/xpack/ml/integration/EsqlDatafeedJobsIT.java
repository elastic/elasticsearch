/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.PreviewDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.ChunkingConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.datafeed.DelayedDataCheckConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.datafeed.extractor.esql.EsqlDataExtractor;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xcontent.json.JsonXContent.jsonXContent;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class EsqlDatafeedJobsIT extends MlNativeAutodetectIntegTestCase {

    private static final TimeValue BUCKET_SPAN = TimeValue.timeValueHours(1);
    // Large epoch-millis anchor: values of 10 digits or fewer are read as seconds by flush's skip_time. It must be a whole
    // multiple of BUCKET_SPAN because the chunked extractor aligns the window to the grouping interval.
    private static final int ROW_LIMIT = Math.toIntExact(EsqlDataExtractor.INJECTED_ROW_LIMIT);
    private static final long BASE_TIME = 472_222 * BUCKET_SPAN.millis();
    private static final long FIRST_WINDOW_END = BASE_TIME + BUCKET_SPAN.millis();
    private static final long SECOND_WINDOW_END = FIRST_WINDOW_END + BUCKET_SPAN.millis();

    @Before
    public void assumeEsqlDatafeedsEnabled() {
        assumeTrue("ES|QL datafeeds feature flag must be enabled", MachineLearning.ESQL_DATAFEEDS_FEATURE_FLAG.isEnabled());
    }

    @After
    public void cleanup() {
        cleanUp();
    }

    public void testEsqlDatafeedWithNoUserLimitShouldKeepAllRowsBelowInjectedCap() throws Exception {
        String index = "esql-no-user-limit";
        createSourceIndex(index);
        indexDocuments(index, 1_500, BASE_TIME + 1_000L, BASE_TIME + 2_000L);

        assertPreviewAndLookbackRowCount("esql-injected-limit", "FROM " + index + " | KEEP event_time, value", 1_500);
    }

    public void testEsqlDatafeedWithUserLimitShouldReturnOnlyLimitedRows() throws Exception {
        String index = "esql-user-limit";
        createSourceIndex(index);
        indexDocuments(index, 1_500, BASE_TIME + 1_000L, BASE_TIME + 2_000L);

        assertPreviewAndLookbackRowCount("esql-user-limit", "FROM " + index + " | KEEP event_time, value | LIMIT 20", 20);
    }

    public void testEsqlDatafeedWithUnshrinkableChunkShouldWarnAndAuditIncompleteChunk() throws Exception {
        String index = "esql-truncation-oracle";
        String jobId = "esql-truncation-job";
        String datafeedId = jobId + "-datafeed";
        createSourceIndex(index);
        indexDocuments(index, 1, BASE_TIME + 1_000L, BASE_TIME + 2_000L);

        Job.Builder job = createJob(jobId);
        putJob(job);
        DatafeedConfig datafeed = createDatafeed(
            datafeedId,
            jobId,
            "FROM " + index + " | KEEP event_time, value",
            ChunkingConfig.newManual(BUCKET_SPAN)
        );
        putDatafeed(datafeed);
        openJob(jobId);

        runLookback(datafeedId, jobId, BASE_TIME, FIRST_WINDOW_END, 1L);

        // One row more than the cap, so the chunk really is truncated.
        indexDocuments(index, ROW_LIMIT + 1, FIRST_WINDOW_END + 1L, FIRST_WINDOW_END + 2L);
        assertThat(previewRowCount(datafeedId, FIRST_WINDOW_END, SECOND_WINDOW_END), equalTo(ROW_LIMIT));
        assertThat(fetchAllAuditMessages(jobId).toString(), not(containsString("reached its row limit")));
        openJob(jobId);
        // the first window's row plus the capped rows of the truncated window
        runLookback(datafeedId, jobId, FIRST_WINDOW_END, SECOND_WINDOW_END, ROW_LIMIT + 1L);
        // the truncated window is posted and audited rather than silently dropped
        assertBusy(
            () -> assertThat(fetchAllAuditMessages(jobId).toString(), containsString("reached its row limit")),
            60,
            TimeUnit.SECONDS
        );
    }

    /**
     * When the row cap is hit, the rows that survive must be the earliest by time (the injected cap follows the time
     * sort). Documents are indexed latest-first so that a cap applied before the sort would keep the latest rows
     * in index order instead.
     */
    public void testEsqlDatafeedAtRowCapShouldKeepEarliestRowsByTime() throws Exception {
        String index = "esql-cap-order-oracle";
        String jobId = "esql-cap-order-job";
        String datafeedId = jobId + "-datafeed";
        createSourceIndex(index);
        int totalDocs = ROW_LIMIT + 50;
        long firstEventTime = BASE_TIME + 1_000L;
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < totalDocs; i++) {
            long eventTime = firstEventTime + (totalDocs - 1 - i);
            bulk.add(new IndexRequest(index).source("source_time", firstEventTime, "event_time", eventTime, "value", i));
        }
        BulkResponse bulkResponse = bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        for (BulkItemResponse item : bulkResponse) {
            assertFalse(item.getFailureMessage(), item.isFailed());
        }

        putJob(createJob(jobId));
        putDatafeed(
            createDatafeed(datafeedId, jobId, "FROM " + index + " | KEEP event_time, value", ChunkingConfig.newManual(BUCKET_SPAN))
        );

        List<Map<String, Object>> rows = previewRows(datafeedId, BASE_TIME, FIRST_WINDOW_END);

        assertThat(rows.size(), equalTo(ROW_LIMIT));
        LongSummaryStatistics eventTimes = rows.stream().mapToLong(row -> ((Number) row.get("event_time")).longValue()).summaryStatistics();
        assertThat(eventTimes.getMin(), equalTo(firstEventTime));
        assertThat(eventTimes.getMax(), equalTo(firstEventTime + ROW_LIMIT - 1L));
    }

    private void assertPreviewAndLookbackRowCount(String jobId, String query, long expectedRecords) throws Exception {
        String datafeedId = jobId + "-datafeed";
        putJob(createJob(jobId));
        // ES|QL datafeeds require chunking (grouping-interval alignment and truncation detection depend on it);
        // a manual chunk spanning the whole test window behaves like no chunking here.
        putDatafeed(createDatafeed(datafeedId, jobId, query, ChunkingConfig.newManual(BUCKET_SPAN)));

        assertThat(previewRowCount(datafeedId, BASE_TIME, FIRST_WINDOW_END), equalTo((int) expectedRecords));
        openJob(jobId);
        runLookback(datafeedId, jobId, BASE_TIME, FIRST_WINDOW_END, expectedRecords);
        assertThat(fetchAllAuditMessages(jobId).toString(), not(containsString("reached its row limit")));
    }

    private Job.Builder createJob(String jobId) {
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(
            Collections.singletonList(new Detector.Builder("count", null).build())
        );
        analysisConfig.setBucketSpan(BUCKET_SPAN);
        DataDescription.Builder dataDescription = new DataDescription.Builder().setTimeField("event_time");
        return new Job.Builder(jobId).setAnalysisConfig(analysisConfig).setDataDescription(dataDescription);
    }

    private DatafeedConfig createDatafeed(String datafeedId, String jobId, String query, ChunkingConfig chunkingConfig) {
        // These tests exercise LIMIT injection and chunk-truncation auditing, not delayed
        // data detection, which for ES|QL datafeeds separately requires summary_count_field_name.
        return new DatafeedConfig.Builder(datafeedId, jobId).setEsqlQuery(query)
            .setSourceTimeField("source_time")
            .setGroupingInterval(BUCKET_SPAN)
            .setChunkingConfig(chunkingConfig)
            .setDelayedDataCheckConfig(DelayedDataCheckConfig.disabledDelayedDataCheckConfig())
            .build();
    }

    private int previewRowCount(String datafeedId, long start, long end) throws Exception {
        return previewRows(datafeedId, start, end).size();
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> previewRows(String datafeedId, long start, long end) throws Exception {
        PreviewDatafeedAction.Response response = client().execute(
            PreviewDatafeedAction.INSTANCE,
            new PreviewDatafeedAction.Request(datafeedId, Long.toString(start), Long.toString(end))
        ).actionGet();
        try (var parser = jsonXContent.createParser(xContentRegistry(), null, new BytesArray(Strings.toString(response)).streamInput())) {
            List<Map<String, Object>> rows = new ArrayList<>();
            for (Object row : parser.list()) {
                rows.add((Map<String, Object>) row);
            }
            return rows;
        }
    }

    private void runLookback(String datafeedId, String jobId, long start, long end, long expectedRecords) throws Exception {
        startDatafeed(datafeedId, start, end);
        assertBusy(() -> {
            DataCounts dataCounts = getJobStats(jobId).get(0).getDataCounts();
            assertThat(dataCounts.getProcessedRecordCount(), equalTo(expectedRecords));
            GetDatafeedsStatsAction.Response stats = client().execute(
                GetDatafeedsStatsAction.INSTANCE,
                new GetDatafeedsStatsAction.Request(datafeedId)
            ).actionGet();
            assertThat(stats.getResponse().results().get(0).getDatafeedState(), equalTo(DatafeedState.STOPPED));
        }, 120, TimeUnit.SECONDS);
        waitUntilJobIsClosed(jobId, TimeValue.timeValueSeconds(60));
    }

    private void createSourceIndex(String index) {
        // Pin dates to epoch_millis: the default format would parse a bare 4-digit number such as 1000 as an ISO year.
        client().admin()
            .indices()
            .prepareCreate(index)
            .setMapping("source_time", "type=date,format=epoch_millis", "event_time", "type=date,format=epoch_millis", "value", "type=long")
            .get();
    }

    private void indexDocuments(String index, int count, long sourceTime, long eventTime) {
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < count; i++) {
            bulk.add(new IndexRequest(index).source("source_time", sourceTime, "event_time", eventTime, "value", i));
        }
        BulkResponse response = bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        for (BulkItemResponse item : response) {
            assertFalse(item.getFailureMessage(), item.isFailed());
        }
    }
}
