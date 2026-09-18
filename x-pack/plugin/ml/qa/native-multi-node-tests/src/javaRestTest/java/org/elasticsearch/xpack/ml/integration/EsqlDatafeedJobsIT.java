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
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.PreviewDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.ChunkingConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.datafeed.DelayedDataCheckConfig;
import org.elasticsearch.xpack.core.ml.datafeed.EsqlDatafeedSourceCheckpoint;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xcontent.json.JsonXContent.jsonXContent;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class EsqlDatafeedJobsIT extends MlNativeAutodetectIntegTestCase {

    private static final TimeValue BUCKET_SPAN = TimeValue.timeValueHours(1);
    // A realistic (large) epoch-millis anchor for all absolute timestamps used by these tests. Values near
    // the epoch (e.g. plain "0", "1000") are ambiguous to more than one piece of ML/ES code that accepts
    // either seconds or milliseconds: FlushJobParams#parseTimeParam (via TimeUtils#dateStringToEpoch) treats
    // any numeric string of 10 digits or fewer as epoch *seconds* and multiplies by 1000, which silently
    // inflates a small millis value like the datafeed's window bounds a thousandfold when DatafeedJob resumes
    // a lookback via flushJob's skip_time. Anchoring to a real-looking (13-digit) millis timestamp keeps every
    // absolute time value used here safely past that ambiguity. It must also be a whole multiple of BUCKET_SPAN:
    // ChunkedDataExtractorFactory aligns the extraction window to the datafeed's grouping interval by rounding
    // the start up (alignToCeil) and the end down (alignToFloor), so an unaligned anchor can silently round
    // start and end to the same instant and collapse the window to zero width.
    private static final long BASE_TIME = 472_222 * BUCKET_SPAN.millis();
    private static final long FIRST_WINDOW_END = BASE_TIME + BUCKET_SPAN.millis();
    private static final long SECOND_WINDOW_END = FIRST_WINDOW_END + BUCKET_SPAN.millis();

    @Before
    public void enableEsqlDatafeeds() throws Exception {
        // xpack.ml.esql_datafeeds.enabled is read from cluster-state metadata (see
        // MachineLearning#isEsqlDatafeedsEnabled), not from node-local elasticsearch.yml settings,
        // so it must be toggled at runtime rather than via the shared cluster's static config.
        updateClusterSettings(Settings.builder().put(MachineLearning.ESQL_DATAFEEDS_ENABLED.getKey(), true));
    }

    @After
    public void cleanup() {
        // Undo the @Before toggle so it doesn't leak into other tests sharing this cluster.
        updateClusterSettings(Settings.builder().putNull(MachineLearning.ESQL_DATAFEEDS_ENABLED.getKey()));
        cleanUp();
    }

    public void testEsqlDatafeedWithNoUserLimitShouldPreviewAndExecuteAllRows() throws Exception {
        String index = "esql-limit-oracle";
        createSourceIndex(index);
        indexDocuments(index, 1_500, BASE_TIME + 1_000L, BASE_TIME + 2_000L);

        assertLimitOracle(index, "esql-injected-limit", "FROM esql-limit-oracle | KEEP event_time, value", 1_500);
        assertLimitOracle(index, "esql-user-limit", "FROM esql-limit-oracle | KEEP event_time, value | LIMIT 20", 20);
    }

    public void testEsqlDatafeedWithUnshrinkableChunkShouldWarnAndHoldCheckpoint() throws Exception {
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
            "FROM esql-truncation-oracle | KEEP event_time, value",
            ChunkingConfig.newManual(BUCKET_SPAN)
        );
        putDatafeed(datafeed);
        openJob(jobId);

        runLookback(datafeedId, jobId, BASE_TIME, FIRST_WINDOW_END, 1L);
        assertThat(sourceCheckpointEnd(jobId), equalTo(FIRST_WINDOW_END));

        indexDocuments(index, 10_000, FIRST_WINDOW_END + 1L, FIRST_WINDOW_END + 2L);
        List<String> auditMessagesBeforePreview = fetchAllAuditMessages(jobId);
        assertThat(previewRowCount(datafeedId, FIRST_WINDOW_END, SECOND_WINDOW_END), equalTo(10_000));
        assertThat(fetchAllAuditMessages(jobId), equalTo(auditMessagesBeforePreview));
        openJob(jobId);
        runLookback(datafeedId, jobId, FIRST_WINDOW_END, SECOND_WINDOW_END, 10_001L);
        assertThat(sourceCheckpointEnd(jobId), equalTo(FIRST_WINDOW_END));
        assertBusy(
            () -> assertThat(fetchAllAuditMessages(jobId).toString(), containsString("chunker could not cover the interval")),
            60,
            TimeUnit.SECONDS
        );
    }

    private void assertLimitOracle(String index, String jobId, String query, long expectedRecords) throws Exception {
        String datafeedId = jobId + "-datafeed";
        Job.Builder job = createJob(jobId);
        putJob(job);
        // ES|QL datafeeds require chunking to remain enabled (grouping-interval alignment and
        // truncation detection depend on it); a manual chunk spanning the whole test window behaves
        // like no chunking for these single-window assertions.
        DatafeedConfig datafeed = createDatafeed(datafeedId, jobId, query, ChunkingConfig.newManual(BUCKET_SPAN));
        putDatafeed(datafeed);

        // putJob() audits "Job created" asynchronously, after the PUT job API already returned. Wait for
        // it to land before taking the baseline snapshot below, otherwise it can race with the immediately
        // following preview call and make it look like preview itself produced a new audit message.
        AtomicReference<List<String>> auditMessagesBeforePreviewRef = new AtomicReference<>();
        assertBusy(() -> {
            List<String> messages = fetchAllAuditMessages(jobId);
            assertThat(messages, not(empty()));
            auditMessagesBeforePreviewRef.set(messages);
        });
        List<String> auditMessagesBeforePreview = auditMessagesBeforePreviewRef.get();
        assertThat(previewRowCount(datafeedId, BASE_TIME, FIRST_WINDOW_END), equalTo((int) expectedRecords));
        assertThat(fetchAllAuditMessages(jobId), equalTo(auditMessagesBeforePreview));
        openJob(jobId);
        runLookback(datafeedId, jobId, BASE_TIME, FIRST_WINDOW_END, expectedRecords);
        assertThat(fetchAllAuditMessages(jobId).toString(), not(containsString("chunker could not cover the interval")));
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
        // These tests exercise LIMIT injection and chunk-truncation/checkpoint-hold behavior, not delayed
        // data detection, which for ES|QL datafeeds separately requires summary_count_field_name.
        return new DatafeedConfig.Builder(datafeedId, jobId).setEsqlQuery(query)
            .setSourceTimeField("source_time")
            .setGroupingInterval(BUCKET_SPAN)
            .setChunkingConfig(chunkingConfig)
            .setDelayedDataCheckConfig(DelayedDataCheckConfig.disabledDelayedDataCheckConfig())
            .build();
    }

    private int previewRowCount(String datafeedId, long start, long end) throws Exception {
        PreviewDatafeedAction.Response response = client().execute(
            PreviewDatafeedAction.INSTANCE,
            new PreviewDatafeedAction.Request(datafeedId, Long.toString(start), Long.toString(end))
        ).actionGet();
        try (
            var parser = jsonXContent.createParser(
                xContentRegistry(),
                null,
                new BytesArray(org.elasticsearch.common.Strings.toString(response)).streamInput()
            )
        ) {
            return parser.list().size();
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

    private long sourceCheckpointEnd(String jobId) {
        Map<String, Object> source = client().get(
            new GetRequest(AnomalyDetectorsIndex.jobResultsAliasedName(jobId), EsqlDatafeedSourceCheckpoint.documentId(jobId))
        ).actionGet().getSourceAsMap();
        return ((Number) source.get(EsqlDatafeedSourceCheckpoint.SOURCE_END_MS.getPreferredName())).longValue();
    }

    private void createSourceIndex(String index) {
        // Pin both date fields to epoch_millis. Elasticsearch's default date format
        // (strict_date_optional_time||epoch_millis) tries strict_date_optional_time first, which accepts a
        // bare 4-digit number as an ISO year -- so small numeric offsets from the epoch used by these tests
        // (e.g. 1000, 2000) get silently parsed as the years 1000/2000 instead of milliseconds, landing the
        // stored value far outside any window these tests query.
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
