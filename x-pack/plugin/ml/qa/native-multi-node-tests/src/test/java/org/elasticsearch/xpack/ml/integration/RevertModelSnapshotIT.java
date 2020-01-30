/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * This test pushes data through a job in 2 runs creating
 * 2 model snapshots. It then reverts to the earlier snapshot
 * and asserts the reversion worked as expected.
 */
public class RevertModelSnapshotIT extends MlNativeAutodetectIntegTestCase {

    @After
    public void tearDownData() throws Exception {
        cleanUp();
    }

    public void test() throws Exception {
        TimeValue bucketSpan = TimeValue.timeValueHours(1);
        long startTime = 1491004800000L;

        Job.Builder job = buildAndRegisterJob("revert-model-snapshot-it-job", bucketSpan);
        openJob(job.getId());
        postData(job.getId(), generateData(startTime, bucketSpan, 10, Arrays.asList("foo"),
                (bucketIndex, series) -> bucketIndex == 5 ? 100.0 : 10.0).stream().collect(Collectors.joining()));
        flushJob(job.getId(), true);
        closeJob(job.getId());

        ModelSizeStats modelSizeStats1 = getJobStats(job.getId()).get(0).getModelSizeStats();
        Quantiles quantiles1 = getQuantiles(job.getId());

        List<Bucket> midwayBuckets = getBuckets(job.getId());
        Bucket revertPointBucket = midwayBuckets.get(midwayBuckets.size() - 1);
        assertThat(revertPointBucket.isInterim(), is(true));

        // We need to wait a second to ensure the second time around model snapshot will have a different ID (it depends on epoch seconds)
        waitUntil(() -> false, 1, TimeUnit.SECONDS);

        openJob(job.getId());
        postData(job.getId(), generateData(startTime + 10 * bucketSpan.getMillis(), bucketSpan, 10, Arrays.asList("foo", "bar"),
                (bucketIndex, series) -> 10.0).stream().collect(Collectors.joining()));
        closeJob(job.getId());

        ModelSizeStats modelSizeStats2 = getJobStats(job.getId()).get(0).getModelSizeStats();
        Quantiles quantiles2 = getQuantiles(job.getId());

        // Check model has grown since a new series was introduced
        assertThat(modelSizeStats2.getModelBytes(), greaterThan(modelSizeStats1.getModelBytes()));

        // Check quantiles have changed
        assertThat(quantiles2, not(equalTo(quantiles1)));

        List<Bucket> finalPreRevertBuckets = getBuckets(job.getId());
        Bucket finalPreRevertPointBucket = finalPreRevertBuckets.get(midwayBuckets.size() - 1);
        assertThat(finalPreRevertPointBucket.isInterim(), is(false));

        List<ModelSnapshot> modelSnapshots = getModelSnapshots(job.getId());
        assertThat(modelSnapshots.size(), equalTo(2));

        // Snapshots are sorted in descending timestamp order so we revert to the last of the list/earliest.
        assertThat(modelSnapshots.get(0).getTimestamp().getTime(), greaterThan(modelSnapshots.get(1).getTimestamp().getTime()));
        assertThat(getJob(job.getId()).get(0).getModelSnapshotId(), equalTo(modelSnapshots.get(0).getSnapshotId()));
        ModelSnapshot revertSnapshot = modelSnapshots.get(1);

        assertThat(revertModelSnapshot(job.getId(), revertSnapshot.getSnapshotId()).status(), equalTo(RestStatus.OK));

        // Check model_size_stats has been reverted
        assertThat(getJobStats(job.getId()).get(0).getModelSizeStats().getModelBytes(), equalTo(modelSizeStats1.getModelBytes()));

        // Check quantiles have been reverted
        assertThat(getQuantiles(job.getId()).getTimestamp(), equalTo(revertSnapshot.getLatestResultTimeStamp()));

        // Re-run 2nd half of data
        openJob(job.getId());
        postData(job.getId(), generateData(startTime + 10 * bucketSpan.getMillis(), bucketSpan, 10, Arrays.asList("foo", "bar"),
                (bucketIndex, series) -> 10.0).stream().collect(Collectors.joining()));
        closeJob(job.getId());

        List<Bucket> finalPostRevertBuckets = getBuckets(job.getId());
        Bucket finalPostRevertPointBucket = finalPostRevertBuckets.get(midwayBuckets.size() - 1);
        assertThat(finalPostRevertPointBucket.getTimestamp(), equalTo(finalPreRevertPointBucket.getTimestamp()));
        assertThat(finalPostRevertPointBucket.getAnomalyScore(), equalTo(finalPreRevertPointBucket.getAnomalyScore()));
        assertThat(finalPostRevertPointBucket.getEventCount(), equalTo(finalPreRevertPointBucket.getEventCount()));
    }

    private Job.Builder buildAndRegisterJob(String jobId, TimeValue bucketSpan) throws Exception {
        Detector.Builder detector = new Detector.Builder("mean", "value");
        detector.setPartitionFieldName("series");
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Arrays.asList(detector.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        Job.Builder job = new Job.Builder(jobId);
        job.setAnalysisConfig(analysisConfig);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        job.setDataDescription(dataDescription);
        registerJob(job);
        putJob(job);
        return job;
    }

    private static List<String> generateData(long timestamp, TimeValue bucketSpan, int bucketCount, List<String> series,
                              BiFunction<Integer, String, Double> timeAndSeriesToValueFunction) throws IOException {
        List<String> data = new ArrayList<>();
        long now = timestamp;
        for (int i = 0; i < bucketCount; i++) {
            for (String field : series) {
                Map<String, Object> record = new HashMap<>();
                record.put("time", now);
                record.put("value", timeAndSeriesToValueFunction.apply(i, field));
                record.put("series", field);
                data.add(createJsonRecord(record));

                record = new HashMap<>();
                record.put("time", now + bucketSpan.getMillis() / 2);
                record.put("value", timeAndSeriesToValueFunction.apply(i, field));
                record.put("series", field);
                data.add(createJsonRecord(record));
            }
            now += bucketSpan.getMillis();
        }
        return data;
    }

    private Quantiles getQuantiles(String jobId) {
        SearchResponse response = client().prepareSearch(".ml-state")
                .setQuery(QueryBuilders.idsQuery().addIds(Quantiles.documentId(jobId)))
                .setSize(1)
                .get();
        SearchHits hits = response.getHits();
        assertThat(hits.getTotalHits().value, equalTo(1L));
        try {
            XContentParser parser = JsonXContent.jsonXContent
                    .createParser(null, LoggingDeprecationHandler.INSTANCE, hits.getAt(0).getSourceAsString());
            return Quantiles.LENIENT_PARSER.apply(parser, null);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
