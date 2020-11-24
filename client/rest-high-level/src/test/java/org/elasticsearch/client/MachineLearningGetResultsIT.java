/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.core.PageParams;
import org.elasticsearch.client.ml.GetBucketsRequest;
import org.elasticsearch.client.ml.GetBucketsResponse;
import org.elasticsearch.client.ml.GetCategoriesRequest;
import org.elasticsearch.client.ml.GetCategoriesResponse;
import org.elasticsearch.client.ml.GetModelSnapshotsRequest;
import org.elasticsearch.client.ml.GetModelSnapshotsResponse;
import org.elasticsearch.client.ml.GetInfluencersRequest;
import org.elasticsearch.client.ml.GetInfluencersResponse;
import org.elasticsearch.client.ml.GetOverallBucketsRequest;
import org.elasticsearch.client.ml.GetOverallBucketsResponse;
import org.elasticsearch.client.ml.GetRecordsRequest;
import org.elasticsearch.client.ml.GetRecordsResponse;
import org.elasticsearch.client.ml.PutJobRequest;
import org.elasticsearch.client.ml.job.config.AnalysisConfig;
import org.elasticsearch.client.ml.job.config.DataDescription;
import org.elasticsearch.client.ml.job.config.Detector;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.client.ml.job.process.ModelSizeStats;
import org.elasticsearch.client.ml.job.results.AnomalyRecord;
import org.elasticsearch.client.ml.job.results.Bucket;
import org.elasticsearch.client.ml.job.results.Influencer;
import org.elasticsearch.client.ml.job.results.OverallBucket;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class MachineLearningGetResultsIT extends ESRestHighLevelClientTestCase {

    private static final String RESULTS_INDEX = ".ml-anomalies-shared";

    private static final String JOB_ID = "get-results-it-job";

    // 2018-08-01T00:00:00Z
    private static final long START_TIME_EPOCH_MS = 1533081600000L;

    private Stats bucketStats = new Stats();
    private Stats recordStats = new Stats();

    @Before
    public void createJobAndIndexResults() throws IOException {
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        Job job = buildJob(JOB_ID);
        machineLearningClient.putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        long time = START_TIME_EPOCH_MS;
        long endTime = time + 3600000L * 24 * 10; // 10 days of hourly buckets
        while (time < endTime) {
            addBucketIndexRequest(time, false, bulkRequest);
            addRecordIndexRequests(time, false, bulkRequest);
            time += 3600000L;
        }

        // Also index an interim bucket
        addBucketIndexRequest(time, true, bulkRequest);
        addRecordIndexRequest(time, true, bulkRequest);

        highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);
    }

    private void addBucketIndexRequest(long timestamp, boolean isInterim, BulkRequest bulkRequest) {
        IndexRequest indexRequest = new IndexRequest(RESULTS_INDEX);
        double bucketScore = randomDoubleBetween(0.0, 100.0, true);
        bucketStats.report(bucketScore);
        indexRequest.source("{\"job_id\":\"" + JOB_ID + "\", \"result_type\":\"bucket\", \"timestamp\": " + timestamp + "," +
                "\"bucket_span\": 3600,\"is_interim\": " + isInterim + ", \"anomaly_score\": " + bucketScore +
                ", \"bucket_influencers\":[{\"job_id\": \"" + JOB_ID + "\", \"result_type\":\"bucket_influencer\", " +
                "\"influencer_field_name\": \"bucket_time\", \"timestamp\": " + timestamp + ", \"bucket_span\": 3600, " +
                "\"is_interim\": " + isInterim + "}]}", XContentType.JSON);
        bulkRequest.add(indexRequest);
    }

    private void addRecordIndexRequests(long timestamp, boolean isInterim, BulkRequest bulkRequest) {
        if (randomBoolean()) {
            return;
        }
        int recordCount = randomIntBetween(1, 3);
        for (int i = 0; i < recordCount; ++i) {
            addRecordIndexRequest(timestamp, isInterim, bulkRequest);
        }
    }

    private void addRecordIndexRequest(long timestamp, boolean isInterim, BulkRequest bulkRequest) {
        IndexRequest indexRequest = new IndexRequest(RESULTS_INDEX);
        double recordScore = randomDoubleBetween(0.0, 100.0, true);
        recordStats.report(recordScore);
        double p = randomDoubleBetween(0.0, 0.05, false);
        indexRequest.source("{\"job_id\":\"" + JOB_ID + "\", \"result_type\":\"record\", \"timestamp\": " + timestamp + "," +
                "\"bucket_span\": 3600,\"is_interim\": " + isInterim + ", \"record_score\": " + recordScore + ", \"probability\": "
                + p + "}", XContentType.JSON);
        bulkRequest.add(indexRequest);
    }

    private void addCategoryIndexRequest(long categoryId, String categoryName, BulkRequest bulkRequest) {
        IndexRequest indexRequest = new IndexRequest(RESULTS_INDEX);
        indexRequest.source("{\"job_id\":\"" + JOB_ID + "\", \"category_id\": " + categoryId + ", \"terms\": \"" +
            categoryName + "\",  \"regex\": \".*?" + categoryName + ".*\", \"max_matching_length\": 3, \"examples\": [\"" +
            categoryName + "\"]}", XContentType.JSON);
        bulkRequest.add(indexRequest);
    }

    private void addCategoriesIndexRequests(BulkRequest bulkRequest) {

        List<String> categories = Arrays.asList("AAL", "JZA", "JBU");

        for (int i = 0; i < categories.size(); i++) {
            addCategoryIndexRequest(i+1, categories.get(i), bulkRequest);
        }
    }

    private void addModelSnapshotIndexRequests(BulkRequest bulkRequest) {
        {
            // Index a number of model snapshots, one of which contains the new model_size_stats fields
            // 'model_bytes_exceeded' and 'model_bytes_memory_limit' that were introduced in 7.2.0.
            // We want to verify that we can parse the snapshots whether or not these fields are present.
            IndexRequest indexRequest = new IndexRequest(RESULTS_INDEX);
            indexRequest.source("{\"job_id\":\"" + JOB_ID + "\", \"timestamp\":1541587919000, " +
                "\"description\":\"State persisted due to job close at 2018-11-07T10:51:59+0000\", \"snapshot_id\":\"1541587919\"," +
                "\"snapshot_doc_count\":1, \"model_size_stats\":{\"job_id\":\"" + JOB_ID + "\", \"result_type\":\"model_size_stats\"," +
                "\"model_bytes\":51722, \"peak_model_bytes\":61322, \"model_bytes_exceeded\":10762, \"model_bytes_memory_limit\":40960," +
                "\"total_by_field_count\":3, \"total_over_field_count\":0, \"total_partition_field_count\":2," +
                "\"bucket_allocation_failures_count\":0, \"memory_status\":\"ok\", \"log_time\":1541587919000," +
                " \"timestamp\":1519930800000},\"latest_record_time_stamp\":1519931700000, \"latest_result_time_stamp\":1519930800000," +
                " \"retain\":false }", XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        {
            IndexRequest indexRequest = new IndexRequest(RESULTS_INDEX);
            indexRequest.source("{\"job_id\":\"" + JOB_ID + "\", \"timestamp\":1541588919000, " +
                "\"description\":\"State persisted due to job close at 2018-11-07T11:08:39+0000\", \"snapshot_id\":\"1541588919\"," +
                "\"snapshot_doc_count\":1, \"model_size_stats\":{\"job_id\":\"" + JOB_ID + "\", \"result_type\":\"model_size_stats\"," +
                "\"model_bytes\":51722, \"peak_model_bytes\":61322, \"total_by_field_count\":3, \"total_over_field_count\":0," +
                "\"total_partition_field_count\":2,\"bucket_allocation_failures_count\":0, \"memory_status\":\"ok\"," +
                "\"log_time\":1541588919000,\"timestamp\":1519930800000},\"latest_record_time_stamp\":1519931700000," +
                "\"latest_result_time_stamp\":1519930800000, \"retain\":false }", XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        {
            IndexRequest indexRequest = new IndexRequest(RESULTS_INDEX);
            indexRequest.source("{\"job_id\":\"" + JOB_ID + "\", \"timestamp\":1541589919000, " +
                "\"description\":\"State persisted due to job close at 2018-11-07T11:25:19+0000\", \"snapshot_id\":\"1541589919\"," +
                "\"snapshot_doc_count\":1, \"model_size_stats\":{\"job_id\":\"" + JOB_ID + "\", \"result_type\":\"model_size_stats\"," +
                "\"model_bytes\":51722, \"peak_model_bytes\":61322, \"total_by_field_count\":3, \"total_over_field_count\":0," +
                "\"total_partition_field_count\":2,\"bucket_allocation_failures_count\":0, \"memory_status\":\"ok\"," +
                "\"log_time\":1541589919000,\"timestamp\":1519930800000},\"latest_record_time_stamp\":1519931700000," +
                "\"latest_result_time_stamp\":1519930800000, \"retain\":false }", XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
    }

    @After
    public void deleteJob() throws IOException {
        new MlTestStateCleaner(logger, highLevelClient().machineLearning()).clearMlMetadata();
    }

    public void testGetModelSnapshots() throws IOException {

        // index some model_snapshot results
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        addModelSnapshotIndexRequests(bulkRequest);

        highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);

        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        {
            GetModelSnapshotsRequest request = new GetModelSnapshotsRequest(JOB_ID);
            request.setSort("timestamp");
            request.setDesc(false);
            request.setPageParams(new PageParams(0, 10000));

            GetModelSnapshotsResponse response = execute(request, machineLearningClient::getModelSnapshots,
                machineLearningClient::getModelSnapshotsAsync);

            assertThat(response.count(), equalTo(3L));
            assertThat(response.snapshots().size(), equalTo(3));
            assertThat(response.snapshots().get(0).getJobId(), equalTo(JOB_ID));
            assertThat(response.snapshots().get(0).getSnapshotId(), equalTo("1541587919"));
            assertThat(response.snapshots().get(0).getSnapshotDocCount(), equalTo(1));
            assertThat(response.snapshots().get(0).getDescription(), equalTo("State persisted due to job close at" +
                " 2018-11-07T10:51:59+0000"));
            assertThat(response.snapshots().get(0).getSnapshotDocCount(), equalTo(1));
            assertThat(response.snapshots().get(0).getTimestamp(), equalTo(new Date(1541587919000L)));
            assertThat(response.snapshots().get(0).getLatestRecordTimeStamp(), equalTo(new Date(1519931700000L)));
            assertThat(response.snapshots().get(0).getLatestResultTimeStamp(), equalTo(new Date(1519930800000L)));
            assertThat(response.snapshots().get(0).getModelSizeStats().getJobId(), equalTo(JOB_ID));
            assertThat(response.snapshots().get(0).getModelSizeStats().getModelBytes(), equalTo(51722L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getPeakModelBytes(), equalTo(61322L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getModelBytesExceeded(), equalTo(10762L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getModelBytesMemoryLimit(), equalTo(40960L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getTotalByFieldCount(), equalTo(3L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getTotalOverFieldCount(), equalTo(0L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getTotalPartitionFieldCount(), equalTo(2L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getBucketAllocationFailuresCount(), equalTo(0L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getMemoryStatus(),
                equalTo(ModelSizeStats.MemoryStatus.fromString("ok")));

            assertThat(response.snapshots().get(1).getJobId(), equalTo(JOB_ID));
            assertThat(response.snapshots().get(1).getSnapshotId(), equalTo("1541588919"));
            assertThat(response.snapshots().get(1).getSnapshotDocCount(), equalTo(1));
            assertThat(response.snapshots().get(1).getDescription(), equalTo("State persisted due to job close at" +
                " 2018-11-07T11:08:39+0000"));
            assertThat(response.snapshots().get(1).getSnapshotDocCount(), equalTo(1));
            assertThat(response.snapshots().get(1).getTimestamp(), equalTo(new Date(1541588919000L)));
            assertThat(response.snapshots().get(1).getLatestRecordTimeStamp(), equalTo(new Date(1519931700000L)));
            assertThat(response.snapshots().get(1).getLatestResultTimeStamp(), equalTo(new Date(1519930800000L)));
            assertThat(response.snapshots().get(1).getModelSizeStats().getJobId(), equalTo(JOB_ID));
            assertThat(response.snapshots().get(1).getModelSizeStats().getModelBytes(), equalTo(51722L));
            assertThat(response.snapshots().get(1).getModelSizeStats().getPeakModelBytes(), equalTo(61322L));
            assertThat(response.snapshots().get(1).getModelSizeStats().getModelBytesExceeded(), equalTo(null));
            assertThat(response.snapshots().get(1).getModelSizeStats().getModelBytesMemoryLimit(), equalTo(null));
            assertThat(response.snapshots().get(1).getModelSizeStats().getTotalByFieldCount(), equalTo(3L));
            assertThat(response.snapshots().get(1).getModelSizeStats().getTotalOverFieldCount(), equalTo(0L));
            assertThat(response.snapshots().get(1).getModelSizeStats().getTotalPartitionFieldCount(), equalTo(2L));
            assertThat(response.snapshots().get(1).getModelSizeStats().getBucketAllocationFailuresCount(), equalTo(0L));
            assertThat(response.snapshots().get(1).getModelSizeStats().getMemoryStatus(),
                equalTo(ModelSizeStats.MemoryStatus.fromString("ok")));

            assertThat(response.snapshots().get(2).getJobId(), equalTo(JOB_ID));
            assertThat(response.snapshots().get(2).getSnapshotId(), equalTo("1541589919"));
            assertThat(response.snapshots().get(2).getSnapshotDocCount(), equalTo(1));
            assertThat(response.snapshots().get(2).getDescription(), equalTo("State persisted due to job close at" +
                " 2018-11-07T11:25:19+0000"));
            assertThat(response.snapshots().get(2).getSnapshotDocCount(), equalTo(1));
            assertThat(response.snapshots().get(2).getTimestamp(), equalTo(new Date(1541589919000L)));
            assertThat(response.snapshots().get(2).getLatestRecordTimeStamp(), equalTo(new Date(1519931700000L)));
            assertThat(response.snapshots().get(2).getLatestResultTimeStamp(), equalTo(new Date(1519930800000L)));
            assertThat(response.snapshots().get(2).getModelSizeStats().getJobId(), equalTo(JOB_ID));
            assertThat(response.snapshots().get(2).getModelSizeStats().getModelBytes(), equalTo(51722L));
            assertThat(response.snapshots().get(2).getModelSizeStats().getPeakModelBytes(), equalTo(61322L));
            assertThat(response.snapshots().get(2).getModelSizeStats().getModelBytesExceeded(), equalTo(null));
            assertThat(response.snapshots().get(2).getModelSizeStats().getModelBytesMemoryLimit(), equalTo(null));
            assertThat(response.snapshots().get(2).getModelSizeStats().getTotalByFieldCount(), equalTo(3L));
            assertThat(response.snapshots().get(2).getModelSizeStats().getTotalOverFieldCount(), equalTo(0L));
            assertThat(response.snapshots().get(2).getModelSizeStats().getTotalPartitionFieldCount(), equalTo(2L));
            assertThat(response.snapshots().get(2).getModelSizeStats().getBucketAllocationFailuresCount(), equalTo(0L));
            assertThat(response.snapshots().get(2).getModelSizeStats().getMemoryStatus(),
                equalTo(ModelSizeStats.MemoryStatus.fromString("ok")));
        }
        {
            GetModelSnapshotsRequest request = new GetModelSnapshotsRequest(JOB_ID);
            request.setSort("timestamp");
            request.setDesc(true);
            request.setPageParams(new PageParams(0, 10000));

            GetModelSnapshotsResponse response = execute(request, machineLearningClient::getModelSnapshots,
                machineLearningClient::getModelSnapshotsAsync);

            assertThat(response.count(), equalTo(3L));
            assertThat(response.snapshots().size(), equalTo(3));
            assertThat(response.snapshots().get(2).getJobId(), equalTo(JOB_ID));
            assertThat(response.snapshots().get(2).getSnapshotId(), equalTo("1541587919"));
            assertThat(response.snapshots().get(2).getSnapshotDocCount(), equalTo(1));
            assertThat(response.snapshots().get(2).getDescription(), equalTo("State persisted due to job close at" +
                " 2018-11-07T10:51:59+0000"));
            assertThat(response.snapshots().get(2).getSnapshotDocCount(), equalTo(1));
            assertThat(response.snapshots().get(2).getTimestamp(), equalTo(new Date(1541587919000L)));
            assertThat(response.snapshots().get(2).getLatestRecordTimeStamp(), equalTo(new Date(1519931700000L)));
            assertThat(response.snapshots().get(2).getLatestResultTimeStamp(), equalTo(new Date(1519930800000L)));
            assertThat(response.snapshots().get(2).getModelSizeStats().getJobId(), equalTo(JOB_ID));
            assertThat(response.snapshots().get(2).getModelSizeStats().getModelBytes(), equalTo(51722L));
            assertThat(response.snapshots().get(2).getModelSizeStats().getPeakModelBytes(), equalTo(61322L));
            assertThat(response.snapshots().get(2).getModelSizeStats().getModelBytesExceeded(), equalTo(10762L));
            assertThat(response.snapshots().get(2).getModelSizeStats().getModelBytesMemoryLimit(), equalTo(40960L));
            assertThat(response.snapshots().get(2).getModelSizeStats().getTotalByFieldCount(), equalTo(3L));
            assertThat(response.snapshots().get(2).getModelSizeStats().getTotalOverFieldCount(), equalTo(0L));
            assertThat(response.snapshots().get(2).getModelSizeStats().getTotalPartitionFieldCount(), equalTo(2L));
            assertThat(response.snapshots().get(2).getModelSizeStats().getBucketAllocationFailuresCount(), equalTo(0L));
            assertThat(response.snapshots().get(2).getModelSizeStats().getMemoryStatus(),
                equalTo(ModelSizeStats.MemoryStatus.fromString("ok")));

            assertThat(response.snapshots().get(1).getJobId(), equalTo(JOB_ID));
            assertThat(response.snapshots().get(1).getSnapshotId(), equalTo("1541588919"));
            assertThat(response.snapshots().get(1).getSnapshotDocCount(), equalTo(1));
            assertThat(response.snapshots().get(1).getDescription(), equalTo("State persisted due to job close at" +
                " 2018-11-07T11:08:39+0000"));
            assertThat(response.snapshots().get(1).getSnapshotDocCount(), equalTo(1));
            assertThat(response.snapshots().get(1).getTimestamp(), equalTo(new Date(1541588919000L)));
            assertThat(response.snapshots().get(1).getLatestRecordTimeStamp(), equalTo(new Date(1519931700000L)));
            assertThat(response.snapshots().get(1).getLatestResultTimeStamp(), equalTo(new Date(1519930800000L)));
            assertThat(response.snapshots().get(1).getModelSizeStats().getJobId(), equalTo(JOB_ID));
            assertThat(response.snapshots().get(1).getModelSizeStats().getModelBytes(), equalTo(51722L));
            assertThat(response.snapshots().get(1).getModelSizeStats().getPeakModelBytes(), equalTo(61322L));
            assertThat(response.snapshots().get(1).getModelSizeStats().getModelBytesExceeded(), equalTo(null));
            assertThat(response.snapshots().get(1).getModelSizeStats().getModelBytesMemoryLimit(), equalTo(null));
            assertThat(response.snapshots().get(1).getModelSizeStats().getTotalByFieldCount(), equalTo(3L));
            assertThat(response.snapshots().get(1).getModelSizeStats().getTotalOverFieldCount(), equalTo(0L));
            assertThat(response.snapshots().get(1).getModelSizeStats().getTotalPartitionFieldCount(), equalTo(2L));
            assertThat(response.snapshots().get(1).getModelSizeStats().getBucketAllocationFailuresCount(), equalTo(0L));
            assertThat(response.snapshots().get(1).getModelSizeStats().getMemoryStatus(),
                equalTo(ModelSizeStats.MemoryStatus.fromString("ok")));

            assertThat(response.snapshots().get(0).getJobId(), equalTo(JOB_ID));
            assertThat(response.snapshots().get(0).getSnapshotId(), equalTo("1541589919"));
            assertThat(response.snapshots().get(0).getSnapshotDocCount(), equalTo(1));
            assertThat(response.snapshots().get(0).getDescription(), equalTo("State persisted due to job close at" +
                " 2018-11-07T11:25:19+0000"));
            assertThat(response.snapshots().get(0).getSnapshotDocCount(), equalTo(1));
            assertThat(response.snapshots().get(0).getTimestamp(), equalTo(new Date(1541589919000L)));
            assertThat(response.snapshots().get(0).getLatestRecordTimeStamp(), equalTo(new Date(1519931700000L)));
            assertThat(response.snapshots().get(0).getLatestResultTimeStamp(), equalTo(new Date(1519930800000L)));
            assertThat(response.snapshots().get(0).getModelSizeStats().getJobId(), equalTo(JOB_ID));
            assertThat(response.snapshots().get(0).getModelSizeStats().getModelBytes(), equalTo(51722L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getPeakModelBytes(), equalTo(61322L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getModelBytesExceeded(), equalTo(null));
            assertThat(response.snapshots().get(0).getModelSizeStats().getModelBytesMemoryLimit(), equalTo(null));
            assertThat(response.snapshots().get(0).getModelSizeStats().getTotalByFieldCount(), equalTo(3L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getTotalOverFieldCount(), equalTo(0L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getTotalPartitionFieldCount(), equalTo(2L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getBucketAllocationFailuresCount(), equalTo(0L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getMemoryStatus(),
                equalTo(ModelSizeStats.MemoryStatus.fromString("ok")));
        }
        {
            GetModelSnapshotsRequest request = new GetModelSnapshotsRequest(JOB_ID);
            request.setSort("timestamp");
            request.setDesc(false);
            request.setPageParams(new PageParams(0, 1));

            GetModelSnapshotsResponse response = execute(request, machineLearningClient::getModelSnapshots,
                machineLearningClient::getModelSnapshotsAsync);

            assertThat(response.count(), equalTo(3L));
            assertThat(response.snapshots().size(), equalTo(1));
            assertThat(response.snapshots().get(0).getJobId(), equalTo(JOB_ID));
            assertThat(response.snapshots().get(0).getSnapshotId(), equalTo("1541587919"));
            assertThat(response.snapshots().get(0).getSnapshotDocCount(), equalTo(1));
            assertThat(response.snapshots().get(0).getDescription(), equalTo("State persisted due to job close at" +
                " 2018-11-07T10:51:59+0000"));
            assertThat(response.snapshots().get(0).getSnapshotDocCount(), equalTo(1));
            assertThat(response.snapshots().get(0).getTimestamp(), equalTo(new Date(1541587919000L)));
            assertThat(response.snapshots().get(0).getLatestRecordTimeStamp(), equalTo(new Date(1519931700000L)));
            assertThat(response.snapshots().get(0).getLatestResultTimeStamp(), equalTo(new Date(1519930800000L)));
            assertThat(response.snapshots().get(0).getModelSizeStats().getJobId(), equalTo(JOB_ID));
            assertThat(response.snapshots().get(0).getModelSizeStats().getModelBytes(), equalTo(51722L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getPeakModelBytes(), equalTo(61322L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getModelBytesExceeded(), equalTo(10762L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getModelBytesMemoryLimit(), equalTo(40960L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getTotalByFieldCount(), equalTo(3L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getTotalOverFieldCount(), equalTo(0L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getTotalPartitionFieldCount(), equalTo(2L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getBucketAllocationFailuresCount(), equalTo(0L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getMemoryStatus(),
                equalTo(ModelSizeStats.MemoryStatus.fromString("ok")));
        }
        {
            GetModelSnapshotsRequest request = new GetModelSnapshotsRequest(JOB_ID);
            request.setSort("timestamp");
            request.setDesc(false);
            request.setPageParams(new PageParams(1, 2));

            GetModelSnapshotsResponse response = execute(request, machineLearningClient::getModelSnapshots,
                machineLearningClient::getModelSnapshotsAsync);

            assertThat(response.count(), equalTo(3L));
            assertThat(response.snapshots().size(), equalTo(2));

            assertThat(response.snapshots().get(0).getJobId(), equalTo(JOB_ID));
            assertThat(response.snapshots().get(0).getSnapshotId(), equalTo("1541588919"));
            assertThat(response.snapshots().get(0).getSnapshotDocCount(), equalTo(1));
            assertThat(response.snapshots().get(0).getDescription(), equalTo("State persisted due to job close at" +
                " 2018-11-07T11:08:39+0000"));
            assertThat(response.snapshots().get(0).getSnapshotDocCount(), equalTo(1));
            assertThat(response.snapshots().get(0).getTimestamp(), equalTo(new Date(1541588919000L)));
            assertThat(response.snapshots().get(0).getLatestRecordTimeStamp(), equalTo(new Date(1519931700000L)));
            assertThat(response.snapshots().get(0).getLatestResultTimeStamp(), equalTo(new Date(1519930800000L)));
            assertThat(response.snapshots().get(0).getModelSizeStats().getJobId(), equalTo(JOB_ID));
            assertThat(response.snapshots().get(0).getModelSizeStats().getModelBytes(), equalTo(51722L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getPeakModelBytes(), equalTo(61322L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getModelBytesExceeded(), equalTo(null));
            assertThat(response.snapshots().get(0).getModelSizeStats().getModelBytesMemoryLimit(), equalTo(null));
            assertThat(response.snapshots().get(0).getModelSizeStats().getTotalByFieldCount(), equalTo(3L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getTotalOverFieldCount(), equalTo(0L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getTotalPartitionFieldCount(), equalTo(2L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getBucketAllocationFailuresCount(), equalTo(0L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getMemoryStatus(),
                equalTo(ModelSizeStats.MemoryStatus.fromString("ok")));


            assertThat(response.snapshots().get(1).getJobId(), equalTo(JOB_ID));
            assertThat(response.snapshots().get(1).getSnapshotId(), equalTo("1541589919"));
            assertThat(response.snapshots().get(1).getSnapshotDocCount(), equalTo(1));
            assertThat(response.snapshots().get(1).getDescription(), equalTo("State persisted due to job close at" +
                " 2018-11-07T11:25:19+0000"));
            assertThat(response.snapshots().get(1).getSnapshotDocCount(), equalTo(1));
            assertThat(response.snapshots().get(1).getTimestamp(), equalTo(new Date(1541589919000L)));
            assertThat(response.snapshots().get(1).getLatestRecordTimeStamp(), equalTo(new Date(1519931700000L)));
            assertThat(response.snapshots().get(1).getLatestResultTimeStamp(), equalTo(new Date(1519930800000L)));
            assertThat(response.snapshots().get(1).getModelSizeStats().getJobId(), equalTo(JOB_ID));
            assertThat(response.snapshots().get(1).getModelSizeStats().getModelBytes(), equalTo(51722L));
            assertThat(response.snapshots().get(1).getModelSizeStats().getPeakModelBytes(), equalTo(61322L));
            assertThat(response.snapshots().get(1).getModelSizeStats().getModelBytesExceeded(), equalTo(null));
            assertThat(response.snapshots().get(1).getModelSizeStats().getModelBytesMemoryLimit(), equalTo(null));
            assertThat(response.snapshots().get(1).getModelSizeStats().getTotalByFieldCount(), equalTo(3L));
            assertThat(response.snapshots().get(1).getModelSizeStats().getTotalOverFieldCount(), equalTo(0L));
            assertThat(response.snapshots().get(1).getModelSizeStats().getTotalPartitionFieldCount(), equalTo(2L));
            assertThat(response.snapshots().get(1).getModelSizeStats().getBucketAllocationFailuresCount(), equalTo(0L));
            assertThat(response.snapshots().get(1).getModelSizeStats().getMemoryStatus(),
                equalTo(ModelSizeStats.MemoryStatus.fromString("ok")));
        }
        {
            GetModelSnapshotsRequest request = new GetModelSnapshotsRequest(JOB_ID);
            request.setSnapshotId("1541588919");

            GetModelSnapshotsResponse response = execute(request, machineLearningClient::getModelSnapshots,
                machineLearningClient::getModelSnapshotsAsync);

            assertThat(response.count(), equalTo(1L));
            assertThat(response.snapshots().size(), equalTo(1));

            assertThat(response.snapshots().get(0).getJobId(), equalTo(JOB_ID));
            assertThat(response.snapshots().get(0).getSnapshotId(), equalTo("1541588919"));
            assertThat(response.snapshots().get(0).getSnapshotDocCount(), equalTo(1));
            assertThat(response.snapshots().get(0).getDescription(), equalTo("State persisted due to job close at" +
                " 2018-11-07T11:08:39+0000"));
            assertThat(response.snapshots().get(0).getSnapshotDocCount(), equalTo(1));
            assertThat(response.snapshots().get(0).getTimestamp(), equalTo(new Date(1541588919000L)));
            assertThat(response.snapshots().get(0).getLatestRecordTimeStamp(), equalTo(new Date(1519931700000L)));
            assertThat(response.snapshots().get(0).getLatestResultTimeStamp(), equalTo(new Date(1519930800000L)));
            assertThat(response.snapshots().get(0).getModelSizeStats().getJobId(), equalTo(JOB_ID));
            assertThat(response.snapshots().get(0).getModelSizeStats().getModelBytes(), equalTo(51722L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getPeakModelBytes(), equalTo(61322L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getModelBytesExceeded(), equalTo(null));
            assertThat(response.snapshots().get(0).getModelSizeStats().getModelBytesMemoryLimit(), equalTo(null));
            assertThat(response.snapshots().get(0).getModelSizeStats().getTotalByFieldCount(), equalTo(3L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getTotalOverFieldCount(), equalTo(0L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getTotalPartitionFieldCount(), equalTo(2L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getBucketAllocationFailuresCount(), equalTo(0L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getMemoryStatus(),
                equalTo(ModelSizeStats.MemoryStatus.fromString("ok")));
        }
        {
            GetModelSnapshotsRequest request = new GetModelSnapshotsRequest(JOB_ID);
            request.setSnapshotId("1541586919"); // request a non-existent snapshotId

            GetModelSnapshotsResponse response = execute(request, machineLearningClient::getModelSnapshots,
                machineLearningClient::getModelSnapshotsAsync);

            assertThat(response.count(), equalTo(0L));
            assertThat(response.snapshots().size(), equalTo(0));
        }
        {
            GetModelSnapshotsRequest request = new GetModelSnapshotsRequest(JOB_ID);
            request.setSort("timestamp");
            request.setDesc(false);
            request.setStart("1541586919000");
            request.setEnd("1541589019000");

            GetModelSnapshotsResponse response = execute(request, machineLearningClient::getModelSnapshots,
                machineLearningClient::getModelSnapshotsAsync);

            assertThat(response.count(), equalTo(2L));
            assertThat(response.snapshots().size(), equalTo(2));
            assertThat(response.snapshots().get(0).getJobId(), equalTo(JOB_ID));
            assertThat(response.snapshots().get(0).getSnapshotId(), equalTo("1541587919"));
            assertThat(response.snapshots().get(0).getSnapshotDocCount(), equalTo(1));
            assertThat(response.snapshots().get(0).getDescription(), equalTo("State persisted due to job close at" +
                " 2018-11-07T10:51:59+0000"));
            assertThat(response.snapshots().get(0).getSnapshotDocCount(), equalTo(1));
            assertThat(response.snapshots().get(0).getTimestamp(), equalTo(new Date(1541587919000L)));
            assertThat(response.snapshots().get(0).getLatestRecordTimeStamp(), equalTo(new Date(1519931700000L)));
            assertThat(response.snapshots().get(0).getLatestResultTimeStamp(), equalTo(new Date(1519930800000L)));
            assertThat(response.snapshots().get(0).getModelSizeStats().getJobId(), equalTo(JOB_ID));
            assertThat(response.snapshots().get(0).getModelSizeStats().getModelBytes(), equalTo(51722L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getPeakModelBytes(), equalTo(61322L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getModelBytesExceeded(), equalTo(10762L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getModelBytesMemoryLimit(), equalTo(40960L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getTotalByFieldCount(), equalTo(3L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getTotalOverFieldCount(), equalTo(0L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getTotalPartitionFieldCount(), equalTo(2L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getBucketAllocationFailuresCount(), equalTo(0L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getMemoryStatus(),
                equalTo(ModelSizeStats.MemoryStatus.fromString("ok")));

            assertThat(response.snapshots().get(1).getJobId(), equalTo(JOB_ID));
            assertThat(response.snapshots().get(1).getSnapshotId(), equalTo("1541588919"));
            assertThat(response.snapshots().get(1).getSnapshotDocCount(), equalTo(1));
            assertThat(response.snapshots().get(1).getDescription(), equalTo("State persisted due to job close at" +
                " 2018-11-07T11:08:39+0000"));
            assertThat(response.snapshots().get(1).getSnapshotDocCount(), equalTo(1));
            assertThat(response.snapshots().get(1).getTimestamp(), equalTo(new Date(1541588919000L)));
            assertThat(response.snapshots().get(1).getLatestRecordTimeStamp(), equalTo(new Date(1519931700000L)));
            assertThat(response.snapshots().get(1).getLatestResultTimeStamp(), equalTo(new Date(1519930800000L)));
            assertThat(response.snapshots().get(1).getModelSizeStats().getJobId(), equalTo(JOB_ID));
            assertThat(response.snapshots().get(1).getModelSizeStats().getModelBytes(), equalTo(51722L));
            assertThat(response.snapshots().get(1).getModelSizeStats().getPeakModelBytes(), equalTo(61322L));
            assertThat(response.snapshots().get(1).getModelSizeStats().getModelBytesExceeded(), equalTo(null));
            assertThat(response.snapshots().get(1).getModelSizeStats().getModelBytesMemoryLimit(), equalTo(null));
            assertThat(response.snapshots().get(1).getModelSizeStats().getTotalByFieldCount(), equalTo(3L));
            assertThat(response.snapshots().get(1).getModelSizeStats().getTotalOverFieldCount(), equalTo(0L));
            assertThat(response.snapshots().get(1).getModelSizeStats().getTotalPartitionFieldCount(), equalTo(2L));
            assertThat(response.snapshots().get(1).getModelSizeStats().getBucketAllocationFailuresCount(), equalTo(0L));
            assertThat(response.snapshots().get(1).getModelSizeStats().getMemoryStatus(),
                equalTo(ModelSizeStats.MemoryStatus.fromString("ok")));
        }
        {
            GetModelSnapshotsRequest request = new GetModelSnapshotsRequest(JOB_ID);
            request.setSort("timestamp");
            request.setDesc(false);
            request.setStart("1541589019000");

            GetModelSnapshotsResponse response = execute(request, machineLearningClient::getModelSnapshots,
                machineLearningClient::getModelSnapshotsAsync);

            assertThat(response.count(), equalTo(1L));
            assertThat(response.snapshots().size(), equalTo(1));
            assertThat(response.snapshots().get(0).getJobId(), equalTo(JOB_ID));
            assertThat(response.snapshots().get(0).getSnapshotId(), equalTo("1541589919"));
            assertThat(response.snapshots().get(0).getSnapshotDocCount(), equalTo(1));
            assertThat(response.snapshots().get(0).getDescription(), equalTo("State persisted due to job close at" +
                " 2018-11-07T11:25:19+0000"));
            assertThat(response.snapshots().get(0).getSnapshotDocCount(), equalTo(1));
            assertThat(response.snapshots().get(0).getTimestamp(), equalTo(new Date(1541589919000L)));
            assertThat(response.snapshots().get(0).getLatestRecordTimeStamp(), equalTo(new Date(1519931700000L)));
            assertThat(response.snapshots().get(0).getLatestResultTimeStamp(), equalTo(new Date(1519930800000L)));
            assertThat(response.snapshots().get(0).getModelSizeStats().getJobId(), equalTo(JOB_ID));
            assertThat(response.snapshots().get(0).getModelSizeStats().getModelBytes(), equalTo(51722L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getPeakModelBytes(), equalTo(61322L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getModelBytesExceeded(), equalTo(null));
            assertThat(response.snapshots().get(0).getModelSizeStats().getModelBytesMemoryLimit(), equalTo(null));
            assertThat(response.snapshots().get(0).getModelSizeStats().getTotalByFieldCount(), equalTo(3L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getTotalOverFieldCount(), equalTo(0L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getTotalPartitionFieldCount(), equalTo(2L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getBucketAllocationFailuresCount(), equalTo(0L));
            assertThat(response.snapshots().get(0).getModelSizeStats().getMemoryStatus(),
                equalTo(ModelSizeStats.MemoryStatus.fromString("ok")));
        }
    }

    public void testGetCategories() throws IOException {

        // index some category results
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        addCategoriesIndexRequests(bulkRequest);

        highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);

        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        {
            GetCategoriesRequest request = new GetCategoriesRequest(JOB_ID);
            request.setPageParams(new PageParams(0, 10000));

            GetCategoriesResponse response = execute(request, machineLearningClient::getCategories,
                    machineLearningClient::getCategoriesAsync);

            assertThat(response.count(), equalTo(3L));
            assertThat(response.categories().size(), equalTo(3));
            assertThat(response.categories().get(0).getCategoryId(), equalTo(1L));
            assertThat(response.categories().get(0).getGrokPattern(), equalTo(".*?AAL.*"));
            assertThat(response.categories().get(0).getRegex(), equalTo(".*?AAL.*"));
            assertThat(response.categories().get(0).getTerms(), equalTo("AAL"));

            assertThat(response.categories().get(1).getCategoryId(), equalTo(2L));
            assertThat(response.categories().get(1).getGrokPattern(), equalTo(".*?JZA.*"));
            assertThat(response.categories().get(1).getRegex(), equalTo(".*?JZA.*"));
            assertThat(response.categories().get(1).getTerms(), equalTo("JZA"));

            assertThat(response.categories().get(2).getCategoryId(), equalTo(3L));
            assertThat(response.categories().get(2).getGrokPattern(), equalTo(".*?JBU.*"));
            assertThat(response.categories().get(2).getRegex(), equalTo(".*?JBU.*"));
            assertThat(response.categories().get(2).getTerms(), equalTo("JBU"));
        }
        {
            GetCategoriesRequest request = new GetCategoriesRequest(JOB_ID);
            request.setPageParams(new PageParams(0, 1));

            GetCategoriesResponse response = execute(request, machineLearningClient::getCategories,
                    machineLearningClient::getCategoriesAsync);

            assertThat(response.count(), equalTo(3L));
            assertThat(response.categories().size(), equalTo(1));
            assertThat(response.categories().get(0).getCategoryId(), equalTo(1L));
            assertThat(response.categories().get(0).getGrokPattern(), equalTo(".*?AAL.*"));
            assertThat(response.categories().get(0).getRegex(), equalTo(".*?AAL.*"));
            assertThat(response.categories().get(0).getTerms(), equalTo("AAL"));
        }
        {
            GetCategoriesRequest request = new GetCategoriesRequest(JOB_ID);
            request.setPageParams(new PageParams(1, 2));

            GetCategoriesResponse response = execute(request, machineLearningClient::getCategories,
                    machineLearningClient::getCategoriesAsync);

            assertThat(response.count(), equalTo(3L));
            assertThat(response.categories().size(), equalTo(2));
            assertThat(response.categories().get(0).getCategoryId(), equalTo(2L));
            assertThat(response.categories().get(0).getGrokPattern(), equalTo(".*?JZA.*"));
            assertThat(response.categories().get(0).getRegex(), equalTo(".*?JZA.*"));
            assertThat(response.categories().get(0).getTerms(), equalTo("JZA"));

            assertThat(response.categories().get(1).getCategoryId(), equalTo(3L));
            assertThat(response.categories().get(1).getGrokPattern(), equalTo(".*?JBU.*"));
            assertThat(response.categories().get(1).getRegex(), equalTo(".*?JBU.*"));
            assertThat(response.categories().get(1).getTerms(), equalTo("JBU"));
        }
        {
            GetCategoriesRequest request = new GetCategoriesRequest(JOB_ID);
            request.setCategoryId(0L); // request a non-existent category

            GetCategoriesResponse response = execute(request, machineLearningClient::getCategories,
                    machineLearningClient::getCategoriesAsync);

            assertThat(response.count(), equalTo(0L));
            assertThat(response.categories().size(), equalTo(0));
        }
        {
            GetCategoriesRequest request = new GetCategoriesRequest(JOB_ID);
            request.setCategoryId(1L);

            GetCategoriesResponse response = execute(request, machineLearningClient::getCategories,
                    machineLearningClient::getCategoriesAsync);

            assertThat(response.count(), equalTo(1L));
            assertThat(response.categories().size(), equalTo(1));
            assertThat(response.categories().get(0).getCategoryId(), equalTo(1L));
            assertThat(response.categories().get(0).getGrokPattern(), equalTo(".*?AAL.*"));
            assertThat(response.categories().get(0).getRegex(), equalTo(".*?AAL.*"));
            assertThat(response.categories().get(0).getTerms(), equalTo("AAL"));
        }
        {
            GetCategoriesRequest request = new GetCategoriesRequest(JOB_ID);
            request.setCategoryId(2L);

            GetCategoriesResponse response = execute(request, machineLearningClient::getCategories,
                    machineLearningClient::getCategoriesAsync);

            assertThat(response.count(), equalTo(1L));
            assertThat(response.categories().get(0).getCategoryId(), equalTo(2L));
            assertThat(response.categories().get(0).getGrokPattern(), equalTo(".*?JZA.*"));
            assertThat(response.categories().get(0).getRegex(), equalTo(".*?JZA.*"));
            assertThat(response.categories().get(0).getTerms(), equalTo("JZA"));

        }
        {
            GetCategoriesRequest request = new GetCategoriesRequest(JOB_ID);
            request.setCategoryId(3L);

            GetCategoriesResponse response = execute(request, machineLearningClient::getCategories,
                    machineLearningClient::getCategoriesAsync);

            assertThat(response.count(), equalTo(1L));
            assertThat(response.categories().get(0).getCategoryId(), equalTo(3L));
            assertThat(response.categories().get(0).getGrokPattern(), equalTo(".*?JBU.*"));
            assertThat(response.categories().get(0).getRegex(), equalTo(".*?JBU.*"));
            assertThat(response.categories().get(0).getTerms(), equalTo("JBU"));
        }
    }

    public void testGetBuckets() throws IOException {
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        {
            GetBucketsRequest request = new GetBucketsRequest(JOB_ID);

            GetBucketsResponse response = execute(request, machineLearningClient::getBuckets, machineLearningClient::getBucketsAsync);

            assertThat(response.count(), equalTo(241L));
            assertThat(response.buckets().size(), equalTo(100));
            assertThat(response.buckets().get(0).getTimestamp().getTime(), equalTo(START_TIME_EPOCH_MS));
        }
        {
            GetBucketsRequest request = new GetBucketsRequest(JOB_ID);
            request.setTimestamp("1533081600000");

            GetBucketsResponse response = execute(request, machineLearningClient::getBuckets, machineLearningClient::getBucketsAsync);

            assertThat(response.count(), equalTo(1L));
            assertThat(response.buckets().size(), equalTo(1));
            assertThat(response.buckets().get(0).getTimestamp().getTime(), equalTo(START_TIME_EPOCH_MS));
        }
        {
            GetBucketsRequest request = new GetBucketsRequest(JOB_ID);
            request.setAnomalyScore(75.0);

            GetBucketsResponse response = execute(request, machineLearningClient::getBuckets, machineLearningClient::getBucketsAsync);

            assertThat(response.count(), equalTo(bucketStats.criticalCount));
            assertThat(response.buckets().size(), equalTo((int) Math.min(100, bucketStats.criticalCount)));
            assertThat(response.buckets().stream().anyMatch(b -> b.getAnomalyScore() < 75.0), is(false));
        }
        {
            GetBucketsRequest request = new GetBucketsRequest(JOB_ID);
            request.setExcludeInterim(true);

            GetBucketsResponse response = execute(request, machineLearningClient::getBuckets, machineLearningClient::getBucketsAsync);

            assertThat(response.count(), equalTo(240L));
        }
        {
            GetBucketsRequest request = new GetBucketsRequest(JOB_ID);
            request.setStart("1533081600000");
            request.setEnd("1533092400000");

            GetBucketsResponse response = execute(request, machineLearningClient::getBuckets, machineLearningClient::getBucketsAsync);

            assertThat(response.count(), equalTo(3L));
            assertThat(response.buckets().get(0).getTimestamp().getTime(), equalTo(START_TIME_EPOCH_MS));
            assertThat(response.buckets().get(1).getTimestamp().getTime(), equalTo(START_TIME_EPOCH_MS + 3600000L));
            assertThat(response.buckets().get(2).getTimestamp().getTime(), equalTo(START_TIME_EPOCH_MS + 2 * + 3600000L));
        }
        {
            GetBucketsRequest request = new GetBucketsRequest(JOB_ID);
            request.setPageParams(new PageParams(3, 3));

            GetBucketsResponse response = execute(request, machineLearningClient::getBuckets, machineLearningClient::getBucketsAsync);

            assertThat(response.buckets().size(), equalTo(3));
            assertThat(response.buckets().get(0).getTimestamp().getTime(), equalTo(START_TIME_EPOCH_MS + 3 * 3600000L));
            assertThat(response.buckets().get(1).getTimestamp().getTime(), equalTo(START_TIME_EPOCH_MS + 4 * 3600000L));
            assertThat(response.buckets().get(2).getTimestamp().getTime(), equalTo(START_TIME_EPOCH_MS + 5 * 3600000L));
        }
        {
            GetBucketsRequest request = new GetBucketsRequest(JOB_ID);
            request.setSort("anomaly_score");
            request.setDescending(true);

            GetBucketsResponse response = execute(request, machineLearningClient::getBuckets, machineLearningClient::getBucketsAsync);

            double previousScore = 100.0;
            for (Bucket bucket : response.buckets()) {
                assertThat(bucket.getAnomalyScore(), lessThanOrEqualTo(previousScore));
                previousScore = bucket.getAnomalyScore();
            }
        }
        {
            GetBucketsRequest request = new GetBucketsRequest(JOB_ID);
            // Make sure we get all buckets
            request.setPageParams(new PageParams(0, 10000));
            request.setExpand(true);

            GetBucketsResponse response = execute(request, machineLearningClient::getBuckets, machineLearningClient::getBucketsAsync);

            assertThat(response.buckets().stream().anyMatch(b -> b.getRecords().size() > 0), is(true));
        }
    }

    public void testGetOverallBuckets() throws IOException {
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        GetBucketsRequest getBucketsRequest = new GetBucketsRequest(JOB_ID);
        getBucketsRequest.setPageParams(new PageParams(0, 3));
        List<Bucket> firstBuckets = machineLearningClient.getBuckets(getBucketsRequest, RequestOptions.DEFAULT).buckets();

        String anotherJobId = "test-get-overall-buckets-job";
        Job anotherJob = buildJob(anotherJobId);
        machineLearningClient.putJob(new PutJobRequest(anotherJob), RequestOptions.DEFAULT);

        // Let's index matching buckets with the score being 10.0 higher
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (Bucket bucket : firstBuckets) {
            IndexRequest indexRequest = new IndexRequest(RESULTS_INDEX);
            indexRequest.source("{\"job_id\":\"" + anotherJobId + "\", \"result_type\":\"bucket\", \"timestamp\": " +
                    bucket.getTimestamp().getTime() + "," + "\"bucket_span\": 3600,\"is_interim\": " + bucket.isInterim()
                    + ", \"anomaly_score\": " + String.valueOf(bucket.getAnomalyScore() + 10.0) + "}", XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);

        {
            GetOverallBucketsRequest request = new GetOverallBucketsRequest(JOB_ID, anotherJobId);

            GetOverallBucketsResponse response = execute(request, machineLearningClient::getOverallBuckets,
                    machineLearningClient::getOverallBucketsAsync);

            assertThat(response.count(), equalTo(241L));
            List<OverallBucket> overallBuckets = response.overallBuckets();
            assertThat(overallBuckets.size(), equalTo(241));
            assertThat(overallBuckets.stream().allMatch(b -> b.getBucketSpan() == 3600L), is(true));
            assertThat(overallBuckets.get(0).getTimestamp().getTime(), equalTo(START_TIME_EPOCH_MS));
            assertThat(overallBuckets.get(240).isInterim(), is(true));
        }
        {
            GetOverallBucketsRequest request = new GetOverallBucketsRequest(JOB_ID, anotherJobId);
            request.setBucketSpan(TimeValue.timeValueHours(2));

            GetOverallBucketsResponse response = execute(request, machineLearningClient::getOverallBuckets,
                    machineLearningClient::getOverallBucketsAsync);

            assertThat(response.count(), equalTo(121L));
        }
        {
            long end = START_TIME_EPOCH_MS + 10 * 3600000L;
            GetOverallBucketsRequest request = new GetOverallBucketsRequest(JOB_ID, anotherJobId);
            request.setEnd(String.valueOf(end));

            GetOverallBucketsResponse response = execute(request, machineLearningClient::getOverallBuckets,
                    machineLearningClient::getOverallBucketsAsync);

            assertThat(response.count(), equalTo(10L));
            assertThat(response.overallBuckets().get(0).getTimestamp().getTime(), equalTo(START_TIME_EPOCH_MS));
            assertThat(response.overallBuckets().get(9).getTimestamp().getTime(), equalTo(end - 3600000L));
        }
        {
            GetOverallBucketsRequest request = new GetOverallBucketsRequest(JOB_ID, anotherJobId);
            request.setExcludeInterim(true);

            GetOverallBucketsResponse response = execute(request, machineLearningClient::getOverallBuckets,
                    machineLearningClient::getOverallBucketsAsync);

            assertThat(response.count(), equalTo(240L));
            assertThat(response.overallBuckets().stream().allMatch(b -> b.isInterim() == false), is(true));
        }
        {
            GetOverallBucketsRequest request = new GetOverallBucketsRequest(JOB_ID);
            request.setOverallScore(75.0);

            GetOverallBucketsResponse response = execute(request, machineLearningClient::getOverallBuckets,
                    machineLearningClient::getOverallBucketsAsync);

            assertThat(response.count(), equalTo(bucketStats.criticalCount));
            assertThat(response.overallBuckets().stream().allMatch(b -> b.getOverallScore() >= 75.0), is(true));
        }
        {
            long start = START_TIME_EPOCH_MS + 10 * 3600000L;
            GetOverallBucketsRequest request = new GetOverallBucketsRequest(JOB_ID, anotherJobId);
            request.setStart(String.valueOf(start));

            GetOverallBucketsResponse response = execute(request, machineLearningClient::getOverallBuckets,
                    machineLearningClient::getOverallBucketsAsync);

            assertThat(response.count(), equalTo(231L));
            assertThat(response.overallBuckets().get(0).getTimestamp().getTime(), equalTo(start));
        }
        {
            GetOverallBucketsRequest request = new GetOverallBucketsRequest(JOB_ID, anotherJobId);
            request.setEnd(String.valueOf(START_TIME_EPOCH_MS + 3 * 3600000L));
            request.setTopN(2);

            GetOverallBucketsResponse response = execute(request, machineLearningClient::getOverallBuckets,
                    machineLearningClient::getOverallBucketsAsync);

            assertThat(response.count(), equalTo(3L));
            List<OverallBucket> overallBuckets = response.overallBuckets();
            for (int i = 0; i < overallBuckets.size(); ++i) {
                // As the second job has scores that are -10 from the first, the overall buckets should be +5 from the initial job
                assertThat(overallBuckets.get(i).getOverallScore(), is(closeTo(firstBuckets.get(i).getAnomalyScore() + 5.0, 0.0001)));
            }
        }
    }

    public void testGetRecords() throws IOException {
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        {
            GetRecordsRequest request = new GetRecordsRequest(JOB_ID);

            GetRecordsResponse response = execute(request, machineLearningClient::getRecords, machineLearningClient::getRecordsAsync);

            assertThat(response.count(), greaterThan(0L));
            assertThat(response.count(), equalTo(recordStats.totalCount()));
        }
        {
            GetRecordsRequest request = new GetRecordsRequest(JOB_ID);
            request.setRecordScore(50.0);

            GetRecordsResponse response = execute(request, machineLearningClient::getRecords, machineLearningClient::getRecordsAsync);

            long majorAndCriticalCount = recordStats.majorCount + recordStats.criticalCount;
            assertThat(response.count(), equalTo(majorAndCriticalCount));
            assertThat(response.records().size(), equalTo((int) Math.min(100, majorAndCriticalCount)));
            assertThat(response.records().stream().anyMatch(r -> r.getRecordScore() < 50.0), is(false));
        }
        {
            GetRecordsRequest request = new GetRecordsRequest(JOB_ID);
            request.setExcludeInterim(true);

            GetRecordsResponse response = execute(request, machineLearningClient::getRecords, machineLearningClient::getRecordsAsync);

            assertThat(response.count(), equalTo(recordStats.totalCount() - 1));
        }
        {
            long end = START_TIME_EPOCH_MS + 10 * 3600000;
            GetRecordsRequest request = new GetRecordsRequest(JOB_ID);
            request.setStart(String.valueOf(START_TIME_EPOCH_MS));
            request.setEnd(String.valueOf(end));

            GetRecordsResponse response = execute(request, machineLearningClient::getRecords, machineLearningClient::getRecordsAsync);

            for (AnomalyRecord record : response.records()) {
                assertThat(record.getTimestamp().getTime(), greaterThanOrEqualTo(START_TIME_EPOCH_MS));
                assertThat(record.getTimestamp().getTime(), lessThan(end));
            }
        }
        {
            GetRecordsRequest request = new GetRecordsRequest(JOB_ID);
            request.setPageParams(new PageParams(3, 3));

            GetRecordsResponse response = execute(request, machineLearningClient::getRecords, machineLearningClient::getRecordsAsync);

            assertThat(response.records().size(), equalTo(3));
        }
        {
            GetRecordsRequest request = new GetRecordsRequest(JOB_ID);
            request.setSort("probability");
            request.setDescending(true);

            GetRecordsResponse response = execute(request, machineLearningClient::getRecords, machineLearningClient::getRecordsAsync);

            double previousProb = 1.0;
            for (AnomalyRecord record : response.records()) {
                assertThat(record.getProbability(), lessThanOrEqualTo(previousProb));
                previousProb = record.getProbability();
            }
        }
    }

    public void testGetInfluencers() throws IOException {
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        // Let us index a few influencer docs
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        long timestamp = START_TIME_EPOCH_MS;
        long end = START_TIME_EPOCH_MS + 5 * 3600000L;
        while (timestamp < end) {
            boolean isLast = timestamp == end - 3600000L;
            // Last one is interim
            boolean isInterim = isLast;
            // Last one score is higher
            double score = isLast ? 90.0 : 42.0;

            IndexRequest indexRequest = new IndexRequest(RESULTS_INDEX);
            indexRequest.source("{\"job_id\":\"" + JOB_ID + "\", \"result_type\":\"influencer\", \"timestamp\": " +
                    timestamp + "," + "\"bucket_span\": 3600,\"is_interim\": " + isInterim + ", \"influencer_score\": " + score + ", " +
                    "\"influencer_field_name\":\"my_influencer\", \"influencer_field_value\": \"inf_1\", \"probability\":"
                    + randomDouble() + "}", XContentType.JSON);
            bulkRequest.add(indexRequest);
            timestamp += 3600000L;
        }
        highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);

        {
            GetInfluencersRequest request = new GetInfluencersRequest(JOB_ID);
            request.setDescending(false);

            GetInfluencersResponse response = execute(request, machineLearningClient::getInfluencers,
                    machineLearningClient::getInfluencersAsync);

            assertThat(response.count(), equalTo(5L));
        }
        {
            long requestStart = START_TIME_EPOCH_MS + 3600000L;
            long requestEnd = end - 3600000L;
            GetInfluencersRequest request = new GetInfluencersRequest(JOB_ID);
            request.setStart(String.valueOf(requestStart));
            request.setEnd(String.valueOf(requestEnd));

            GetInfluencersResponse response = execute(request, machineLearningClient::getInfluencers,
                    machineLearningClient::getInfluencersAsync);

            assertThat(response.count(), equalTo(3L));
            for (Influencer influencer : response.influencers()) {
                assertThat(influencer.getTimestamp().getTime(), greaterThanOrEqualTo(START_TIME_EPOCH_MS));
                assertThat(influencer.getTimestamp().getTime(), lessThan(end));
            }
        }
        {
            GetInfluencersRequest request = new GetInfluencersRequest(JOB_ID);
            request.setSort("timestamp");
            request.setDescending(false);
            request.setPageParams(new PageParams(1, 2));

            GetInfluencersResponse response = execute(request, machineLearningClient::getInfluencers,
                    machineLearningClient::getInfluencersAsync);

            assertThat(response.influencers().size(), equalTo(2));
            assertThat(response.influencers().get(0).getTimestamp().getTime(), equalTo(START_TIME_EPOCH_MS + 3600000L));
            assertThat(response.influencers().get(1).getTimestamp().getTime(), equalTo(START_TIME_EPOCH_MS + 2 * 3600000L));
        }
        {
            GetInfluencersRequest request = new GetInfluencersRequest(JOB_ID);
            request.setExcludeInterim(true);

            GetInfluencersResponse response = execute(request, machineLearningClient::getInfluencers,
                    machineLearningClient::getInfluencersAsync);

            assertThat(response.count(), equalTo(4L));
            assertThat(response.influencers().stream().anyMatch(Influencer::isInterim), is(false));
        }
        {
            GetInfluencersRequest request = new GetInfluencersRequest(JOB_ID);
            request.setInfluencerScore(75.0);

            GetInfluencersResponse response = execute(request, machineLearningClient::getInfluencers,
                    machineLearningClient::getInfluencersAsync);

            assertThat(response.count(), equalTo(1L));
            assertThat(response.influencers().get(0).getInfluencerScore(), greaterThanOrEqualTo(75.0));
        }
        {
            GetInfluencersRequest request = new GetInfluencersRequest(JOB_ID);
            request.setSort("probability");
            request.setDescending(true);

            GetInfluencersResponse response = execute(request, machineLearningClient::getInfluencers,
                    machineLearningClient::getInfluencersAsync);

            assertThat(response.influencers().size(), equalTo(5));
            double previousProb = 1.0;
            for (Influencer influencer : response.influencers()) {
                assertThat(influencer.getProbability(), lessThanOrEqualTo(previousProb));
                previousProb = influencer.getProbability();
            }
        }
    }

    public static Job buildJob(String jobId) {
        Job.Builder builder = new Job.Builder(jobId);

        Detector detector = new Detector.Builder("count", null).build();
        AnalysisConfig.Builder configBuilder = new AnalysisConfig.Builder(Arrays.asList(detector));
        configBuilder.setBucketSpan(TimeValue.timeValueHours(1));
        builder.setAnalysisConfig(configBuilder);

        DataDescription.Builder dataDescription = new DataDescription.Builder();
        builder.setDataDescription(dataDescription);
        return builder.build();
    }

    private static class Stats {
        // score < 50.0
        private long minorCount;

        // score < 75.0
        private long majorCount;

        // score > 75.0
        private long criticalCount;

        private void report(double score) {
            if (score < 50.0) {
                minorCount++;
            } else if (score < 75.0) {
                majorCount++;
            } else {
                criticalCount++;
            }
        }

        private long totalCount() {
            return minorCount + majorCount + criticalCount;
        }
    }
}
