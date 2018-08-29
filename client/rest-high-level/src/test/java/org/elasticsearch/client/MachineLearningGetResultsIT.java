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
import org.elasticsearch.client.ml.GetBucketsRequest;
import org.elasticsearch.client.ml.GetBucketsResponse;
import org.elasticsearch.client.ml.PutJobRequest;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.client.ml.job.results.Bucket;
import org.elasticsearch.client.ml.job.util.PageParams;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class MachineLearningGetResultsIT extends ESRestHighLevelClientTestCase {

    private static final String RESULTS_INDEX = ".ml-anomalies-shared";
    private static final String DOC = "doc";

    private static final String JOB_ID = "get-results-it-job";

    // 2018-08-01T00:00:00Z
    private static final long START_TIME_EPOCH_MS = 1533081600000L;

    private BucketStats bucketStats = new BucketStats();

    @Before
    public void createJobAndIndexResults() throws IOException {
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        Job job = MachineLearningIT.buildJob(JOB_ID);
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
        addRecordIndexRequests(time, true, bulkRequest);

        highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);
    }

    private void addBucketIndexRequest(long timestamp, boolean isInterim, BulkRequest bulkRequest) {
        IndexRequest indexRequest = new IndexRequest(RESULTS_INDEX, DOC);
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
            IndexRequest indexRequest = new IndexRequest(RESULTS_INDEX, DOC);
            double recordScore = randomDoubleBetween(0.0, 100.0, true);
            double p = randomDoubleBetween(0.0, 0.05, false);
            indexRequest.source("{\"job_id\":\"" + JOB_ID + "\", \"result_type\":\"record\", \"timestamp\": " + timestamp + "," +
                    "\"bucket_span\": 3600,\"is_interim\": " + isInterim + ", \"record_score\": " + recordScore + ", \"probability\": "
                    + p + "}", XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
    }

    @After
    public void deleteJob() throws IOException {
        new MlRestTestStateCleaner(logger, client()).clearMlMetadata();
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

    private static class BucketStats {
        // score < 50.0
        private long minorCount;

        // score < 75.0
        private long majorCount;

        // score > 75.0
        private long criticalCount;

        private void report(double anomalyScore) {
            if (anomalyScore < 50.0) {
                minorCount++;
            } else if (anomalyScore < 75.0) {
                majorCount++;
            } else {
                criticalCount++;
            }
        }
    }
}
