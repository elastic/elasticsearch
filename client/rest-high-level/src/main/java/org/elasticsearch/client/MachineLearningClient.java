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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.ml.FlushJobRequest;
import org.elasticsearch.client.ml.FlushJobResponse;
import org.elasticsearch.client.ml.GetJobStatsRequest;
import org.elasticsearch.client.ml.GetJobStatsResponse;
import org.elasticsearch.client.ml.job.stats.JobStats;
import org.elasticsearch.client.ml.CloseJobRequest;
import org.elasticsearch.client.ml.CloseJobResponse;
import org.elasticsearch.client.ml.DeleteJobRequest;
import org.elasticsearch.client.ml.DeleteJobResponse;
import org.elasticsearch.client.ml.GetBucketsRequest;
import org.elasticsearch.client.ml.GetBucketsResponse;
import org.elasticsearch.client.ml.GetJobRequest;
import org.elasticsearch.client.ml.GetJobResponse;
import org.elasticsearch.client.ml.GetRecordsRequest;
import org.elasticsearch.client.ml.GetRecordsResponse;
import org.elasticsearch.client.ml.OpenJobRequest;
import org.elasticsearch.client.ml.OpenJobResponse;
import org.elasticsearch.client.ml.PutJobRequest;
import org.elasticsearch.client.ml.PutJobResponse;

import java.io.IOException;
import java.util.Collections;

/**
 * Machine Learning API client wrapper for the {@link RestHighLevelClient}
 *
 * <p>
 * See the <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-apis.html">
 * X-Pack Machine Learning APIs </a> for additional information.
 */
public final class MachineLearningClient {

    private final RestHighLevelClient restHighLevelClient;

    MachineLearningClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Creates a new Machine Learning Job
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-put-job.html">ML PUT job documentation</a>
     *
     * @param request The PutJobRequest containing the {@link org.elasticsearch.client.ml.job.config.Job} settings
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return PutJobResponse with enclosed {@link org.elasticsearch.client.ml.job.config.Job} object
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public PutJobResponse putJob(PutJobRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::putJob,
            options,
            PutJobResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Creates a new Machine Learning Job asynchronously and notifies listener on completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-put-job.html">ML PUT job documentation</a>
     *
     * @param request  The request containing the {@link org.elasticsearch.client.ml.job.config.Job} settings
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     */
    public void putJobAsync(PutJobRequest request, RequestOptions options, ActionListener<PutJobResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::putJob,
            options,
            PutJobResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Gets one or more Machine Learning job configuration info.
     *
     * <p>
     *     For additional info
     *     see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-job.html"></a>
     * </p>
     * @param request {@link GetJobRequest} Request containing a list of jobId(s) and additional options
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return {@link GetJobResponse} response object containing
     * the {@link org.elasticsearch.client.ml.job.config.Job} objects and the number of jobs found
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public GetJobResponse getJob(GetJobRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::getJob,
            options,
            GetJobResponse::fromXContent,
            Collections.emptySet());
    }

     /**
     * Gets one or more Machine Learning job configuration info, asynchronously.
     *
     * <p>
     *     For additional info
     *     see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-job.html"></a>
     * </p>
     * @param request {@link GetJobRequest} Request containing a list of jobId(s) and additional options
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified with {@link GetJobResponse} upon request completion
     */
    public void getJobAsync(GetJobRequest request, RequestOptions options, ActionListener<GetJobResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::getJob,
            options,
            GetJobResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Deletes the given Machine Learning Job
     * <p>
     *     For additional info
     *     see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/ml-delete-job.html">ML Delete Job documentation</a>
     * </p>
     * @param request The request to delete the job
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return action acknowledgement
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public DeleteJobResponse deleteJob(DeleteJobRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::deleteJob,
            options,
            DeleteJobResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Deletes the given Machine Learning Job asynchronously and notifies the listener on completion
     * <p>
     *     For additional info
     *     see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/ml-delete-job.html">ML Delete Job documentation</a>
     * </p>
     * @param request The request to delete the job
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     */
    public void deleteJobAsync(DeleteJobRequest request, RequestOptions options, ActionListener<DeleteJobResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::deleteJob,
            options,
            DeleteJobResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Opens a Machine Learning Job.
     * When you open a new job, it starts with an empty model.
     *
     * When you open an existing job, the most recent model state is automatically loaded.
     * The job is ready to resume its analysis from where it left off, once new data is received.
     *
     * <p>
     *     For additional info
     *     see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-open-job.html"></a>
     * </p>
     * @param request Request containing job_id and additional optional options
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return response containing if the job was successfully opened or not.
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public OpenJobResponse openJob(OpenJobRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::openJob,
            options,
            OpenJobResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Opens a Machine Learning Job asynchronously, notifies listener on completion.
     * When you open a new job, it starts with an empty model.
     *
     * When you open an existing job, the most recent model state is automatically loaded.
     * The job is ready to resume its analysis from where it left off, once new data is received.
     * <p>
     *     For additional info
     *     see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-open-job.html"></a>
     * </p>
     * @param request Request containing job_id and additional optional options
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     */
    public void openJobAsync(OpenJobRequest request, RequestOptions options, ActionListener<OpenJobResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::openJob,
            options,
            OpenJobResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Closes one or more Machine Learning Jobs. A job can be opened and closed multiple times throughout its lifecycle.
     *
     * A closed job cannot receive data or perform analysis operations, but you can still explore and navigate results.
     *
     * @param request Request containing job_ids and additional options. See {@link CloseJobRequest}
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return response containing if the job was successfully closed or not.
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public CloseJobResponse closeJob(CloseJobRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::closeJob,
            options,
            CloseJobResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Closes one or more Machine Learning Jobs asynchronously, notifies listener on completion
     *
     * A closed job cannot receive data or perform analysis operations, but you can still explore and navigate results.
     *
     * @param request Request containing job_ids and additional options. See {@link CloseJobRequest}
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     */
    public void closeJobAsync(CloseJobRequest request, RequestOptions options, ActionListener<CloseJobResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::closeJob,
            options,
            CloseJobResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Gets the buckets for a Machine Learning Job.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-bucket.html">ML GET buckets documentation</a>
     *
     * @param request  The request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     */
    public GetBucketsResponse getBuckets(GetBucketsRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
                MLRequestConverters::getBuckets,
                options,
                GetBucketsResponse::fromXContent,
                Collections.emptySet());
    }

    /**
     * Gets the buckets for a Machine Learning Job, notifies listener once the requested buckets are retrieved.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-bucket.html">ML GET buckets documentation</a>
     *
     * @param request  The request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     */
    public void getBucketsAsync(GetBucketsRequest request, RequestOptions options, ActionListener<GetBucketsResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request,
                MLRequestConverters::getBuckets,
                options,
                GetBucketsResponse::fromXContent,
                listener,
                Collections.emptySet());
     }

    /**
     * Flushes internally buffered data for the given Machine Learning Job ensuring all data sent to the has been processed.
     * This may cause new results to be calculated depending on the contents of the buffer
     *
     * Both flush and close operations are similar,
     * however the flush is more efficient if you are expecting to send more data for analysis.
     *
     * When flushing, the job remains open and is available to continue analyzing data.
     * A close operation additionally prunes and persists the model state to disk and the
     * job must be opened again before analyzing further data.
     *
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-flush-job.html">Flush ML job documentation</a>
     *
     * @param request  The {@link FlushJobRequest} object enclosing the `jobId` and additional request options
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     */
     public FlushJobResponse flushJob(FlushJobRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::flushJob,
            options,
            FlushJobResponse::fromXContent,
            Collections.emptySet());
     }

    /**
     * Flushes internally buffered data for the given Machine Learning Job asynchronously ensuring all data sent to the has been processed.
     * This may cause new results to be calculated depending on the contents of the buffer
     *
     * Both flush and close operations are similar,
     * however the flush is more efficient if you are expecting to send more data for analysis.
     *
     * When flushing, the job remains open and is available to continue analyzing data.
     * A close operation additionally prunes and persists the model state to disk and the
     * job must be opened again before analyzing further data.
     *
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-flush-job.html">Flush ML job documentation</a>
     *
     * @param request  The {@link FlushJobRequest} object enclosing the `jobId` and additional request options
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     */
     public void flushJobAsync(FlushJobRequest request, RequestOptions options, ActionListener<FlushJobResponse> listener) {
         restHighLevelClient.performRequestAsyncAndParseEntity(request,
             MLRequestConverters::flushJob,
             options,
             FlushJobResponse::fromXContent,
             listener,
             Collections.emptySet());
     }

     /**
     * Gets usage statistics for one or more Machine Learning jobs
     *
     * <p>
     *     For additional info
     *     see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-job-stats.html">Get Job stats docs</a>
     * </p>
     * @param request {@link GetJobStatsRequest} Request containing a list of jobId(s) and additional options
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return {@link GetJobStatsResponse} response object containing
     * the {@link JobStats} objects and the number of jobs found
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public GetJobStatsResponse getJobStats(GetJobStatsRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::getJobStats,
            options,
            GetJobStatsResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Gets one or more Machine Learning job configuration info, asynchronously.
     *
     * <p>
     *     For additional info
     *     see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-job-stats.html">Get Job stats docs</a>
     * </p>
     * @param request {@link GetJobStatsRequest} Request containing a list of jobId(s) and additional options
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified with {@link GetJobStatsResponse} upon request completion
     */
    public void getJobStatsAsync(GetJobStatsRequest request, RequestOptions options, ActionListener<GetJobStatsResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::getJobStats,
            options,
            GetJobStatsResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Gets the records for a Machine Learning Job.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-record.html">ML GET records documentation</a>
     *
     * @param request  the request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     */
    public GetRecordsResponse getRecords(GetRecordsRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
                MLRequestConverters::getRecords,
                options,
                GetRecordsResponse::fromXContent,
                Collections.emptySet());
    }

    /**
     * Gets the records for a Machine Learning Job, notifies listener once the requested records are retrieved.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-record.html">ML GET records documentation</a>
     *
     * @param request  the request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     */
    public void getRecordsAsync(GetRecordsRequest request, RequestOptions options, ActionListener<GetRecordsResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request,
                MLRequestConverters::getRecords,
                options,
                GetRecordsResponse::fromXContent,
                listener,
                Collections.emptySet());
    }
}
