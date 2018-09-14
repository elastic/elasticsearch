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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ml.CloseJobRequest;
import org.elasticsearch.client.ml.CloseJobResponse;
import org.elasticsearch.client.ml.DeleteDatafeedRequest;
import org.elasticsearch.client.ml.DeleteForecastRequest;
import org.elasticsearch.client.ml.DeleteJobRequest;
import org.elasticsearch.client.ml.FlushJobRequest;
import org.elasticsearch.client.ml.FlushJobResponse;
import org.elasticsearch.client.ml.ForecastJobRequest;
import org.elasticsearch.client.ml.ForecastJobResponse;
import org.elasticsearch.client.ml.GetBucketsRequest;
import org.elasticsearch.client.ml.GetBucketsResponse;
import org.elasticsearch.client.ml.GetCategoriesRequest;
import org.elasticsearch.client.ml.GetCategoriesResponse;
import org.elasticsearch.client.ml.GetInfluencersRequest;
import org.elasticsearch.client.ml.GetInfluencersResponse;
import org.elasticsearch.client.ml.GetJobRequest;
import org.elasticsearch.client.ml.GetJobResponse;
import org.elasticsearch.client.ml.GetJobStatsRequest;
import org.elasticsearch.client.ml.GetJobStatsResponse;
import org.elasticsearch.client.ml.GetOverallBucketsRequest;
import org.elasticsearch.client.ml.GetOverallBucketsResponse;
import org.elasticsearch.client.ml.GetRecordsRequest;
import org.elasticsearch.client.ml.GetRecordsResponse;
import org.elasticsearch.client.ml.OpenJobRequest;
import org.elasticsearch.client.ml.OpenJobResponse;
import org.elasticsearch.client.ml.PostDataRequest;
import org.elasticsearch.client.ml.PostDataResponse;
import org.elasticsearch.client.ml.PutDatafeedRequest;
import org.elasticsearch.client.ml.PutDatafeedResponse;
import org.elasticsearch.client.ml.PutJobRequest;
import org.elasticsearch.client.ml.PutJobResponse;
import org.elasticsearch.client.ml.UpdateJobRequest;
import org.elasticsearch.client.ml.job.stats.JobStats;

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
    public AcknowledgedResponse deleteJob(DeleteJobRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::deleteJob,
            options,
            AcknowledgedResponse::fromXContent,
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
    public void deleteJobAsync(DeleteJobRequest request, RequestOptions options, ActionListener<AcknowledgedResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::deleteJob,
            options,
            AcknowledgedResponse::fromXContent,
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
     * @throws IOException when there is a serialization issue sending the request or receiving the response
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
     * Creates a forecast of an existing, opened Machine Learning Job
     *
     * This predicts the future behavior of a time series by using its historical behavior.
     *
     * <p>
     *     For additional info
     *     see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/master/ml-forecast.html">Forecast ML Job Documentation</a>
     * </p>
     * @param request ForecastJobRequest with forecasting options
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return response containing forecast acknowledgement and new forecast's ID
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public ForecastJobResponse forecastJob(ForecastJobRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::forecastJob,
            options,
            ForecastJobResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Updates a Machine Learning {@link org.elasticsearch.client.ml.job.config.Job}
     *
     * <p>
     *     For additional info
     *     see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-update-job.html"></a>
     * </p>
     *
     * @param request the {@link UpdateJobRequest} object enclosing the desired updates
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return a PutJobResponse object containing the updated job object
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public PutJobResponse updateJob(UpdateJobRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::updateJob,
            options,
            PutJobResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Creates a forecast of an existing, opened Machine Learning Job asynchronously
     *
     * This predicts the future behavior of a time series by using its historical behavior.
     *
     * <p>
     *     For additional info
     *     see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/master/ml-forecast.html">Forecast ML Job Documentation</a>
     * </p>
     * @param request ForecastJobRequest with forecasting options
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     */
    public void forecastJobAsync(ForecastJobRequest request, RequestOptions options, ActionListener<ForecastJobResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::forecastJob,
            options,
            ForecastJobResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Updates a Machine Learning {@link org.elasticsearch.client.ml.job.config.Job} asynchronously
     *
     * <p>
     *     For additional info
     *     see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-update-job.html"></a>
     * </p>
     * @param request the {@link UpdateJobRequest} object enclosing the desired updates
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     */
    public void updateJobAsync(UpdateJobRequest request, RequestOptions options, ActionListener<PutJobResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::updateJob,
            options,
            PutJobResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Creates a new Machine Learning Datafeed
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-put-datafeed.html">ML PUT datafeed documentation</a>
     *
     * @param request The PutDatafeedRequest containing the {@link org.elasticsearch.client.ml.datafeed.DatafeedConfig} settings
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return PutDatafeedResponse with enclosed {@link org.elasticsearch.client.ml.datafeed.DatafeedConfig} object
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public PutDatafeedResponse putDatafeed(PutDatafeedRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
                MLRequestConverters::putDatafeed,
                options,
                PutDatafeedResponse::fromXContent,
                Collections.emptySet());
    }

    /**
     * Creates a new Machine Learning Datafeed asynchronously and notifies listener on completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-put-datafeed.html">ML PUT datafeed documentation</a>
     *
     * @param request  The request containing the {@link org.elasticsearch.client.ml.datafeed.DatafeedConfig} settings
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     */
    public void putDatafeedAsync(PutDatafeedRequest request, RequestOptions options, ActionListener<PutDatafeedResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request,
                MLRequestConverters::putDatafeed,
                options,
                PutDatafeedResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Deletes the given Machine Learning Datafeed
     * <p>
     *     For additional info
     *     see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/ml-delete-datafeed.html">
     *         ML Delete Datafeed documentation</a>
     * </p>
     * @param request The request to delete the datafeed
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return action acknowledgement
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public AcknowledgedResponse deleteDatafeed(DeleteDatafeedRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
                MLRequestConverters::deleteDatafeed,
                options,
                AcknowledgedResponse::fromXContent,
                Collections.emptySet());
    }

    /**
     * Deletes the given Machine Learning Datafeed asynchronously and notifies the listener on completion
     * <p>
     *     For additional info
     *     see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/ml-delete-datafeed.html">
     *         ML Delete Datafeed documentation</a>
     * </p>
     * @param request The request to delete the datafeed
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     */
    public void deleteDatafeedAsync(DeleteDatafeedRequest request, RequestOptions options, ActionListener<AcknowledgedResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request,
                MLRequestConverters::deleteDatafeed,
                options,
                AcknowledgedResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Deletes Machine Learning Job Forecasts
     *
     * <p>
     *     For additional info
     *     see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-delete-forecast.html"></a>
     * </p>
     *
     * @param request the {@link DeleteForecastRequest} object enclosing the desired jobId, forecastIDs, and other options
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return a AcknowledgedResponse object indicating request success
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public AcknowledgedResponse deleteForecast(DeleteForecastRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::deleteForecast,
            options,
            AcknowledgedResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Deletes Machine Learning Job Forecasts asynchronously
     *
     * <p>
     *     For additional info
     *     see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-delete-forecast.html"></a>
     * </p>
     *
     * @param request the {@link DeleteForecastRequest} object enclosing the desired jobId, forecastIDs, and other options
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     */
    public void deleteForecastAsync(DeleteForecastRequest request, RequestOptions options, ActionListener<AcknowledgedResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::deleteForecast,
            options,
            AcknowledgedResponse::fromXContent,
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
     * Gets the categories for a Machine Learning Job.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-category.html">
     *     ML GET categories documentation</a>
     *
     * @param request  The request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public GetCategoriesResponse getCategories(GetCategoriesRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::getCategories,
            options,
            GetCategoriesResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Gets the categories for a Machine Learning Job, notifies listener once the requested buckets are retrieved.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-category.html">
     *     ML GET categories documentation</a>
     *
     * @param request  The request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     */
    public void getCategoriesAsync(GetCategoriesRequest request, RequestOptions options, ActionListener<GetCategoriesResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::getCategories,
            options,
            GetCategoriesResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Gets overall buckets for a set of Machine Learning Jobs.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-overall-buckets.html">
     *     ML GET overall buckets documentation</a>
     *
     * @param request  The request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     */
    public GetOverallBucketsResponse getOverallBuckets(GetOverallBucketsRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
                MLRequestConverters::getOverallBuckets,
                options,
                GetOverallBucketsResponse::fromXContent,
                Collections.emptySet());
    }

    /**
     * Gets overall buckets for a set of Machine Learning Jobs, notifies listener once the requested buckets are retrieved.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-overall-buckets.html">
     *     ML GET overall buckets documentation</a>
     *
     * @param request  The request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     */
    public void getOverallBucketsAsync(GetOverallBucketsRequest request, RequestOptions options,
                                       ActionListener<GetOverallBucketsResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request,
                MLRequestConverters::getOverallBuckets,
                options,
                GetOverallBucketsResponse::fromXContent,
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

    /**
     * Sends data to an anomaly detection job for analysis.
     *
     * NOTE: The job must have a state of open to receive and process the data.
     *
     * <p>
     *     For additional info
     *     see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-post-data.html">ML POST Data documentation</a>
     * </p>
     *
     * @param request PostDataRequest containing the data to post and some additional options
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return response containing operational progress about the job
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public PostDataResponse postData(PostDataRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::postData,
            options,
            PostDataResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Sends data to an anomaly detection job for analysis, asynchronously
     *
     * NOTE: The job must have a state of open to receive and process the data.
     *
     * <p>
     *     For additional info
     *     see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-post-data.html">ML POST Data documentation</a>
     * </p>
     *
     * @param request PostDataRequest containing the data to post and some additional options
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     */
    public void postDataAsync(PostDataRequest request, RequestOptions options, ActionListener<PostDataResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::postData,
            options,
            PostDataResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Gets the influencers for a Machine Learning Job.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-influencer.html">
     *     ML GET influencers documentation</a>
     *
     * @param request  the request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     */
    public GetInfluencersResponse getInfluencers(GetInfluencersRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
                MLRequestConverters::getInfluencers,
                options,
                GetInfluencersResponse::fromXContent,
                Collections.emptySet());
    }

    /**
     * Gets the influencers for a Machine Learning Job, notifies listener once the requested influencers are retrieved.
     * <p>
     * For additional info
     * * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-influencer.html">
     *     ML GET influencers documentation</a>
     *
     * @param request  the request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     */
    public void getInfluencersAsync(GetInfluencersRequest request, RequestOptions options,
                                    ActionListener<GetInfluencersResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request,
                MLRequestConverters::getInfluencers,
                options,
                GetInfluencersResponse::fromXContent,
                listener,
                Collections.emptySet());
    }
}
