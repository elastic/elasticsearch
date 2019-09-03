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
import org.elasticsearch.client.ml.DeleteCalendarEventRequest;
import org.elasticsearch.client.ml.DeleteCalendarJobRequest;
import org.elasticsearch.client.ml.DeleteCalendarRequest;
import org.elasticsearch.client.ml.DeleteDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.DeleteDatafeedRequest;
import org.elasticsearch.client.ml.DeleteExpiredDataRequest;
import org.elasticsearch.client.ml.DeleteExpiredDataResponse;
import org.elasticsearch.client.ml.DeleteFilterRequest;
import org.elasticsearch.client.ml.DeleteForecastRequest;
import org.elasticsearch.client.ml.DeleteJobRequest;
import org.elasticsearch.client.ml.DeleteJobResponse;
import org.elasticsearch.client.ml.DeleteModelSnapshotRequest;
import org.elasticsearch.client.ml.EstimateMemoryUsageResponse;
import org.elasticsearch.client.ml.EvaluateDataFrameRequest;
import org.elasticsearch.client.ml.EvaluateDataFrameResponse;
import org.elasticsearch.client.ml.FindFileStructureRequest;
import org.elasticsearch.client.ml.FindFileStructureResponse;
import org.elasticsearch.client.ml.FlushJobRequest;
import org.elasticsearch.client.ml.FlushJobResponse;
import org.elasticsearch.client.ml.ForecastJobRequest;
import org.elasticsearch.client.ml.ForecastJobResponse;
import org.elasticsearch.client.ml.GetBucketsRequest;
import org.elasticsearch.client.ml.GetBucketsResponse;
import org.elasticsearch.client.ml.GetCalendarEventsRequest;
import org.elasticsearch.client.ml.GetCalendarEventsResponse;
import org.elasticsearch.client.ml.GetCalendarsRequest;
import org.elasticsearch.client.ml.GetCalendarsResponse;
import org.elasticsearch.client.ml.GetCategoriesRequest;
import org.elasticsearch.client.ml.GetCategoriesResponse;
import org.elasticsearch.client.ml.GetDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.GetDataFrameAnalyticsResponse;
import org.elasticsearch.client.ml.GetDataFrameAnalyticsStatsRequest;
import org.elasticsearch.client.ml.GetDataFrameAnalyticsStatsResponse;
import org.elasticsearch.client.ml.GetDatafeedRequest;
import org.elasticsearch.client.ml.GetDatafeedResponse;
import org.elasticsearch.client.ml.GetDatafeedStatsRequest;
import org.elasticsearch.client.ml.GetDatafeedStatsResponse;
import org.elasticsearch.client.ml.GetFiltersRequest;
import org.elasticsearch.client.ml.GetFiltersResponse;
import org.elasticsearch.client.ml.GetInfluencersRequest;
import org.elasticsearch.client.ml.GetInfluencersResponse;
import org.elasticsearch.client.ml.GetJobRequest;
import org.elasticsearch.client.ml.GetJobResponse;
import org.elasticsearch.client.ml.GetJobStatsRequest;
import org.elasticsearch.client.ml.GetJobStatsResponse;
import org.elasticsearch.client.ml.GetModelSnapshotsRequest;
import org.elasticsearch.client.ml.GetModelSnapshotsResponse;
import org.elasticsearch.client.ml.GetOverallBucketsRequest;
import org.elasticsearch.client.ml.GetOverallBucketsResponse;
import org.elasticsearch.client.ml.GetRecordsRequest;
import org.elasticsearch.client.ml.GetRecordsResponse;
import org.elasticsearch.client.ml.MlInfoRequest;
import org.elasticsearch.client.ml.MlInfoResponse;
import org.elasticsearch.client.ml.OpenJobRequest;
import org.elasticsearch.client.ml.OpenJobResponse;
import org.elasticsearch.client.ml.PostCalendarEventRequest;
import org.elasticsearch.client.ml.PostCalendarEventResponse;
import org.elasticsearch.client.ml.PostDataRequest;
import org.elasticsearch.client.ml.PostDataResponse;
import org.elasticsearch.client.ml.PreviewDatafeedRequest;
import org.elasticsearch.client.ml.PreviewDatafeedResponse;
import org.elasticsearch.client.ml.PutCalendarJobRequest;
import org.elasticsearch.client.ml.PutCalendarRequest;
import org.elasticsearch.client.ml.PutCalendarResponse;
import org.elasticsearch.client.ml.PutDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.PutDataFrameAnalyticsResponse;
import org.elasticsearch.client.ml.PutDatafeedRequest;
import org.elasticsearch.client.ml.PutDatafeedResponse;
import org.elasticsearch.client.ml.PutFilterRequest;
import org.elasticsearch.client.ml.PutFilterResponse;
import org.elasticsearch.client.ml.PutJobRequest;
import org.elasticsearch.client.ml.PutJobResponse;
import org.elasticsearch.client.ml.RevertModelSnapshotRequest;
import org.elasticsearch.client.ml.RevertModelSnapshotResponse;
import org.elasticsearch.client.ml.SetUpgradeModeRequest;
import org.elasticsearch.client.ml.StartDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.StartDatafeedRequest;
import org.elasticsearch.client.ml.StartDatafeedResponse;
import org.elasticsearch.client.ml.StopDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.StopDataFrameAnalyticsResponse;
import org.elasticsearch.client.ml.StopDatafeedRequest;
import org.elasticsearch.client.ml.StopDatafeedResponse;
import org.elasticsearch.client.ml.UpdateDatafeedRequest;
import org.elasticsearch.client.ml.UpdateFilterRequest;
import org.elasticsearch.client.ml.UpdateJobRequest;
import org.elasticsearch.client.ml.UpdateModelSnapshotRequest;
import org.elasticsearch.client.ml.UpdateModelSnapshotResponse;
import org.elasticsearch.client.ml.job.stats.JobStats;

import java.io.IOException;
import java.util.Collections;


/**
 * Machine Learning API client wrapper for the {@link RestHighLevelClient}
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
     * @param request  The request containing the {@link org.elasticsearch.client.ml.job.config.Job} settings
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putJobAsync(PutJobRequest request, RequestOptions options, ActionListener<PutJobResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                MLRequestConverters::putJob,
                options,
                PutJobResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Gets one or more Machine Learning job configuration info.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-job.html">ML GET job documentation</a>
     *
     * @param request {@link GetJobRequest} Request containing a list of jobId(s) and additional options
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
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
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-job.html">ML GET job documentation</a>
     * @param request  {@link GetJobRequest} Request containing a list of jobId(s) and additional options
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified with {@link GetJobResponse} upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getJobAsync(GetJobRequest request, RequestOptions options, ActionListener<GetJobResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                MLRequestConverters::getJob,
                options,
                GetJobResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Gets usage statistics for one or more Machine Learning jobs
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-job-stats.html">Get job stats docs</a>
     *
     * @param request {@link GetJobStatsRequest} Request containing a list of jobId(s) and additional options
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
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
     * Gets usage statistics for one or more Machine Learning jobs, asynchronously.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-job-stats.html">Get job stats docs</a>
     * @param request  {@link GetJobStatsRequest} Request containing a list of jobId(s) and additional options
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified with {@link GetJobStatsResponse} upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getJobStatsAsync(GetJobStatsRequest request, RequestOptions options, ActionListener<GetJobStatsResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                MLRequestConverters::getJobStats,
                options,
                GetJobStatsResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Deletes expired data from Machine Learning Jobs
     * <p>
     * For additional info
     * see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/ml-delete-expired-data.html">ML Delete Expired Data
     * documentation</a>
     *
     * @param request The request to delete expired ML data
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return The action response which contains the acknowledgement or the task id depending on whether the action was set to wait for
     * completion
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public DeleteExpiredDataResponse deleteExpiredData(DeleteExpiredDataRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::deleteExpiredData,
            options,
            DeleteExpiredDataResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Deletes expired data from Machine Learning Jobs asynchronously and notifies the listener on completion
     * <p>
     * For additional info
     * see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/ml-delete-expired-data.html">ML Delete Expired Data
     * documentation</a>
     * @param request  The request to delete expired ML data
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteExpiredDataAsync(DeleteExpiredDataRequest request, RequestOptions options,
                                              ActionListener<DeleteExpiredDataResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::deleteExpiredData,
            options,
            DeleteExpiredDataResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Deletes the given Machine Learning Job
     * <p>
     * For additional info
     * see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/ml-delete-job.html">ML Delete job documentation</a>
     *
     * @param request The request to delete the job
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return The action response which contains the acknowledgement or the task id depending on whether the action was set to wait for
     * completion
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
     * For additional info
     * see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/ml-delete-job.html">ML Delete Job documentation</a>
     *
     *  @param request  The request to delete the job
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteJobAsync(DeleteJobRequest request, RequestOptions options, ActionListener<DeleteJobResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::deleteJob,
            options,
            DeleteJobResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Opens a Machine Learning Job.
     * When you open a new job, it starts with an empty model.
     * When you open an existing job, the most recent model state is automatically loaded.
     * The job is ready to resume its analysis from where it left off, once new data is received.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-open-job.html">ML Open Job documentation</a>
     *
     * @param request Request containing job_id and additional optional options
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
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
     * When you open an existing job, the most recent model state is automatically loaded.
     * The job is ready to resume its analysis from where it left off, once new data is received.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-open-job.html">ML Open Job documentation</a>
     *
     * @param request  Request containing job_id and additional optional options
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable openJobAsync(OpenJobRequest request, RequestOptions options, ActionListener<OpenJobResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                MLRequestConverters::openJob,
                options,
                OpenJobResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Closes one or more Machine Learning Jobs. A job can be opened and closed multiple times throughout its lifecycle.
     * A closed job cannot receive data or perform analysis operations, but you can still explore and navigate results.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-close-job.html">ML Close Job documentation</a>
     *
     * @param request Request containing job_ids and additional options. See {@link CloseJobRequest}
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
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
     * A closed job cannot receive data or perform analysis operations, but you can still explore and navigate results.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-close-job.html">ML Close Job documentation</a>
     *
     * @param request  Request containing job_ids and additional options. See {@link CloseJobRequest}
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable closeJobAsync(CloseJobRequest request, RequestOptions options, ActionListener<CloseJobResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                MLRequestConverters::closeJob,
                options,
                CloseJobResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Flushes internally buffered data for the given Machine Learning Job ensuring all data sent to the has been processed.
     * This may cause new results to be calculated depending on the contents of the buffer
     * Both flush and close operations are similar,
     * however the flush is more efficient if you are expecting to send more data for analysis.
     * When flushing, the job remains open and is available to continue analyzing data.
     * A close operation additionally prunes and persists the model state to disk and the
     * job must be opened again before analyzing further data.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-flush-job.html">Flush ML job documentation</a>
     *
     * @param request The {@link FlushJobRequest} object enclosing the `jobId` and additional request options
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
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
     * Both flush and close operations are similar,
     * however the flush is more efficient if you are expecting to send more data for analysis.
     * When flushing, the job remains open and is available to continue analyzing data.
     * A close operation additionally prunes and persists the model state to disk and the
     * job must be opened again before analyzing further data.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-flush-job.html">Flush ML job documentation</a>
     *
     * @param request  The {@link FlushJobRequest} object enclosing the `jobId` and additional request options
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable flushJobAsync(FlushJobRequest request, RequestOptions options, ActionListener<FlushJobResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                MLRequestConverters::flushJob,
                options,
                FlushJobResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Creates a forecast of an existing, opened Machine Learning Job
     * This predicts the future behavior of a time series by using its historical behavior.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/master/ml-forecast.html">Forecast ML Job Documentation</a>
     *
     * @param request ForecastJobRequest with forecasting options
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
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
     * Creates a forecast of an existing, opened Machine Learning Job asynchronously
     * This predicts the future behavior of a time series by using its historical behavior.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/master/ml-forecast.html">Forecast ML Job Documentation</a>
     *
     * @param request  ForecastJobRequest with forecasting options
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable forecastJobAsync(ForecastJobRequest request, RequestOptions options, ActionListener<ForecastJobResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                MLRequestConverters::forecastJob,
                options,
                ForecastJobResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Deletes Machine Learning Job Forecasts
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-delete-forecast.html">Delete Job Forecast
     * Documentation</a>
     *
     * @param request the {@link DeleteForecastRequest} object enclosing the desired jobId, forecastIDs, and other options
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
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
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-delete-forecast.html">Delete Job Forecast
     * Documentation</a>
     *
     * @param request  the {@link DeleteForecastRequest} object enclosing the desired jobId, forecastIDs, and other options
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteForecastAsync(DeleteForecastRequest request, RequestOptions options,
                                           ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                MLRequestConverters::deleteForecast,
                options,
                AcknowledgedResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Deletes Machine Learning Model Snapshots
     * <p>
     * For additional info
     * see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/ml-delete-snapshot.html">
     *     ML Delete Model Snapshot documentation</a>
     *
     * @param request The request to delete the model snapshot
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return action acknowledgement
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public AcknowledgedResponse deleteModelSnapshot(DeleteModelSnapshotRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::deleteModelSnapshot,
            options,
            AcknowledgedResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Deletes Machine Learning Model Snapshots asynchronously and notifies the listener on completion
     * <p>
     * For additional info
     * see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/ml-delete-snapshot.html">
     *         ML Delete Model Snapshot documentation</a>
     *
     * @param request The request to delete the model snapshot
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteModelSnapshotAsync(DeleteModelSnapshotRequest request, RequestOptions options,
                                                ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::deleteModelSnapshot,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Reverts to a particular Machine Learning Model Snapshot
     * <p>
     * For additional info
     * see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/ml-revert-snapshot.html">
     *     ML Revert Model Snapshot documentation</a>
     *
     * @param request The request to revert to a previous model snapshot
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return action acknowledgement
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public RevertModelSnapshotResponse revertModelSnapshot(RevertModelSnapshotRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::revertModelSnapshot,
            options,
            RevertModelSnapshotResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Reverts to a particular Machine Learning Model Snapshot asynchronously and notifies the listener on completion
     * <p>
     * For additional info
     * see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/ml-revert-snapshot.html">
     *         ML Revert Model Snapshot documentation</a>
     *
     * @param request The request to revert to a previous model snapshot
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable revertModelSnapshotAsync(RevertModelSnapshotRequest request, RequestOptions options,
                                                ActionListener<RevertModelSnapshotResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::revertModelSnapshot,
            options,
            RevertModelSnapshotResponse::fromXContent,
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
     * @param request The request containing the {@link org.elasticsearch.client.ml.datafeed.DatafeedConfig} settings
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putDatafeedAsync(PutDatafeedRequest request, RequestOptions options, ActionListener<PutDatafeedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                MLRequestConverters::putDatafeed,
                options,
                PutDatafeedResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Updates a Machine Learning Datafeed
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-update-datafeed.html">
     *     ML Update datafeed documentation</a>
     *
     * @param request The UpdateDatafeedRequest containing the {@link org.elasticsearch.client.ml.datafeed.DatafeedUpdate} settings
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return PutDatafeedResponse with enclosed, updated {@link org.elasticsearch.client.ml.datafeed.DatafeedConfig} object
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public PutDatafeedResponse updateDatafeed(UpdateDatafeedRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::updateDatafeed,
            options,
            PutDatafeedResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Updates a Machine Learning Datafeed asynchronously and notifies listener on completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-update-datafeed.html">
     *     ML Update datafeed documentation</a>
     *
     * @param request The request containing the {@link org.elasticsearch.client.ml.datafeed.DatafeedUpdate} settings
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable updateDatafeedAsync(UpdateDatafeedRequest request, RequestOptions options,
                                           ActionListener<PutDatafeedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::updateDatafeed,
            options,
            PutDatafeedResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Gets one or more Machine Learning datafeed configuration info.
     *
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-datafeed.html">ML GET datafeed documentation</a>
     *
     * @param request {@link GetDatafeedRequest} Request containing a list of datafeedId(s) and additional options
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return {@link GetDatafeedResponse} response object containing
     * the {@link org.elasticsearch.client.ml.datafeed.DatafeedConfig} objects and the number of jobs found
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public GetDatafeedResponse getDatafeed(GetDatafeedRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
                MLRequestConverters::getDatafeed,
                options,
                GetDatafeedResponse::fromXContent,
                Collections.emptySet());
    }

    /**
     * Gets one or more Machine Learning datafeed configuration info, asynchronously.
     *
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-datafeed.html">ML GET datafeed documentation</a>
     *
     * @param request {@link GetDatafeedRequest} Request containing a list of datafeedId(s) and additional options
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified with {@link GetDatafeedResponse} upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getDatafeedAsync(GetDatafeedRequest request, RequestOptions options,
                                        ActionListener<GetDatafeedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                MLRequestConverters::getDatafeed,
                options,
                GetDatafeedResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Deletes the given Machine Learning Datafeed
     * <p>
     * For additional info
     * see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/ml-delete-datafeed.html">
     *     ML Delete Datafeed documentation</a>
     *
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
     * For additional info
     * see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/ml-delete-datafeed.html">
     *         ML Delete Datafeed documentation</a>
     *
     * @param request The request to delete the datafeed
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteDatafeedAsync(DeleteDatafeedRequest request, RequestOptions options,
                                           ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                MLRequestConverters::deleteDatafeed,
                options,
                AcknowledgedResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Starts the given Machine Learning Datafeed
     * <p>
     * For additional info
     * see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/ml-start-datafeed.html">
     *     ML Start Datafeed documentation</a>
     *
     * @param request The request to start the datafeed
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return action acknowledgement
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public StartDatafeedResponse startDatafeed(StartDatafeedRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::startDatafeed,
            options,
            StartDatafeedResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Starts the given Machine Learning Datafeed asynchronously and notifies the listener on completion
     * <p>
     * For additional info
     * see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/ml-start-datafeed.html">
     *         ML Start Datafeed documentation</a>
     *
     * @param request The request to start the datafeed
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable startDatafeedAsync(StartDatafeedRequest request, RequestOptions options,
                                          ActionListener<StartDatafeedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::startDatafeed,
            options,
            StartDatafeedResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Stops the given Machine Learning Datafeed
     * <p>
     * For additional info
     * see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/ml-stop-datafeed.html">
     *     ML Stop Datafeed documentation</a>
     *
     * @param request The request to stop the datafeed
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return action acknowledgement
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public StopDatafeedResponse stopDatafeed(StopDatafeedRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::stopDatafeed,
            options,
            StopDatafeedResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Stops the given Machine Learning Datafeed asynchronously and notifies the listener on completion
     * <p>
     * For additional info
     * see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/ml-stop-datafeed.html">
     *         ML Stop Datafeed documentation</a>
     *
     * @param request The request to stop the datafeed
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable stopDatafeedAsync(StopDatafeedRequest request, RequestOptions options,
                                         ActionListener<StopDatafeedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::stopDatafeed,
            options,
            StopDatafeedResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Gets statistics for one or more Machine Learning datafeeds
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-datafeed-stats.html">Get datafeed stats docs</a>
     *
     * @param request {@link GetDatafeedStatsRequest} Request containing a list of datafeedId(s) and additional options
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return {@link GetDatafeedStatsResponse} response object containing
     * the {@link org.elasticsearch.client.ml.datafeed.DatafeedStats} objects and the number of datafeeds found
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public GetDatafeedStatsResponse getDatafeedStats(GetDatafeedStatsRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::getDatafeedStats,
            options,
            GetDatafeedStatsResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Previews the given Machine Learning Datafeed
     * <p>
     * For additional info
     * see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/ml-preview-datafeed.html">
     *     ML Preview Datafeed documentation</a>
     *
     * @param request The request to preview the datafeed
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return {@link PreviewDatafeedResponse} object containing a {@link org.elasticsearch.common.bytes.BytesReference} of the data in
     * JSON format
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public PreviewDatafeedResponse previewDatafeed(PreviewDatafeedRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::previewDatafeed,
            options,
            PreviewDatafeedResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Gets statistics for one or more Machine Learning datafeeds, asynchronously.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-datafeed-stats.html">Get datafeed stats docs</a>
     *
     * @param request  {@link GetDatafeedStatsRequest} Request containing a list of datafeedId(s) and additional options
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified with {@link GetDatafeedStatsResponse} upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getDatafeedStatsAsync(GetDatafeedStatsRequest request,
                                             RequestOptions options,
                                             ActionListener<GetDatafeedStatsResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::getDatafeedStats,
            options,
            GetDatafeedStatsResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Previews the given Machine Learning Datafeed asynchronously and notifies the listener on completion
     * <p>
     * For additional info
     * see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/ml-preview-datafeed.html">
     *         ML Preview Datafeed documentation</a>
     *
     * @param request The request to preview the datafeed
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable previewDatafeedAsync(PreviewDatafeedRequest request,
                                            RequestOptions options,
                                            ActionListener<PreviewDatafeedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::previewDatafeed,
            options,
            PreviewDatafeedResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Updates a Machine Learning {@link org.elasticsearch.client.ml.job.config.Job}
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-update-job.html">ML Update Job Documentation</a>
     *
     * @param request the {@link UpdateJobRequest} object enclosing the desired updates
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
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
     * Updates a Machine Learning {@link org.elasticsearch.client.ml.job.config.Job} asynchronously
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-update-job.html">ML Update Job Documentation</a>
     *
     * @param request  the {@link UpdateJobRequest} object enclosing the desired updates
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable updateJobAsync(UpdateJobRequest request, RequestOptions options, ActionListener<PutJobResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                MLRequestConverters::updateJob,
                options,
                PutJobResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Gets the buckets for a Machine Learning Job.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-bucket.html">ML GET buckets documentation</a>
     *
     * @param request The request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
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
     *  @param request  The request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getBucketsAsync(GetBucketsRequest request, RequestOptions options, ActionListener<GetBucketsResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
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
     * ML GET categories documentation</a>
     *
     * @param request The request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
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
     * ML GET categories documentation</a>
     *
     * @param request  The request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getCategoriesAsync(GetCategoriesRequest request, RequestOptions options,
                                          ActionListener<GetCategoriesResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                MLRequestConverters::getCategories,
                options,
                GetCategoriesResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Gets the snapshots for a Machine Learning Job.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-snapshot.html">
     * ML GET model snapshots documentation</a>
     *
     * @param request The request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public GetModelSnapshotsResponse getModelSnapshots(GetModelSnapshotsRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::getModelSnapshots,
            options,
            GetModelSnapshotsResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Gets the snapshots for a Machine Learning Job, notifies listener once the requested snapshots are retrieved.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-snapshot.html">
     * ML GET model snapshots documentation</a>
     *
     * @param request  The request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getModelSnapshotsAsync(GetModelSnapshotsRequest request, RequestOptions options,
                                              ActionListener<GetModelSnapshotsResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::getModelSnapshots,
            options,
            GetModelSnapshotsResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Updates a snapshot for a Machine Learning Job.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-update-snapshot.html">
     * ML UPDATE model snapshots documentation</a>
     *
     * @param request The request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public UpdateModelSnapshotResponse updateModelSnapshot(UpdateModelSnapshotRequest request,
                                                             RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::updateModelSnapshot,
            options,
            UpdateModelSnapshotResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Updates a snapshot for a Machine Learning Job, notifies listener once the requested snapshots are retrieved.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-update-snapshot.html">
     * ML UPDATE model snapshots documentation</a>
     *
     * @param request  The request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable updateModelSnapshotAsync(UpdateModelSnapshotRequest request, RequestOptions options,
                                                ActionListener<UpdateModelSnapshotResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::updateModelSnapshot,
            options,
            UpdateModelSnapshotResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Gets overall buckets for a set of Machine Learning Jobs.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-overall-buckets.html">
     * ML GET overall buckets documentation</a>
     *
     * @param request The request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
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
     * ML GET overall buckets documentation</a>
     *
     * @param request  The request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getOverallBucketsAsync(GetOverallBucketsRequest request, RequestOptions options,
                                              ActionListener<GetOverallBucketsResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
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
     * @param request the request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
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
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getRecordsAsync(GetRecordsRequest request, RequestOptions options, ActionListener<GetRecordsResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                MLRequestConverters::getRecords,
                options,
                GetRecordsResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Sends data to an anomaly detection job for analysis.
     * <p>
     * NOTE: The job must have a state of open to receive and process the data.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-post-data.html">ML POST Data documentation</a>
     *
     * @param request PostDataRequest containing the data to post and some additional options
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
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
     * <p>
     * NOTE: The job must have a state of open to receive and process the data.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-post-data.html">ML POST Data documentation</a>
     *
     * @param request  PostDataRequest containing the data to post and some additional options
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable postDataAsync(PostDataRequest request, RequestOptions options, ActionListener<PostDataResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                MLRequestConverters::postData,
                options,
                PostDataResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Gets a single or multiple calendars.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-calendar.html">ML GET calendars documentation</a>
     *
     * @param request The calendars request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return {@link GetCalendarsResponse} response object containing the {@link org.elasticsearch.client.ml.calendars.Calendar}
     * objects and the number of calendars found
     */
    public GetCalendarsResponse getCalendars(GetCalendarsRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
                MLRequestConverters::getCalendars,
                options,
                GetCalendarsResponse::fromXContent,
                Collections.emptySet());
    }

    /**
     * Gets a single or multiple calendars, notifies listener once the requested records are retrieved.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-calendar.html">ML GET calendars documentation</a>
     *
     * @param request The calendars request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getCalendarsAsync(GetCalendarsRequest request, RequestOptions options,
                                         ActionListener<GetCalendarsResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                MLRequestConverters::getCalendars,
                options,
                GetCalendarsResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Gets the influencers for a Machine Learning Job.
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-influencer.html">
     * ML GET influencers documentation</a>
     *
     * @param request the request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
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
     * ML GET influencers documentation</a>
     *
     * @param request  the request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getInfluencersAsync(GetInfluencersRequest request, RequestOptions options,
                                           ActionListener<GetInfluencersResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                MLRequestConverters::getInfluencers,
                options,
                GetInfluencersResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Create a new machine learning calendar
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-put-calendar.html">
     * ML create calendar documentation</a>
     *
     * @param request The request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return The {@link PutCalendarResponse} containing the calendar
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public PutCalendarResponse putCalendar(PutCalendarRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
                MLRequestConverters::putCalendar,
                options,
                PutCalendarResponse::fromXContent,
                Collections.emptySet());
    }

    /**
     * Create a new machine learning calendar, notifies listener with the created calendar
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-put-calendar.html">
     * ML create calendar documentation</a>
     *
     * @param request  The request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putCalendarAsync(PutCalendarRequest request, RequestOptions options, ActionListener<PutCalendarResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                MLRequestConverters::putCalendar,
                options,
                PutCalendarResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Adds Machine Learning Job(s) to a calendar
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-put-calendar-job.html">
     * ML Put calendar job documentation</a>
     *
     * @param request The request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return The {@link PutCalendarResponse} containing the updated calendar
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public PutCalendarResponse putCalendarJob(PutCalendarJobRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::putCalendarJob,
            options,
            PutCalendarResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Adds Machine Learning Job(s) to a calendar, notifies listener when completed
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-put-calendar-job.html">
     * ML Put calendar job documentation</a>
     *
     * @param request  The request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putCalendarJobAsync(PutCalendarJobRequest request, RequestOptions options,
                                           ActionListener<PutCalendarResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::putCalendarJob,
            options,
            PutCalendarResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Removes Machine Learning Job(s) from a calendar
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-delete-calendar-job.html">
     * ML Delete calendar job documentation</a>
     *
     * @param request The request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return The {@link PutCalendarResponse} containing the updated calendar
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public PutCalendarResponse deleteCalendarJob(DeleteCalendarJobRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::deleteCalendarJob,
            options,
            PutCalendarResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Removes Machine Learning Job(s) from a calendar, notifies listener when completed
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-delete-calendar-job.html">
     * ML Delete calendar job documentation</a>
     *
     * @param request  The request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteCalendarJobAsync(DeleteCalendarJobRequest request,
                                              RequestOptions options,
                                              ActionListener<PutCalendarResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::deleteCalendarJob,
            options,
            PutCalendarResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Deletes the given Machine Learning Calendar
     * <p>
     * For additional info see
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-delete-calendar.html">
     *     ML Delete calendar documentation</a>
     *
     * @param request The request to delete the calendar
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return action acknowledgement
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public AcknowledgedResponse deleteCalendar(DeleteCalendarRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
                MLRequestConverters::deleteCalendar,
                options,
                AcknowledgedResponse::fromXContent,
                Collections.emptySet());
    }

    /**
     * Deletes the given Machine Learning Job asynchronously and notifies the listener on completion
     * <p>
     * For additional info see
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-delete-calendar.html">
     *     ML Delete calendar documentation</a>
     *
     * @param request  The request to delete the calendar
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteCalendarAsync(DeleteCalendarRequest request, RequestOptions options,
                                           ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                MLRequestConverters::deleteCalendar,
                options,
                AcknowledgedResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Gets the events for a machine learning calendar
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-calendar-event.html">
     *  GET Calendar Events API</a>
     *
     * @param request The request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return The {@link PostCalendarEventRequest} containing the scheduled events
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public GetCalendarEventsResponse getCalendarEvents(GetCalendarEventsRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::getCalendarEvents,
            options,
            GetCalendarEventsResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Gets the events for a a machine learning calendar asynchronously, notifies the listener on completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-calendar-event.html">
     *  GET Calendar Events API</a>
     *
     * @param request  The request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getCalendarEventsAsync(GetCalendarEventsRequest request, RequestOptions options,
                                              ActionListener<GetCalendarEventsResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::getCalendarEvents,
            options,
            GetCalendarEventsResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Creates new events for a a machine learning calendar
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-post-calendar-event.html">
     *  Add Events to Calendar API</a>
     *
     * @param request The request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return The {@link PostCalendarEventRequest} containing the scheduled events
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public PostCalendarEventResponse postCalendarEvent(PostCalendarEventRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::postCalendarEvents,
            options,
            PostCalendarEventResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Creates new events for a a machine learning calendar asynchronously, notifies the listener on completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-post-calendar-event.html">
     *  Add Events to Calendar API</a>
     *
     * @param request  The request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable postCalendarEventAsync(PostCalendarEventRequest request, RequestOptions options,
                                              ActionListener<PostCalendarEventResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::postCalendarEvents,
            options,
            PostCalendarEventResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Removes a Scheduled Event from a calendar
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-delete-calendar-event.html">
     * ML Delete calendar event documentation</a>
     *
     * @param request The request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return The {@link PutCalendarResponse} containing the updated calendar
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public AcknowledgedResponse deleteCalendarEvent(DeleteCalendarEventRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::deleteCalendarEvent,
            options,
            AcknowledgedResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Removes a Scheduled Event from a calendar, notifies listener when completed
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-delete-calendar-event.html">
     * ML Delete calendar event documentation</a>
     *
     * @param request  The request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteCalendarEventAsync(DeleteCalendarEventRequest request,
                                                RequestOptions options,
                                                ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::deleteCalendarEvent,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Creates a new Machine Learning Filter
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-put-filter.html">ML PUT Filter documentation</a>
     *
     * @param request The PutFilterRequest containing the {@link org.elasticsearch.client.ml.job.config.MlFilter} settings
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return PutFilterResponse with enclosed {@link org.elasticsearch.client.ml.job.config.MlFilter} object
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public PutFilterResponse putFilter(PutFilterRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::putFilter,
            options,
            PutFilterResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Creates a new Machine Learning Filter asynchronously and notifies listener on completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-put-filter.html">ML PUT Filter documentation</a>
     *
     * @param request  The request containing the {@link org.elasticsearch.client.ml.job.config.MlFilter} settings
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putFilterAsync(PutFilterRequest request, RequestOptions options, ActionListener<PutFilterResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::putFilter,
            options,
            PutFilterResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Gets Machine Learning Filters
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-filter.html">ML GET Filter documentation</a>
     *
     * @param request The request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return GetFilterResponse with enclosed {@link org.elasticsearch.client.ml.job.config.MlFilter} objects
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public GetFiltersResponse getFilter(GetFiltersRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::getFilter,
            options,
            GetFiltersResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Gets Machine Learning Filters asynchronously and notifies listener on completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-get-filter.html">ML GET Filter documentation</a>
     *
     * @param request  The request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getFilterAsync(GetFiltersRequest request, RequestOptions options, ActionListener<GetFiltersResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::getFilter,
            options,
            GetFiltersResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Updates a Machine Learning Filter
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-update-filter.html">
     *     ML Update Filter documentation</a>
     *
     * @param request The request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return PutFilterResponse with the updated {@link org.elasticsearch.client.ml.job.config.MlFilter} object
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public PutFilterResponse updateFilter(UpdateFilterRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::updateFilter,
            options,
            PutFilterResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Updates a Machine Learning Filter asynchronously and notifies listener on completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-update-filter.html">
     *     ML Update Filter documentation</a>
     *
     * @param request  The request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable updateFilterAsync(UpdateFilterRequest request, RequestOptions options, ActionListener<PutFilterResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::updateFilter,
            options,
            PutFilterResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Deletes the given Machine Learning filter
     * <p>
     * For additional info
     * see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/ml-delete-filter.html">
     *     ML Delete Filter documentation</a>
     *
     * @param request The request to delete the filter
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return action acknowledgement
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public AcknowledgedResponse deleteFilter(DeleteFilterRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::deleteFilter,
            options,
            AcknowledgedResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Deletes the given Machine Learning filter asynchronously and notifies the listener on completion
     * <p>
     * For additional info
     * see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/ml-delete-filter.html">
     *         ML Delete Filter documentation</a>
     *
     * @param request The request to delete the filter
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteFilterAsync(DeleteFilterRequest request, RequestOptions options,
                                         ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::deleteFilter,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Gets Machine Learning information about default values and limits.
     * <p>
     * For additional info
     * see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/get-ml-info.html">Machine Learning info</a>
     *
     * @param request The request of Machine Learning info
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return response info about default values and limits
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public MlInfoResponse getMlInfo(MlInfoRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::mlInfo,
            options,
            MlInfoResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Gets Machine Learning information about default values and limits, asynchronously.
     * <p>
     * For additional info
     * see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/get-ml-info.html">Machine Learning info</a>
     *
     * @param request The request of Machine Learning info
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getMlInfoAsync(MlInfoRequest request, RequestOptions options, ActionListener<MlInfoResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::mlInfo,
            options,
            MlInfoResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Finds the structure of a file
     * <p>
     * For additional info
     * see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/ml-find-file-structure.html">
     *     ML Find File Structure documentation</a>
     *
     * @param request The find file structure request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response containing details of the file structure
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public FindFileStructureResponse findFileStructure(FindFileStructureRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::findFileStructure,
            options,
            FindFileStructureResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Finds the structure of a file asynchronously and notifies the listener on completion
     * <p>
     * For additional info
     * see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/ml-find-file-structure.html">
     *         ML Find File Structure documentation</a>
     *
     * @param request The find file structure request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable findFileStructureAsync(FindFileStructureRequest request, RequestOptions options,
                                              ActionListener<FindFileStructureResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::findFileStructure,
            options,
            FindFileStructureResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Sets the ML cluster setting upgrade_mode
     * <p>
     * For additional info
     * see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/ml-set-upgrade-mode.html">Set Upgrade Mode</a>
     *
     * @param request The request to set upgrade mode
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return response
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public AcknowledgedResponse setUpgradeMode(SetUpgradeModeRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::setUpgradeMode,
            options,
            AcknowledgedResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Sets the ML cluster setting upgrade_mode asynchronously
     * <p>
     * For additional info
     * see <a href="http://www.elastic.co/guide/en/elasticsearch/reference/current/ml-set-upgrade-mode.html">Set Upgrade Mode</a>
     *
     * @param request The request of Machine Learning info
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable setUpgradeModeAsync(SetUpgradeModeRequest request, RequestOptions options,
                                           ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::setUpgradeMode,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Creates a new Data Frame Analytics config
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/put-dfanalytics.html">
     *     PUT Data Frame Analytics documentation</a>
     *
     * @param request The {@link PutDataFrameAnalyticsRequest} containing the
     * {@link org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsConfig}
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return The {@link PutDataFrameAnalyticsResponse} containing the created
     * {@link org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsConfig}
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public PutDataFrameAnalyticsResponse putDataFrameAnalytics(PutDataFrameAnalyticsRequest request,
                                                               RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::putDataFrameAnalytics,
            options,
            PutDataFrameAnalyticsResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Creates a new Data Frame Analytics config asynchronously and notifies listener upon completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/put-dfanalytics.html">
     *     PUT Data Frame Analytics documentation</a>
     *
     * @param request The {@link PutDataFrameAnalyticsRequest} containing the
     * {@link org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsConfig}
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putDataFrameAnalyticsAsync(PutDataFrameAnalyticsRequest request, RequestOptions options,
                                                  ActionListener<PutDataFrameAnalyticsResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::putDataFrameAnalytics,
            options,
            PutDataFrameAnalyticsResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Gets a single or multiple Data Frame Analytics configs
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/get-dfanalytics.html">
     *     GET Data Frame Analytics documentation</a>
     *
     * @param request The {@link GetDataFrameAnalyticsRequest}
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return {@link GetDataFrameAnalyticsResponse} response object containing the
     * {@link org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsConfig} objects
     */
    public GetDataFrameAnalyticsResponse getDataFrameAnalytics(GetDataFrameAnalyticsRequest request,
                                                               RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::getDataFrameAnalytics,
            options,
            GetDataFrameAnalyticsResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Gets a single or multiple Data Frame Analytics configs asynchronously and notifies listener upon completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/get-dfanalytics.html">
     *     GET Data Frame Analytics documentation</a>
     *
     * @param request The {@link GetDataFrameAnalyticsRequest}
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getDataFrameAnalyticsAsync(GetDataFrameAnalyticsRequest request, RequestOptions options,
                                                  ActionListener<GetDataFrameAnalyticsResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::getDataFrameAnalytics,
            options,
            GetDataFrameAnalyticsResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Gets the running statistics of a Data Frame Analytics
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/get-dfanalytics-stats.html">
     *     GET Data Frame Analytics Stats documentation</a>
     *
     * @param request The {@link GetDataFrameAnalyticsStatsRequest}
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return {@link GetDataFrameAnalyticsStatsResponse} response object
     */
    public GetDataFrameAnalyticsStatsResponse getDataFrameAnalyticsStats(GetDataFrameAnalyticsStatsRequest request,
                                                                         RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::getDataFrameAnalyticsStats,
            options,
            GetDataFrameAnalyticsStatsResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Gets the running statistics of a Data Frame Analytics asynchronously and notifies listener upon completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/get-dfanalytics-stats.html">
     *     GET Data Frame Analytics Stats documentation</a>
     *
     * @param request The {@link GetDataFrameAnalyticsStatsRequest}
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getDataFrameAnalyticsStatsAsync(GetDataFrameAnalyticsStatsRequest request, RequestOptions options,
                                                       ActionListener<GetDataFrameAnalyticsStatsResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::getDataFrameAnalyticsStats,
            options,
            GetDataFrameAnalyticsStatsResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Starts Data Frame Analytics
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/start-dfanalytics.html">
     *     Start Data Frame Analytics documentation</a>
     *
     * @param request The {@link StartDataFrameAnalyticsRequest}
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return action acknowledgement
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public AcknowledgedResponse startDataFrameAnalytics(StartDataFrameAnalyticsRequest request,
                                                        RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::startDataFrameAnalytics,
            options,
            AcknowledgedResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Starts Data Frame Analytics asynchronously and notifies listener upon completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/start-dfanalytics.html">
     *     Start Data Frame Analytics documentation</a>
     *
     *  @param request The {@link StartDataFrameAnalyticsRequest}
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable startDataFrameAnalyticsAsync(StartDataFrameAnalyticsRequest request, RequestOptions options,
                                                    ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::startDataFrameAnalytics,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Stops Data Frame Analytics
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/stop-dfanalytics.html">
     *     Stop Data Frame Analytics documentation</a>
     *
     * @param request The {@link StopDataFrameAnalyticsRequest}
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return {@link StopDataFrameAnalyticsResponse}
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public StopDataFrameAnalyticsResponse stopDataFrameAnalytics(StopDataFrameAnalyticsRequest request,
                                                                 RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::stopDataFrameAnalytics,
            options,
            StopDataFrameAnalyticsResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Stops Data Frame Analytics asynchronously and notifies listener upon completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/stop-dfanalytics.html">
     *     Stop Data Frame Analytics documentation</a>
     *
     * @param request The {@link StopDataFrameAnalyticsRequest}
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable stopDataFrameAnalyticsAsync(StopDataFrameAnalyticsRequest request, RequestOptions options,
                                                   ActionListener<StopDataFrameAnalyticsResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::stopDataFrameAnalytics,
            options,
            StopDataFrameAnalyticsResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Deletes the given Data Frame Analytics config
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/delete-dfanalytics.html">
     *     DELETE Data Frame Analytics documentation</a>
     *
     * @param request The {@link DeleteDataFrameAnalyticsRequest}
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return action acknowledgement
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public AcknowledgedResponse deleteDataFrameAnalytics(DeleteDataFrameAnalyticsRequest request,
                                                         RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::deleteDataFrameAnalytics,
            options,
            AcknowledgedResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Deletes the given Data Frame Analytics config asynchronously and notifies listener upon completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/delete-dfanalytics.html">
     *     DELETE Data Frame Analytics documentation</a>
     *
     * @param request The {@link DeleteDataFrameAnalyticsRequest}
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteDataFrameAnalyticsAsync(DeleteDataFrameAnalyticsRequest request, RequestOptions options,
                                                     ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::deleteDataFrameAnalytics,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Evaluates the given Data Frame
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/evaluate-dfanalytics.html">
     *     Evaluate Data Frame documentation</a>
     *
     * @param request The {@link EvaluateDataFrameRequest}
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return {@link EvaluateDataFrameResponse} response object
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public EvaluateDataFrameResponse evaluateDataFrame(EvaluateDataFrameRequest request,
                                                       RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            MLRequestConverters::evaluateDataFrame,
            options,
            EvaluateDataFrameResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Evaluates the given Data Frame asynchronously and notifies listener upon completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/evaluate-dfanalytics.html">
     *     Evaluate Data Frame documentation</a>
     *
     * @param request The {@link EvaluateDataFrameRequest}
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable evaluateDataFrameAsync(EvaluateDataFrameRequest request, RequestOptions options,
                                              ActionListener<EvaluateDataFrameResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            MLRequestConverters::evaluateDataFrame,
            options,
            EvaluateDataFrameResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Estimates memory usage for the given Data Frame Analytics
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/estimate-memory-usage-dfanalytics.html">
     *     Estimate Memory Usage for Data Frame Analytics documentation</a>
     *
     * @param request The {@link PutDataFrameAnalyticsRequest}
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return {@link EstimateMemoryUsageResponse} response object
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public EstimateMemoryUsageResponse estimateMemoryUsage(PutDataFrameAnalyticsRequest request,
                                                           RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            MLRequestConverters::estimateMemoryUsage,
            options,
            EstimateMemoryUsageResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Estimates memory usage for the given Data Frame Analytics asynchronously and notifies listener upon completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/estimate-memory-usage-dfanalytics.html">
     *     Estimate Memory Usage for Data Frame Analytics documentation</a>
     *
     * @param request The {@link PutDataFrameAnalyticsRequest}
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable estimateMemoryUsageAsync(PutDataFrameAnalyticsRequest request, RequestOptions options,
                                                ActionListener<EstimateMemoryUsageResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            MLRequestConverters::estimateMemoryUsage,
            options,
            EstimateMemoryUsageResponse::fromXContent,
            listener,
            Collections.emptySet());
    }
}
