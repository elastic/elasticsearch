/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.client.dataframe.DeleteDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.GetDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.GetDataFrameTransformResponse;
import org.elasticsearch.client.dataframe.GetDataFrameTransformStatsRequest;
import org.elasticsearch.client.dataframe.GetDataFrameTransformStatsResponse;
import org.elasticsearch.client.dataframe.PreviewDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.PreviewDataFrameTransformResponse;
import org.elasticsearch.client.dataframe.PutDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.StartDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.StartDataFrameTransformResponse;
import org.elasticsearch.client.dataframe.StopDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.StopDataFrameTransformResponse;
import org.elasticsearch.client.dataframe.UpdateDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.UpdateDataFrameTransformResponse;

import java.io.IOException;
import java.util.Collections;

public final class DataFrameClient {

    private final RestHighLevelClient restHighLevelClient;

    DataFrameClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Creates a new Data Frame Transform
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/put-data-frame-transform.html">
     *     Create data frame transform documentation</a>
     *
     * @param request The PutDataFrameTransformRequest containing the
     * {@link org.elasticsearch.client.dataframe.transforms.DataFrameTransformConfig}.
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return An AcknowledgedResponse object indicating request success
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public AcknowledgedResponse putDataFrameTransform(PutDataFrameTransformRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
                DataFrameRequestConverters::putDataFrameTransform,
                options,
                AcknowledgedResponse::fromXContent,
                Collections.emptySet());
    }

    /**
     * Creates a new Data Frame Transform asynchronously and notifies listener on completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/put-data-frame-transform.html">
     *     Create data frame transform documentation</a>
     * @param request The PutDataFrameTransformRequest containing the
     * {@link org.elasticsearch.client.dataframe.transforms.DataFrameTransformConfig}.
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putDataFrameTransformAsync(PutDataFrameTransformRequest request, RequestOptions options,
                                                  ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                DataFrameRequestConverters::putDataFrameTransform,
                options,
                AcknowledgedResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Updates an existing Data Frame Transform
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/update-data-frame-transform.html">
     *     Create data frame transform documentation</a>
     *
     * @param request The UpdateDataFrameTransformRequest containing the
     * {@link org.elasticsearch.client.dataframe.transforms.DataFrameTransformConfigUpdate}.
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return An UpdateDataFrameTransformResponse object containing the updated configuration
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public UpdateDataFrameTransformResponse updateDataFrameTransform(UpdateDataFrameTransformRequest request,
                                                                     RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            DataFrameRequestConverters::updateDataFrameTransform,
            options,
            UpdateDataFrameTransformResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Updates an existing Data Frame Transform asynchronously and notifies listener on completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/update-data-frame-transform.html">
     *     Create data frame transform documentation</a>
     * @param request The UpdateDataFrameTransformRequest containing the
     * {@link org.elasticsearch.client.dataframe.transforms.DataFrameTransformConfigUpdate}.
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable updateDataFrameTransformAsync(UpdateDataFrameTransformRequest request,
                                                     RequestOptions options,
                                                     ActionListener<UpdateDataFrameTransformResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            DataFrameRequestConverters::updateDataFrameTransform,
            options,
            UpdateDataFrameTransformResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Get the running statistics of a Data Frame Transform
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/get-data-frame-transform-stats.html">
     *     Get data frame transform stats documentation</a>
     *
     * @param request Specifies the which transforms to get the stats for
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return The Data Frame Transform stats
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public GetDataFrameTransformStatsResponse getDataFrameTransformStats(GetDataFrameTransformStatsRequest request, RequestOptions options)
            throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
                DataFrameRequestConverters::getDataFrameTransformStats,
                options,
                GetDataFrameTransformStatsResponse::fromXContent,
                Collections.emptySet());
    }

    /**
     * Get the running statistics of a Data Frame Transform asynchronously and notifies listener on completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/get-data-frame-transform-stats.html">
     *     Get data frame transform stats documentation</a>
     * @param request Specifies the which transforms to get the stats for
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getDataFrameTransformStatsAsync(GetDataFrameTransformStatsRequest request, RequestOptions options,
                                                       ActionListener<GetDataFrameTransformStatsResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                DataFrameRequestConverters::getDataFrameTransformStats,
                options,
                GetDataFrameTransformStatsResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Delete a data frame transform
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/delete-data-frame-transform.html">
     *     Delete data frame transform documentation</a>
     *
     * @param request The delete data frame transform request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return An AcknowledgedResponse object indicating request success
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public AcknowledgedResponse deleteDataFrameTransform(DeleteDataFrameTransformRequest request, RequestOptions options)
            throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
                DataFrameRequestConverters::deleteDataFrameTransform,
                options,
                AcknowledgedResponse::fromXContent,
                Collections.emptySet());
    }

    /**
     * Delete a data frame transform asynchronously and notifies listener on completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/delete-data-frame-transform.html">
     *     Delete data frame transform documentation</a>
     * @param request The delete data frame transform request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteDataFrameTransformAsync(DeleteDataFrameTransformRequest request, RequestOptions options,
                                                     ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                DataFrameRequestConverters::deleteDataFrameTransform,
                options,
                AcknowledgedResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Preview the result of a data frame transform
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/preview-data-frame-transform.html">
     *     Preview data frame transform documentation</a>
     *
     * @param request The preview data frame transform request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return A response containing the results of the applied transform
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public PreviewDataFrameTransformResponse previewDataFrameTransform(PreviewDataFrameTransformRequest request, RequestOptions options)
            throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
                DataFrameRequestConverters::previewDataFrameTransform,
                options,
                PreviewDataFrameTransformResponse::fromXContent,
                Collections.emptySet());
    }

    /**
     * Preview the result of a data frame transform asynchronously and notifies listener on completion
     * <p>
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/preview-data-frame-transform.html">
     *     Preview data frame transform documentation</a>
     * @param request The preview data frame transform request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable previewDataFrameTransformAsync(PreviewDataFrameTransformRequest request, RequestOptions options,
                                                      ActionListener<PreviewDataFrameTransformResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                DataFrameRequestConverters::previewDataFrameTransform,
                options,
                PreviewDataFrameTransformResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Start a data frame transform
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/start-data-frame-transform.html">
     *     Start data frame transform documentation</a>
     *
     * @param request The start data frame transform request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return A response object indicating request success
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public StartDataFrameTransformResponse startDataFrameTransform(StartDataFrameTransformRequest request, RequestOptions options)
            throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
                DataFrameRequestConverters::startDataFrameTransform,
                options,
                StartDataFrameTransformResponse::fromXContent,
                Collections.emptySet());
    }

    /**
     * Start a data frame transform asynchronously and notifies listener on completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/start-data-frame-transform.html">
     *     Start data frame transform documentation</a>
     * @param request The start data frame transform request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable startDataFrameTransformAsync(StartDataFrameTransformRequest request, RequestOptions options,
                                                    ActionListener<StartDataFrameTransformResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                DataFrameRequestConverters::startDataFrameTransform,
                options,
                StartDataFrameTransformResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Stop a data frame transform
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/stop-data-frame-transform.html">
     *     Stop data frame transform documentation</a>
     *
     * @param request The stop data frame transform request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return A response object indicating request success
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public StopDataFrameTransformResponse stopDataFrameTransform(StopDataFrameTransformRequest request, RequestOptions options)
            throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
                DataFrameRequestConverters::stopDataFrameTransform,
                options,
                StopDataFrameTransformResponse::fromXContent,
                Collections.emptySet());
    }

    /**
     * Stop a data frame transform asynchronously and notifies listener on completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/stop-data-frame-transform.html">
     *     Stop data frame transform documentation</a>
     * @param request The stop data frame transform request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable stopDataFrameTransformAsync(StopDataFrameTransformRequest request, RequestOptions options,
                                                   ActionListener<StopDataFrameTransformResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                DataFrameRequestConverters::stopDataFrameTransform,
                options,
                StopDataFrameTransformResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Get one or more data frame transform configurations
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/get-data-frame-transform.html">
     *     Get data frame transform documentation</a>
     *
     * @param request The get data frame transform request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return An GetDataFrameTransformResponse containing the requested transforms
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public GetDataFrameTransformResponse getDataFrameTransform(GetDataFrameTransformRequest request, RequestOptions options)
            throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
                DataFrameRequestConverters::getDataFrameTransform,
                options,
                GetDataFrameTransformResponse::fromXContent,
                Collections.emptySet());
    }

    /**
     * Get one or more data frame transform configurations asynchronously and notifies listener on completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/get-data-frame-transform.html">
     *     Get data frame transform documentation</a>
     * @param request The get data frame transform request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getDataFrameTransformAsync(GetDataFrameTransformRequest request, RequestOptions options,
                                                  ActionListener<GetDataFrameTransformResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                DataFrameRequestConverters::getDataFrameTransform,
                options,
                GetDataFrameTransformResponse::fromXContent,
                listener,
                Collections.emptySet());
    }
}
