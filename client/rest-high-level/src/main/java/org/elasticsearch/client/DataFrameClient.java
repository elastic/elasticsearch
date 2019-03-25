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
import org.elasticsearch.client.dataframe.PreviewDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.PreviewDataFrameTransformResponse;
import org.elasticsearch.client.dataframe.PutDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.StartDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.StartDataFrameTransformResponse;
import org.elasticsearch.client.dataframe.StopDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.StopDataFrameTransformResponse;

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
     * see <a href="https://www.TODO.com">Data Frame PUT transform documentation</a>
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
     * see <a href="https://www.TODO.com">Data Frame PUT transform documentation</a>
     *
     * @param request The PutDataFrameTransformRequest containing the
     * {@link org.elasticsearch.client.dataframe.transforms.DataFrameTransformConfig}.
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     */
    public void putDataFrameTransformAsync(PutDataFrameTransformRequest request, RequestOptions options,
                                      ActionListener<AcknowledgedResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request,
                DataFrameRequestConverters::putDataFrameTransform,
                options,
                AcknowledgedResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Delete a data frame transform
     * <p>
     * For additional info
     * see <a href="https://www.TODO.com">Data Frame delete transform documentation</a>
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
     * see <a href="https://www.TODO.com">Data Frame delete transform documentation</a>
     *
     * @param request The delete data frame transform request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     */
    public void deleteDataFrameTransformAsync(DeleteDataFrameTransformRequest request, RequestOptions options,
                                              ActionListener<AcknowledgedResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request,
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
     * see <a href="https://www.TODO.com">Preview Data Frame transform documentation</a>
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
     * For additional info
     * see <a href="https://www.TODO.com">Preview Data Frame transform documentation</a>
     *
     * @param request The preview data frame transform request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     */
    public void previewDataFrameTransformAsync(PreviewDataFrameTransformRequest request, RequestOptions options,
                                             ActionListener<PreviewDataFrameTransformResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request,
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
     * see <a href="https://www.TODO.com">Start Data Frame transform documentation</a>
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
     * see <a href="https://www.TODO.com">Start Data Frame transform documentation</a>
     *
     * @param request The start data frame transform request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     */
    public void startDataFrameTransformAsync(StartDataFrameTransformRequest request, RequestOptions options,
                                            ActionListener<StartDataFrameTransformResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request,
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
     * see <a href="https://www.TODO.com">Stop Data Frame transform documentation</a>
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
     * see <a href="https://www.TODO.com">Stop Data Frame transform documentation</a>
     *
     * @param request The stop data frame transform request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     */
    public void stopDataFrameTransformAsync(StopDataFrameTransformRequest request, RequestOptions options,
                                              ActionListener<StopDataFrameTransformResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request,
                DataFrameRequestConverters::stopDataFrameTransform,
                options,
                StopDataFrameTransformResponse::fromXContent,
                listener,
                Collections.emptySet());
    }
}
