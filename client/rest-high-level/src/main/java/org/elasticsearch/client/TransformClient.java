/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.client.transform.DeleteTransformRequest;
import org.elasticsearch.client.transform.GetTransformRequest;
import org.elasticsearch.client.transform.GetTransformResponse;
import org.elasticsearch.client.transform.GetTransformStatsRequest;
import org.elasticsearch.client.transform.GetTransformStatsResponse;
import org.elasticsearch.client.transform.PreviewTransformRequest;
import org.elasticsearch.client.transform.PreviewTransformResponse;
import org.elasticsearch.client.transform.PutTransformRequest;
import org.elasticsearch.client.transform.StartTransformRequest;
import org.elasticsearch.client.transform.StartTransformResponse;
import org.elasticsearch.client.transform.StopTransformRequest;
import org.elasticsearch.client.transform.StopTransformResponse;
import org.elasticsearch.client.transform.UpdateTransformRequest;
import org.elasticsearch.client.transform.UpdateTransformResponse;

import java.io.IOException;
import java.util.Collections;

public final class TransformClient {

    private final RestHighLevelClient restHighLevelClient;

    TransformClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Creates a new transform
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/put-transform.html">
     *     Create transform documentation</a>
     *
     * @param request The PutTransformRequest containing the
     * {@link org.elasticsearch.client.transform.transforms.TransformConfig}.
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return An AcknowledgedResponse object indicating request success
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public AcknowledgedResponse putTransform(PutTransformRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
                TransformRequestConverters::putTransform,
                options,
                AcknowledgedResponse::fromXContent,
                Collections.emptySet());
    }

    /**
     * Creates a new transform asynchronously and notifies listener on completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/put-transform.html">
     *     Create transform documentation</a>
     * @param request The PutTransformRequest containing the
     * {@link org.elasticsearch.client.transform.transforms.TransformConfig}.
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putTransformAsync(PutTransformRequest request, RequestOptions options,
                                         ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                TransformRequestConverters::putTransform,
                options,
                AcknowledgedResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Updates an existing transform
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/update-transform.html">
     *     Create transform documentation</a>
     *
     * @param request The UpdateTransformRequest containing the
     * {@link org.elasticsearch.client.transform.transforms.TransformConfigUpdate}.
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return An UpdateTransformResponse object containing the updated configuration
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public UpdateTransformResponse updateTransform(UpdateTransformRequest request,
                                                   RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            TransformRequestConverters::updateTransform,
            options,
            UpdateTransformResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Updates an existing transform asynchronously and notifies listener on completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/update-transform.html">
     *     Create transform documentation</a>
     * @param request The UpdateTransformRequest containing the
     * {@link org.elasticsearch.client.transform.transforms.TransformConfigUpdate}.
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable updateTransformAsync(UpdateTransformRequest request,
                                            RequestOptions options,
                                            ActionListener<UpdateTransformResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            TransformRequestConverters::updateTransform,
            options,
            UpdateTransformResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Get the running statistics of a transform
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/get-transform-stats.html">
     *     Get transform stats documentation</a>
     *
     * @param request Specifies which transforms to get the stats for
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return The transform stats
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public GetTransformStatsResponse getTransformStats(GetTransformStatsRequest request, RequestOptions options)
            throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
                TransformRequestConverters::getTransformStats,
                options,
                GetTransformStatsResponse::fromXContent,
                Collections.emptySet());
    }

    /**
     * Get the running statistics of a transform asynchronously and notifies listener on completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/get-transform-stats.html">
     *     Get transform stats documentation</a>
     * @param request Specifies which transforms to get the stats for
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getTransformStatsAsync(GetTransformStatsRequest request, RequestOptions options,
                                              ActionListener<GetTransformStatsResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                TransformRequestConverters::getTransformStats,
                options,
                GetTransformStatsResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Delete a transform
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/delete-transform.html">
     *     Delete transform documentation</a>
     *
     * @param request The delete transform request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return An AcknowledgedResponse object indicating request success
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public AcknowledgedResponse deleteTransform(DeleteTransformRequest request, RequestOptions options)
            throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
                TransformRequestConverters::deleteTransform,
                options,
                AcknowledgedResponse::fromXContent,
                Collections.emptySet());
    }

    /**
     * Delete a transform asynchronously and notifies listener on completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/delete-transform.html">
     *     Delete transform documentation</a>
     * @param request The delete transform request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteTransformAsync(DeleteTransformRequest request, RequestOptions options,
                                            ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                TransformRequestConverters::deleteTransform,
                options,
                AcknowledgedResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Preview the result of a transform
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/preview-transform.html">
     *     Preview transform documentation</a>
     *
     * @param request The preview transform request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return A response containing the results of the applied transform
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public PreviewTransformResponse previewTransform(PreviewTransformRequest request, RequestOptions options)
            throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
                TransformRequestConverters::previewTransform,
                options,
                PreviewTransformResponse::fromXContent,
                Collections.emptySet());
    }

    /**
     * Preview the result of a transform asynchronously and notifies listener on completion
     * <p>
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/preview-transform.html">
     *     Preview transform documentation</a>
     * @param request The preview transform request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable previewTransformAsync(PreviewTransformRequest request, RequestOptions options,
                                             ActionListener<PreviewTransformResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                TransformRequestConverters::previewTransform,
                options,
                PreviewTransformResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Start a transform
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/start-transform.html">
     *     Start transform documentation</a>
     *
     * @param request The start transform request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return A response object indicating request success
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public StartTransformResponse startTransform(StartTransformRequest request, RequestOptions options)
            throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
                TransformRequestConverters::startTransform,
                options,
                StartTransformResponse::fromXContent,
                Collections.emptySet());
    }

    /**
     * Start a transform asynchronously and notifies listener on completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/start-transform.html">
     *     Start transform documentation</a>
     * @param request The start transform request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable startTransformAsync(StartTransformRequest request, RequestOptions options,
                                           ActionListener<StartTransformResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                TransformRequestConverters::startTransform,
                options,
                StartTransformResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Stop a transform
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/stop-transform.html">
     *     Stop transform documentation</a>
     *
     * @param request The stop transform request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return A response object indicating request success
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public StopTransformResponse stopTransform(StopTransformRequest request, RequestOptions options)
            throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
                TransformRequestConverters::stopTransform,
                options,
                StopTransformResponse::fromXContent,
                Collections.emptySet());
    }

    /**
     * Stop a transform asynchronously and notifies listener on completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/stop-transform.html">
     *     Stop transform documentation</a>
     * @param request The stop transform request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable stopTransformAsync(StopTransformRequest request, RequestOptions options,
                                          ActionListener<StopTransformResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                TransformRequestConverters::stopTransform,
                options,
                StopTransformResponse::fromXContent,
                listener,
                Collections.emptySet());
    }

    /**
     * Get one or more transform configurations
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/get-transform.html">
     *     Get transform documentation</a>
     *
     * @param request The get transform request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return An GetTransformResponse containing the requested transforms
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public GetTransformResponse getTransform(GetTransformRequest request, RequestOptions options)
            throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
                TransformRequestConverters::getTransform,
                options,
                GetTransformResponse::fromXContent,
                Collections.emptySet());
    }

    /**
     * Get one or more transform configurations asynchronously and notifies listener on completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/get-transform.html">
     *     Get data transform documentation</a>
     * @param request The get transform request
     * @param options Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getTransformAsync(GetTransformRequest request, RequestOptions options,
                                         ActionListener<GetTransformResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
                TransformRequestConverters::getTransform,
                options,
                GetTransformResponse::fromXContent,
                listener,
                Collections.emptySet());
    }
}
