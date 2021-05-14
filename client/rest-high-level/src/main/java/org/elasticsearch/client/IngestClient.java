/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.GetPipelineRequest;
import org.elasticsearch.action.ingest.GetPipelineResponse;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.ingest.SimulatePipelineRequest;
import org.elasticsearch.action.ingest.SimulatePipelineResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.core.MainRequest;

import java.io.IOException;
import java.util.Collections;

import static java.util.Collections.emptySet;

/**
 * A wrapper for the {@link RestHighLevelClient} that provides methods for accessing the Ingest API.
 * <p>
 * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ingest.html">Ingest API on elastic.co</a>
 */
public final class IngestClient {

    private final RestHighLevelClient restHighLevelClient;

    IngestClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Add a pipeline or update an existing pipeline.
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/put-pipeline-api.html"> Put Pipeline API on elastic.co</a>
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse putPipeline(PutPipelineRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, IngestRequestConverters::putPipeline, options,
            AcknowledgedResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously add a pipeline or update an existing pipeline.
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/put-pipeline-api.html"> Put Pipeline API on elastic.co</a>
     *
     * @param request  the request
     * @param options  the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putPipelineAsync(PutPipelineRequest request, RequestOptions options, ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, IngestRequestConverters::putPipeline, options,
            AcknowledgedResponse::fromXContent, listener, emptySet());
    }

    /**
     * Get an existing pipeline.
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/get-pipeline-api.html"> Get Pipeline API on elastic.co</a>
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetPipelineResponse getPipeline(GetPipelineRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, IngestRequestConverters::getPipeline, options,
            GetPipelineResponse::fromXContent, Collections.singleton(404));
    }

    /**
     * Asynchronously get an existing pipeline.
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/master/get-pipeline-api.html"> Get Pipeline API on elastic.co</a>
     *
     * @param request  the request
     * @param options  the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getPipelineAsync(GetPipelineRequest request, RequestOptions options, ActionListener<GetPipelineResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, IngestRequestConverters::getPipeline, options,
            GetPipelineResponse::fromXContent, listener, Collections.singleton(404));
    }

    /**
     * Delete an existing pipeline.
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/delete-pipeline-api.html">
     * Delete Pipeline API on elastic.co</a>
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse deletePipeline(DeletePipelineRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, IngestRequestConverters::deletePipeline, options,
            AcknowledgedResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously delete an existing pipeline.
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/delete-pipeline-api.html">
     * Delete Pipeline API on elastic.co</a>
     *
     * @param request  the request
     * @param options  the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deletePipelineAsync(DeletePipelineRequest request, RequestOptions options,
                                           ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            IngestRequestConverters::deletePipeline, options,
            AcknowledgedResponse::fromXContent, listener, emptySet());
    }

    /**
     * Simulate a pipeline on a set of documents provided in the request
     * <p>
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/master/simulate-pipeline-api.html">
     * Simulate Pipeline API on elastic.co</a>
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public SimulatePipelineResponse simulate(SimulatePipelineRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, IngestRequestConverters::simulatePipeline, options,
            SimulatePipelineResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously simulate a pipeline on a set of documents provided in the request
     * <p>
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/master/simulate-pipeline-api.html">
     * Simulate Pipeline API on elastic.co</a>
     *
     * @param request  the request
     * @param options  the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable simulateAsync(SimulatePipelineRequest request,
                                     RequestOptions options,
                                     ActionListener<SimulatePipelineResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, IngestRequestConverters::simulatePipeline, options,
            SimulatePipelineResponse::fromXContent, listener, emptySet());
    }

    public GeoIpStatsResponse geoIpStats(MainRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(new MainRequest(), IngestRequestConverters::geoIpStats, options,
            GeoIpStatsResponse::fromXContent, emptySet());
    }

    public Cancellable geoIpStatsAsync(MainRequest request, RequestOptions options, ActionListener<GeoIpStatsResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, IngestRequestConverters::geoIpStats, options,
            GeoIpStatsResponse::fromXContent, listener, emptySet());
    }
}
