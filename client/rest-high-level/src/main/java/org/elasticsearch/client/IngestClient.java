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
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.GetPipelineRequest;
import org.elasticsearch.action.ingest.GetPipelineResponse;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.ingest.WritePipelineResponse;

import java.io.IOException;

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
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public WritePipelineResponse putPipeline(PutPipelineRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity( request, RequestConverters::putPipeline, options,
            WritePipelineResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously add a pipeline or update an existing pipeline.
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/put-pipeline-api.html"> Put Pipeline API on elastic.co</a>
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void putPipelineAsync(PutPipelineRequest request, RequestOptions options, ActionListener<WritePipelineResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity( request, RequestConverters::putPipeline, options,
            WritePipelineResponse::fromXContent, listener, emptySet());
    }

    /**
     * Get an existing pipeline.
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/get-pipeline-api.html"> Get Pipeline API on elastic.co</a>
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetPipelineResponse getPipeline(GetPipelineRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity( request, RequestConverters::getPipeline, options,
            GetPipelineResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously get an existing pipeline.
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/master/get-pipeline-api.html"> Get Pipeline API on elastic.co</a>
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void getPipelineAsync(GetPipelineRequest request, RequestOptions options, ActionListener<GetPipelineResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity( request, RequestConverters::getPipeline, options,
            GetPipelineResponse::fromXContent, listener, emptySet());
    }

    /**
     * Delete an existing pipeline.
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/delete-pipeline-api.html">
     *     Delete Pipeline API on elastic.co</a>
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public WritePipelineResponse deletePipeline(DeletePipelineRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity( request, RequestConverters::deletePipeline, options,
            WritePipelineResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously delete an existing pipeline.
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/delete-pipeline-api.html">
     *     Delete Pipeline API on elastic.co</a>
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void deletePipelineAsync(DeletePipelineRequest request, RequestOptions options, ActionListener<WritePipelineResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity( request, RequestConverters::deletePipeline, options,
            WritePipelineResponse::fromXContent, listener, emptySet());
    }
}
