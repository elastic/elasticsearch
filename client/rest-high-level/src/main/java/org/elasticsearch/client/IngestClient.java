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

import org.apache.http.Header;
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
     * Add a pipeline or update an existing pipeline
     * <p>
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/put-pipeline-api.html"> Put Pipeline API on elastic.co</a>
     */
    public WritePipelineResponse putPipeline(PutPipelineRequest request, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity( request, RequestConverters::putPipeline,
            WritePipelineResponse::fromXContent, emptySet(), headers);
    }

    /**
     * Asynchronously add a pipeline or update an existing pipeline
     * <p>
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/put-pipeline-api.html"> Put Pipeline API on elastic.co</a>
     */
    public void putPipelineAsync(PutPipelineRequest request, ActionListener<WritePipelineResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity( request, RequestConverters::putPipeline,
            WritePipelineResponse::fromXContent, listener, emptySet(), headers);
    }

    /**
     * Get an existing pipeline
     * <p>
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/get-pipeline-api.html"> Get Pipeline API on elastic.co</a>
     */
    public GetPipelineResponse getPipeline(GetPipelineRequest request, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity( request, RequestConverters::getPipeline,
            GetPipelineResponse::fromXContent, emptySet(), headers);
    }

    /**
     * Asynchronously get an existing pipeline
     * <p>
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/master/get-pipeline-api.html"> Get Pipeline API on elastic.co</a>
     */
    public void getPipelineAsync(GetPipelineRequest request, ActionListener<GetPipelineResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity( request, RequestConverters::getPipeline,
            GetPipelineResponse::fromXContent, listener, emptySet(), headers);
    }

    /**
     * Delete an existing pipeline
     * <p>
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/delete-pipeline-api.html">
     *     Delete Pipeline API on elastic.co</a>
     */
    public WritePipelineResponse deletePipeline(DeletePipelineRequest request, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity( request, RequestConverters::deletePipeline,
            WritePipelineResponse::fromXContent, emptySet(), headers);
    }

    /**
     * Asynchronously delete an existing pipeline
     * <p>
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/delete-pipeline-api.html">
     *     Delete Pipeline API on elastic.co</a>
     */
    public void deletePipelineAsync(DeletePipelineRequest request, ActionListener<WritePipelineResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity( request, RequestConverters::deletePipeline,
            WritePipelineResponse::fromXContent, listener, emptySet(), headers);
    }
}
