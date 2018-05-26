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
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineResponse;

import java.io.IOException;

import static java.util.Collections.emptySet;

/**
 * A wrapper for the {@link RestHighLevelClient} that provides methods for accessing the Cluster API.
 * <p>
 * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster.html">Cluster API on elastic.co</a>
 */
public final class ClusterClient {
    private final RestHighLevelClient restHighLevelClient;

    ClusterClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Updates cluster wide specific settings using the Cluster Update Settings API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-update-settings.html"> Cluster Update Settings
     * API on elastic.co</a>
     */
    public ClusterUpdateSettingsResponse putSettings(ClusterUpdateSettingsRequest clusterUpdateSettingsRequest, Header... headers)
            throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(clusterUpdateSettingsRequest, RequestConverters::clusterPutSettings,
                ClusterUpdateSettingsResponse::fromXContent, emptySet(), headers);
    }

    /**
     * Asynchronously updates cluster wide specific settings using the Cluster Update Settings API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-update-settings.html"> Cluster Update Settings
     * API on elastic.co</a>
     */
    public void putSettingsAsync(ClusterUpdateSettingsRequest clusterUpdateSettingsRequest,
            ActionListener<ClusterUpdateSettingsResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(clusterUpdateSettingsRequest, RequestConverters::clusterPutSettings,
                ClusterUpdateSettingsResponse::fromXContent, listener, emptySet(), headers);
    }

    /**
     * Get current tasks using the Task Management API
     * <p>
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/tasks.html"> Task Management API on elastic.co</a>
     */
    public ListTasksResponse listTasks(ListTasksRequest request, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, RequestConverters::listTasks, ListTasksResponse::fromXContent,
                emptySet(), headers);
    }

    /**
     * Asynchronously get current tasks using the Task Management API
     * <p>
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/tasks.html"> Task Management API on elastic.co</a>
     */
    public void listTasksAsync(ListTasksRequest request, ActionListener<ListTasksResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request, RequestConverters::listTasks, ListTasksResponse::fromXContent,
                listener, emptySet(), headers);
    }

    /**
     * Add a pipeline or update an existing pipeline in the cluster
     * <p>
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/put-pipeline-api.html"> Put Pipeline API on elastic.co</a>
     */
    public PutPipelineResponse putPipeline(PutPipelineRequest request, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity( request, RequestConverters::putPipeline,
            PutPipelineResponse::fromXContent, emptySet(), headers);
    }

    /**
     * Asynchronously add a pipeline or update an existing pipeline in the cluster
     * <p>
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/put-pipeline-api.html"> Put Pipeline API on elastic.co</a>
     */
    public void putPipelineAsync(PutPipelineRequest request, ActionListener<PutPipelineResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity( request, RequestConverters::putPipeline,
            PutPipelineResponse::fromXContent, listener, emptySet(), headers);
    }
}
