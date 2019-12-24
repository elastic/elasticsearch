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
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.client.cluster.RemoteInfoRequest;
import org.elasticsearch.client.cluster.RemoteInfoResponse;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

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
     * Updates cluster wide specific settings using the Cluster Update Settings API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-update-settings.html"> Cluster Update Settings
     * API on elastic.co</a>
     * @param clusterUpdateSettingsRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public ClusterUpdateSettingsResponse putSettings(ClusterUpdateSettingsRequest clusterUpdateSettingsRequest, RequestOptions options)
            throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(clusterUpdateSettingsRequest, ClusterRequestConverters::clusterPutSettings,
                options, ClusterUpdateSettingsResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously updates cluster wide specific settings using the Cluster Update Settings API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-update-settings.html"> Cluster Update Settings
     * API on elastic.co</a>
     * @param clusterUpdateSettingsRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putSettingsAsync(ClusterUpdateSettingsRequest clusterUpdateSettingsRequest, RequestOptions options,
                                        ActionListener<ClusterUpdateSettingsResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(clusterUpdateSettingsRequest,
                ClusterRequestConverters::clusterPutSettings,
                options, ClusterUpdateSettingsResponse::fromXContent, listener, emptySet());
    }

    /**
     * Get the cluster wide settings using the Cluster Get Settings API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-get-settings.html"> Cluster Get Settings
     * API on elastic.co</a>
     * @param clusterGetSettingsRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public ClusterGetSettingsResponse getSettings(ClusterGetSettingsRequest clusterGetSettingsRequest, RequestOptions options)
        throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(clusterGetSettingsRequest, ClusterRequestConverters::clusterGetSettings,
            options, ClusterGetSettingsResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously get the cluster wide settings using the Cluster Get Settings API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-get-settings.html"> Cluster Get Settings
     * API on elastic.co</a>
     * @param clusterGetSettingsRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getSettingsAsync(ClusterGetSettingsRequest clusterGetSettingsRequest, RequestOptions options,
                                        ActionListener<ClusterGetSettingsResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            clusterGetSettingsRequest, ClusterRequestConverters::clusterGetSettings,
            options, ClusterGetSettingsResponse::fromXContent, listener, emptySet());
    }

    /**
     * Get cluster health using the Cluster Health API.
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-health.html"> Cluster Health API on elastic.co</a>
     * <p>
     * If timeout occurred, {@link ClusterHealthResponse} will have isTimedOut() == true and status() == RestStatus.REQUEST_TIMEOUT
     * @param healthRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public ClusterHealthResponse health(ClusterHealthRequest healthRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(healthRequest, ClusterRequestConverters::clusterHealth, options,
                ClusterHealthResponse::fromXContent, singleton(RestStatus.REQUEST_TIMEOUT.getStatus()));
    }

    /**
     * Asynchronously get cluster health using the Cluster Health API.
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-health.html"> Cluster Health API on elastic.co</a>
     * If timeout occurred, {@link ClusterHealthResponse} will have isTimedOut() == true and status() == RestStatus.REQUEST_TIMEOUT
     * @param healthRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable healthAsync(ClusterHealthRequest healthRequest, RequestOptions options,
                                   ActionListener<ClusterHealthResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(healthRequest, ClusterRequestConverters::clusterHealth, options,
                ClusterHealthResponse::fromXContent, listener, singleton(RestStatus.REQUEST_TIMEOUT.getStatus()));
    }

    /**
     * Get the remote cluster information using the Remote cluster info API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-remote-info.html"> Remote cluster info
     * API on elastic.co</a>
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public RemoteInfoResponse remoteInfo(RemoteInfoRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, ClusterRequestConverters::remoteInfo, options,
                RemoteInfoResponse::fromXContent, singleton(RestStatus.REQUEST_TIMEOUT.getStatus()));
    }

    /**
     * Asynchronously get remote cluster information using the Remote cluster info API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-remote-info.html"> Remote cluster info
     * API on elastic.co</a>
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable remoteInfoAsync(RemoteInfoRequest request, RequestOptions options,
                                       ActionListener<RemoteInfoResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, ClusterRequestConverters::remoteInfo, options,
                RemoteInfoResponse::fromXContent, listener, singleton(RestStatus.REQUEST_TIMEOUT.getStatus()));
    }
}
