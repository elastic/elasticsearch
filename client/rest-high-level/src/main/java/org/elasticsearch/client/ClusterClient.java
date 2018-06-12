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
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;

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
        return restHighLevelClient.performRequestAndParseEntity(clusterUpdateSettingsRequest, RequestConverters::clusterPutSettings,
                options, ClusterUpdateSettingsResponse::fromXContent, emptySet());
    }

    /**
     * Updates cluster wide specific settings using the Cluster Update Settings API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-update-settings.html"> Cluster Update Settings
     * API on elastic.co</a>
     * @deprecated Prefer {@link #putSettings(ClusterUpdateSettingsRequest, RequestOptions)}
     */
    @Deprecated
    public ClusterUpdateSettingsResponse putSettings(ClusterUpdateSettingsRequest clusterUpdateSettingsRequest, Header... headers)
            throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(clusterUpdateSettingsRequest, RequestConverters::clusterPutSettings,
                ClusterUpdateSettingsResponse::fromXContent, emptySet(), headers);
    }

    /**
     * Asynchronously updates cluster wide specific settings using the Cluster Update Settings API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-update-settings.html"> Cluster Update Settings
     * API on elastic.co</a>
     * @param clusterUpdateSettingsRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void putSettingsAsync(ClusterUpdateSettingsRequest clusterUpdateSettingsRequest, RequestOptions options,
                                 ActionListener<ClusterUpdateSettingsResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(clusterUpdateSettingsRequest, RequestConverters::clusterPutSettings,
                options, ClusterUpdateSettingsResponse::fromXContent, listener, emptySet());
    }
    /**
     * Asynchronously updates cluster wide specific settings using the Cluster Update Settings API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-update-settings.html"> Cluster Update Settings
     * API on elastic.co</a>
     * @deprecated Prefer {@link #putSettingsAsync(ClusterUpdateSettingsRequest, RequestOptions, ActionListener)}
     */
    @Deprecated
    public void putSettingsAsync(ClusterUpdateSettingsRequest clusterUpdateSettingsRequest,
            ActionListener<ClusterUpdateSettingsResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(clusterUpdateSettingsRequest, RequestConverters::clusterPutSettings,
                ClusterUpdateSettingsResponse::fromXContent, listener, emptySet(), headers);
    }
}
