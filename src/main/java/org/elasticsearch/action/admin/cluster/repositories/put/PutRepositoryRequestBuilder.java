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

package org.elasticsearch.action.admin.cluster.repositories.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

/**
 * Register repository request builder
 */
public class PutRepositoryRequestBuilder extends AcknowledgedRequestBuilder<PutRepositoryRequest, PutRepositoryResponse, PutRepositoryRequestBuilder, ClusterAdminClient> {

    /**
     * Constructs register repository request
     *
     * @param clusterAdminClient cluster admin client
     */
    public PutRepositoryRequestBuilder(ClusterAdminClient clusterAdminClient) {
        super(clusterAdminClient, new PutRepositoryRequest());
    }

    /**
     * Constructs register repository request for the repository with a given name
     *
     * @param clusterAdminClient cluster admin client
     * @param name               repository name
     */
    public PutRepositoryRequestBuilder(ClusterAdminClient clusterAdminClient, String name) {
        super(clusterAdminClient, new PutRepositoryRequest(name));
    }

    /**
     * Sets the repository name
     *
     * @param name repository name
     * @return this builder
     */
    public PutRepositoryRequestBuilder setName(String name) {
        request.name(name);
        return this;
    }

    /**
     * Sets the repository type
     *
     * @param type repository type
     * @return this builder
     */
    public PutRepositoryRequestBuilder setType(String type) {
        request.type(type);
        return this;
    }

    /**
     * Sets the repository settings
     *
     * @param settings repository settings
     * @return this builder
     */
    public PutRepositoryRequestBuilder setSettings(Settings settings) {
        request.settings(settings);
        return this;
    }

    /**
     * Sets the repository settings
     *
     * @param settings repository settings builder
     * @return this builder
     */
    public PutRepositoryRequestBuilder setSettings(Settings.Builder settings) {
        request.settings(settings);
        return this;
    }

    /**
     * Sets the repository settings in Json, Yaml or properties format
     *
     * @param source repository settings
     * @return this builder
     */
    public PutRepositoryRequestBuilder setSettings(String source) {
        request.settings(source);
        return this;
    }

    /**
     * Sets the repository settings
     *
     * @param source repository settings
     * @return this builder
     */
    public PutRepositoryRequestBuilder setSettings(Map<String, Object> source) {
        request.settings(source);
        return this;
    }

    /**
     * Sets whether or not repository should be verified after creation
     *
     * @param verify true if repository should be verified after registration, false otherwise
     * @return this builder
     */
    public PutRepositoryRequestBuilder setVerify(boolean verify) {
        request.verify(verify);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<PutRepositoryResponse> listener) {
        client.putRepository(request, listener);
    }
}
