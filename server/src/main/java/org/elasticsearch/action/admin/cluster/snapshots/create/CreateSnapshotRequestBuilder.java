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

package org.elasticsearch.action.admin.cluster.snapshots.create;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Create snapshot request builder
 */
public class CreateSnapshotRequestBuilder extends MasterNodeOperationRequestBuilder<CreateSnapshotRequest,
        CreateSnapshotResponse, CreateSnapshotRequestBuilder> {

    /**
     * Constructs a new create snapshot request builder
     */
    public CreateSnapshotRequestBuilder(ElasticsearchClient client, CreateSnapshotAction action) {
        super(client, action, new CreateSnapshotRequest());
    }

    /**
     * Constructs a new create snapshot request builder with specified repository and snapshot names
     */
    public CreateSnapshotRequestBuilder(ElasticsearchClient client, CreateSnapshotAction action, String repository, String snapshot) {
        super(client, action, new CreateSnapshotRequest(repository, snapshot));
    }

    /**
     * Sets the snapshot name
     *
     * @param snapshot snapshot name
     * @return this builder
     */
    public CreateSnapshotRequestBuilder setSnapshot(String snapshot) {
        request.snapshot(snapshot);
        return this;
    }

    /**
     * Sets the repository name
     *
     * @param repository repository name
     * @return this builder
     */
    public CreateSnapshotRequestBuilder setRepository(String repository) {
        request.repository(repository);
        return this;
    }

    /**
     * Sets a list of indices that should be included into the snapshot
     * <p>
     * The list of indices supports multi-index syntax. For example: "+test*" ,"-test42" will index all indices with
     * prefix "test" except index "test42". Aliases are supported. An empty list or {"_all"} will snapshot all open
     * indices in the cluster.
     *
     * @return this builder
     */
    public CreateSnapshotRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Specifies the indices options. Like what type of requested indices to ignore. For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices options
     * @return this request
     */
    public CreateSnapshotRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }

    /**
     * If set to true the request should wait for the snapshot completion before returning.
     *
     * @param waitForCompletion true if
     * @return this builder
     */
    public CreateSnapshotRequestBuilder setWaitForCompletion(boolean waitForCompletion) {
        request.waitForCompletion(waitForCompletion);
        return this;
    }

    /**
     * If set to true the request should snapshot indices with unavailable shards
     *
     * @param partial true if request should snapshot indices with unavailable shards
     * @return this builder
     */
    public CreateSnapshotRequestBuilder setPartial(boolean partial) {
        request.partial(partial);
        return this;
    }

    /**
     * Set to true if snapshot should include global cluster state
     *
     * @param includeGlobalState true if snapshot should include global cluster state
     * @return this builder
     */
    public CreateSnapshotRequestBuilder setIncludeGlobalState(boolean includeGlobalState) {
        request.includeGlobalState(includeGlobalState);
        return this;
    }
}
