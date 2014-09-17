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

package org.elasticsearch.action.admin.cluster.snapshots.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ClusterAdminClient;

/**
 * Delete snapshot request builder
 */
public class DeleteSnapshotRequestBuilder extends MasterNodeOperationRequestBuilder<DeleteSnapshotRequest, DeleteSnapshotResponse, DeleteSnapshotRequestBuilder, ClusterAdminClient> {

    /**
     * Constructs delete snapshot request builder
     *
     * @param clusterAdminClient cluster admin client
     */
    public DeleteSnapshotRequestBuilder(ClusterAdminClient clusterAdminClient) {
        super(clusterAdminClient, new DeleteSnapshotRequest());
    }

    /**
     * Constructs delete snapshot request builder with specified repository and snapshot names
     *
     * @param clusterAdminClient cluster admin client
     * @param repository         repository name
     * @param snapshot           snapshot name
     */
    public DeleteSnapshotRequestBuilder(ClusterAdminClient clusterAdminClient, String repository, String snapshot) {
        super(clusterAdminClient, new DeleteSnapshotRequest(repository, snapshot));
    }

    /**
     * Sets the repository name
     *
     * @param repository repository name
     * @return this builder
     */
    public DeleteSnapshotRequestBuilder setRepository(String repository) {
        request.repository(repository);
        return this;
    }

    /**
     * Sets the snapshot name
     *
     * @param snapshot snapshot name
     * @return this builder
     */
    public DeleteSnapshotRequestBuilder setSnapshot(String snapshot) {
        request.snapshot(snapshot);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<DeleteSnapshotResponse> listener) {
        client.deleteSnapshot(request, listener);
    }
}
