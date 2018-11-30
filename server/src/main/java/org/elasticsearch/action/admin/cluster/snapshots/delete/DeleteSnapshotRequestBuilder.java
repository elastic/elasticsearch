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

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Delete snapshot request builder
 */
public class DeleteSnapshotRequestBuilder extends MasterNodeOperationRequestBuilder<DeleteSnapshotRequest,
        AcknowledgedResponse, DeleteSnapshotRequestBuilder> {

    /**
     * Constructs delete snapshot request builder
     */
    public DeleteSnapshotRequestBuilder(ElasticsearchClient client, DeleteSnapshotAction action) {
        super(client, action, new DeleteSnapshotRequest());
    }

    /**
     * Constructs delete snapshot request builder with specified repository and snapshot names
     */
    public DeleteSnapshotRequestBuilder(ElasticsearchClient client, DeleteSnapshotAction action, String repository, String snapshot) {
        super(client, action, new DeleteSnapshotRequest(repository, snapshot));
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
}
