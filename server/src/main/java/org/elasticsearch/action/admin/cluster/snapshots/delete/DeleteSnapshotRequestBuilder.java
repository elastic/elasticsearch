/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.delete;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.core.TimeValue;

/**
 * Delete snapshot request builder
 */
public class DeleteSnapshotRequestBuilder extends MasterNodeOperationRequestBuilder<
    DeleteSnapshotRequest,
    AcknowledgedResponse,
    DeleteSnapshotRequestBuilder> {

    /**
     * Constructs delete snapshot request builder with specified repository and snapshot names
     */
    public DeleteSnapshotRequestBuilder(ElasticsearchClient client, TimeValue masterNodeTimeout, String repository, String... snapshots) {
        super(client, TransportDeleteSnapshotAction.TYPE, new DeleteSnapshotRequest(masterNodeTimeout, repository, snapshots));
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
     * @param snapshots snapshot names
     * @return this builder
     */
    public DeleteSnapshotRequestBuilder setSnapshots(String... snapshots) {
        request.snapshots(snapshots);
        return this;
    }

    /**
     * Sets whether to wait for completion
     *
     * @param waitForCompletion true to wait for completion, false otherwise
     * @return this builder
     */
    public DeleteSnapshotRequestBuilder setWaitForCompletion(boolean waitForCompletion) {
        request.waitForCompletion(waitForCompletion);
        return this;
    }
}
