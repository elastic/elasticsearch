/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.delete;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

/**
 * Delete snapshot request builder
 */
public class DeleteSnapshotRequestBuilder extends MasterNodeOperationRequestBuilder<
    DeleteSnapshotRequest,
    AcknowledgedResponse,
    DeleteSnapshotRequestBuilder> {

    /**
     * Constructs delete snapshot request builder
     */
    public DeleteSnapshotRequestBuilder(ElasticsearchClient client, DeleteSnapshotAction action) {
        super(client, action, new DeleteSnapshotRequest());
    }

    /**
     * Constructs delete snapshot request builder with specified repository and snapshot names
     */
    public DeleteSnapshotRequestBuilder(ElasticsearchClient client, DeleteSnapshotAction action, String repository, String... snapshots) {
        super(client, action, new DeleteSnapshotRequest(repository, snapshots));
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
}
