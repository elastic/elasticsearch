/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.readonly;

import org.elasticsearch.cluster.ack.IndicesClusterStateUpdateRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock;

/**
 * Cluster state update request that allows to add a block to one or more indices
 */
public class AddIndexBlockClusterStateUpdateRequest extends IndicesClusterStateUpdateRequest<AddIndexBlockClusterStateUpdateRequest> {

    private final APIBlock block;
    private long taskId;

    public AddIndexBlockClusterStateUpdateRequest(final APIBlock block, final long taskId) {
        this.block = block;
        this.taskId = taskId;
    }

    public long taskId() {
        return taskId;
    }

    public APIBlock getBlock() {
        return block;
    }

    public AddIndexBlockClusterStateUpdateRequest taskId(final long taskId) {
        this.taskId = taskId;
        return this;
    }
}
