/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.clone;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.Strings;

public class CloneSnapshotRequestBuilder extends MasterNodeOperationRequestBuilder<
    CloneSnapshotRequest,
    AcknowledgedResponse,
    CloneSnapshotRequestBuilder> {

    protected CloneSnapshotRequestBuilder(
        ElasticsearchClient client,
        ActionType<AcknowledgedResponse> action,
        CloneSnapshotRequest request
    ) {
        super(client, action, request);
    }

    public CloneSnapshotRequestBuilder(
        ElasticsearchClient client,
        ActionType<AcknowledgedResponse> action,
        String repository,
        String source,
        String target
    ) {
        this(client, action, new CloneSnapshotRequest(repository, source, target, Strings.EMPTY_ARRAY));
    }

    /**
     * Sets a list of indices that should be cloned from the source to the target snapshot
     * <p>
     * The list of indices supports multi-index syntax. For example: "+test*" ,"-test42" will clone all indices with
     * prefix "test" except index "test42".
     *
     * @return this builder
     */
    public CloneSnapshotRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Specifies the indices options. Like what type of requested indices to ignore. For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices options
     * @return this request
     */
    public CloneSnapshotRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }
}
