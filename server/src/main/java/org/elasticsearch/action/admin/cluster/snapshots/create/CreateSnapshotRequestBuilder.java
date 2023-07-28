/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.create;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.core.Nullable;

import java.util.Map;

/**
 * Create snapshot request builder
 */
public class CreateSnapshotRequestBuilder extends MasterNodeOperationRequestBuilder<
    CreateSnapshotRequest,
    CreateSnapshotResponse,
    CreateSnapshotRequestBuilder> {

    /**
     * Constructs a new create snapshot request builder with specified repository and snapshot names
     */
    public CreateSnapshotRequestBuilder(ElasticsearchClient client, CreateSnapshotAction action, String repository, String snapshot) {
        super(client, action, new CreateSnapshotRequest(repository, snapshot));
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

    /**
     * Provide a list of features whose state indices should be included in the snapshot
     *
     * @param featureStates A list of feature names
     * @return this builder
     */
    public CreateSnapshotRequestBuilder setFeatureStates(String... featureStates) {
        request.featureStates(featureStates);
        return this;
    }

    /**
     * Provide a map of user metadata that should be included in the snapshot metadata.
     *
     * @param metadata user metadata map
     * @return this builder
     */
    public CreateSnapshotRequestBuilder setUserMetadata(@Nullable Map<String, Object> metadata) {
        request.userMetadata(metadata);
        return this;
    }
}
