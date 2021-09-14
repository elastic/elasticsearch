/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.ack;

import org.elasticsearch.index.Index;

/**
 * Base cluster state update request that allows to execute update against multiple indices
 */
public abstract class IndicesClusterStateUpdateRequest<T extends IndicesClusterStateUpdateRequest<T>> extends ClusterStateUpdateRequest<T> {

    private Index[] indices;

    /**
     * Returns the indices the operation needs to be executed on
     */
    public Index[] indices() {
        return indices;
    }

    /**
     * Sets the indices the operation needs to be executed on
     */
    @SuppressWarnings("unchecked")
    public T indices(Index[] indices) {
        this.indices = indices;
        return (T)this;
    }
}
