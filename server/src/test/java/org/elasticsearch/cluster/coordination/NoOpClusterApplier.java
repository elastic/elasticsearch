/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterApplier;

import java.util.function.Supplier;

public class NoOpClusterApplier implements ClusterApplier {
    @Override
    public void setInitialState(ClusterState initialState) {

    }

    @Override
    public void onNewClusterState(String source, Supplier<ClusterState> clusterStateSupplier, ClusterApplyListener listener) {
        listener.onSuccess(source);
    }
}
