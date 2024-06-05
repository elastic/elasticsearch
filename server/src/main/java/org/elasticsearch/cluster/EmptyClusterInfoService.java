/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import java.util.function.Consumer;

/**
 * {@link ClusterInfoService} that provides empty maps for disk usage and shard sizes
 */
public class EmptyClusterInfoService implements ClusterInfoService {
    public static final EmptyClusterInfoService INSTANCE = new EmptyClusterInfoService();

    @Override
    public ClusterInfo getClusterInfo() {
        return ClusterInfo.EMPTY;
    }

    @Override
    public void addListener(Consumer<ClusterInfo> clusterInfoConsumer) {
        // never updated, so we can discard the listener
    }
}
