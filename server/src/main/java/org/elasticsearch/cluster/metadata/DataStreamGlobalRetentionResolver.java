/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.core.Nullable;

/**
 * Resolves the global retention configuration for data stream lifecycle taking into consideration the
 * metadata in the cluster state and the factory settings.
 * Currently, we give precedence to the configuration in the metadata and fallback to the factory settings when it's not present.
 */
public class DataStreamGlobalRetentionResolver {

    private final DataStreamFactoryRetention factoryRetention;

    public DataStreamGlobalRetentionResolver(DataStreamFactoryRetention factoryRetention) {
        this.factoryRetention = factoryRetention;
    }

    /**
     * Return the global retention configuration as found in the metadata. If the metadata is null, then it falls back
     * to the factory settings, if both default and max factory settings as null, it returns null.
     */
    @Nullable
    public DataStreamGlobalRetention resolve(ClusterState clusterState) {
        DataStreamGlobalRetention globalRetentionFromClusterState = clusterState.custom(DataStreamGlobalRetention.TYPE);
        if (factoryRetention.getDefaultRetention() == null && factoryRetention.getMaxRetention() == null) {
            return globalRetentionFromClusterState;
        }
        if (globalRetentionFromClusterState == null) {
            return new DataStreamGlobalRetention(factoryRetention.getDefaultRetention(), factoryRetention.getMaxRetention());
        }
        return globalRetentionFromClusterState;
    }

}
