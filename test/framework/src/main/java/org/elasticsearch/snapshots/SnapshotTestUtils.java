/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.NodeShutdownTestUtils;

/**
 * @deprecated Use {@link NodeShutdownTestUtils} directly. This class exists only for backward compatibility
 * and will be removed in a subsequent PR.
 */
@Deprecated
public class SnapshotTestUtils {

    public static void putShutdownForRemovalMetadata(String nodeName, ClusterService clusterService) {
        NodeShutdownTestUtils.putShutdownForRemovalMetadata(nodeName, clusterService);
    }

    public static void putShutdownForRemovalMetadata(ClusterService clusterService, String nodeName, ActionListener<Void> listener) {
        NodeShutdownTestUtils.putShutdownForRemovalMetadata(clusterService, nodeName, listener);
    }

    public static void putShutdownMetadata(
        ClusterService clusterService,
        SingleNodeShutdownMetadata.Builder shutdownMetadataBuilder,
        String nodeName,
        ActionListener<Void> listener
    ) {
        NodeShutdownTestUtils.putShutdownMetadata(clusterService, shutdownMetadataBuilder, nodeName, listener);
    }

    public static void flushMasterQueue(ClusterService clusterService, ActionListener<Void> listener) {
        NodeShutdownTestUtils.flushMasterQueue(clusterService, listener);
    }

    public static void clearShutdownMetadata(ClusterService clusterService) {
        NodeShutdownTestUtils.clearShutdownMetadata(clusterService);
    }
}
