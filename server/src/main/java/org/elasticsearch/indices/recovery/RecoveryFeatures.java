/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

/// Specification for node features related to recovery.
public class RecoveryFeatures implements FeatureSpecification {

    /// Test-only node feature indicating that the `GET /_recovery` and `GET /_cat/recovery` APIs include the recovery priority in their
    /// responses.
    private static final NodeFeature RECOVERY_APIS_INCLUDE_PRIORITY_NODE_FEATURE = new NodeFeature("indices.recovery.recovery_priority");

    /// Node feature indicating that
    /// [org.elasticsearch.indices.cluster.IndicesClusterStateService] recreates the index service, rather than updating it in place, when a
    /// restore is initialized over an index that is already open. Such a transition keeps the index open and keeps its index UUID but
    /// assigns a new history UUID, which a node without this feature cannot apply.
    ///
    /// A master must not publish an in-place restore over an open index until every relevant data node supports this feature.
    public static final NodeFeature RESTORE_OVER_OPEN_INDEX_RECREATES_INDEX_SERVICE = new NodeFeature(
        "indices.recovery.restore_over_open_index_recreates_index_service"
    );

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(RESTORE_OVER_OPEN_INDEX_RECREATES_INDEX_SERVICE);
    }

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(RECOVERY_APIS_INCLUDE_PRIORITY_NODE_FEATURE);
    }
}
