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

    /// Test-only node feature indicating that the `GET /_recovery` and `GET /_cat/recovery` APIs include the locally-retryable failure
    /// count in their responses.
    private static final NodeFeature RECOVERY_APIS_INCLUDE_LOCAL_RETRY_COUNT_NODE_FEATURE = new NodeFeature(
        "indices.recovery.recovery_local_retry_count"
    );

    /// Test-only node feature indicating that `GET /_cat/recovery` includes the blocking recovery gate and blocked duration.
    /// REST tests require this feature to skip clusters where not all nodes support these columns.
    /// For unblocked recoveries, the CAT columns contain `n/a`; `GET /_recovery` omits the corresponding fields.
    private static final NodeFeature CAT_RECOVERY_INCLUDES_GATE_NODE_FEATURE = new NodeFeature("indices.recovery.cat_recovery_gate");

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
        return Set.of(
            RECOVERY_APIS_INCLUDE_PRIORITY_NODE_FEATURE,
            RECOVERY_APIS_INCLUDE_LOCAL_RETRY_COUNT_NODE_FEATURE,
            CAT_RECOVERY_INCLUDES_GATE_NODE_FEATURE
        );
    }
}
