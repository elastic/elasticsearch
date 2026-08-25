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

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(RECOVERY_APIS_INCLUDE_PRIORITY_NODE_FEATURE);
    }
}
