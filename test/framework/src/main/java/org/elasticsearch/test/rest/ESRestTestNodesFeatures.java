/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest;

public interface ESRestTestNodesFeatures {
    boolean supportsSearchableSnapshotsIndices();

    boolean supportsComposableIndexTemplates();

    boolean supportsBulkDeleteOnTemplates();

    boolean deprecatesSoftDeleteDisabled();

    boolean enforcesSoftDeleteEnabled();

    boolean deprecatesSystemIndicesAccess();

    boolean supportsReplicationOfClosedIndices();

    boolean supportsFeatureStateReset();

    boolean enforcesMlResetEnabled();

    boolean supportsNodeShutdownApi();

    boolean supportsOperationsOnHiddenIndices();

    boolean enforcesPeerRecoveryRetentionLeases();

    boolean enforcesAllocationFilteringRulesOnIndicesAutoExpand();
}
