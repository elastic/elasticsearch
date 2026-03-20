/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.features.NodeFeature;

/**
 * Node feature identifiers for reindex task management, visible within the server module code.
 */
public final class ReindexTaskManagementFeatures {

    /**
     * The reindex module already declares a {@code #RELOCATE_ON_SHUTDOWN_NODE_FEATURE} .
     * However, since server cannot depend on modules/reindex, we're forced to duplicate the feature flag here using the same ID.
     * The actual features are registered for the node via {@code org.elasticsearch.reindex.management.ReindexManagementFeatures}
     * when the corresponding feature flags are enabled.
     */
    public static final NodeFeature RELOCATE_ON_SHUTDOWN_NODE_FEATURE = new NodeFeature("reindex_pit_search");

    private ReindexTaskManagementFeatures() {}
}
