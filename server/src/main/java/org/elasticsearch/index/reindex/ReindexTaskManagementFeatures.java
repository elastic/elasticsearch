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
 * {@link NodeFeature} identifiers for reindex task management that are registered by the reindex-management module
 * (see {@code org.elasticsearch.reindex.management.ReindexManagementFeatures} in {@code modules/reindex-management}).
 * Duplicated here so server REST handlers can gate deprecation without a compile dependency on that module.
 * <p>
 * <strong>Important:</strong> the {@link NodeFeature#id()} strings must stay aligned with the registering
 * {@link org.elasticsearch.features.FeatureSpecification}.
 */
public final class ReindexTaskManagementFeatures {

    private ReindexTaskManagementFeatures() {}

    /**
     * Matches {@code ReindexManagementFeatures.NEW_ENDPOINTS} in the reindex-management module.
     */
    public static final NodeFeature REINDEX_MANAGEMENT_ENDPOINTS = new NodeFeature("reindex_management_endpoints");
}
