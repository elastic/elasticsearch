/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.reindex.ReindexPlugin;

import java.util.Set;

public class ReindexManagementFeatures implements FeatureSpecification {

    public static final NodeFeature NEW_ENDPOINTS = new NodeFeature("reindex_management_endpoints");

    @Override
    public Set<NodeFeature> getFeatures() {
        return ReindexPlugin.REINDEX_RESILIENCE_ENABLED ? Set.of(NEW_ENDPOINTS) : Set.of();
    }
}
