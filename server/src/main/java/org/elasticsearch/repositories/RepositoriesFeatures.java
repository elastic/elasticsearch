/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

/**
 * Provides features for repositories functionality.
 */
public class RepositoriesFeatures implements FeatureSpecification {

    /**
     * Feature for the default repository cluster setting.
     * Introduced in 9.2.0 to allow setting a default repository for snapshot and restore operations.
     */
    public static final NodeFeature DEFAULT_REPOSITORY_SETTING = new NodeFeature("repositories.default_repository_setting");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(DEFAULT_REPOSITORY_SETTING);
    }
}
