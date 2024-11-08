/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

import static org.elasticsearch.repositories.blobstore.testkit.integrity.RestRepositoryVerifyIntegrityAction.REPOSITORY_VERIFY_INTEGRITY_FEATURE;

public class SnapshotRepositoryTestKitFeatures implements FeatureSpecification {
    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(REPOSITORY_VERIFY_INTEGRITY_FEATURE);
    }
}
