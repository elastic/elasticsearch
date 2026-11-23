/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

/**
 * {@link FeatureSpecification} that publishes the project encryption key node features.
 */
public class EncryptionFeatures implements FeatureSpecification {

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(
            ProjectEncryptionKeyService.PROJECT_ENCRYPTION_KEY_FEATURE,
            ProjectEncryptionKeyService.CAN_PERSIST_KEY_CHECK_FEATURE
        );
    }
}
