/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MIGRATION_FRAMEWORK;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_ROLES_METADATA_FLATTENED;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_ROLE_MAPPING_CLEANUP;

public class SecurityFeatures implements FeatureSpecification {

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(SECURITY_ROLE_MAPPING_CLEANUP, SECURITY_ROLES_METADATA_FLATTENED, SECURITY_MIGRATION_FRAMEWORK);
    }
}
