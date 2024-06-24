/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.Version;
import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.VERSION_SECURITY_PROFILE_ORIGIN;

public class SecurityFeatures implements FeatureSpecification {

    public static final NodeFeature SECURITY_PROFILE_ORIGIN_FEATURE = new NodeFeature("security.security_profile_origin");
    public static final NodeFeature SECURITY_MIGRATION_FRAMEWORK = new NodeFeature("security.migration_framework");
    public static final NodeFeature SECURITY_ROLES_METADATA_FLATTENED = new NodeFeature("security.roles_metadata_flattened");
    public static final NodeFeature SECURITY_ROLES_INDEX_NAME = new NodeFeature("security.roles_index_name");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(SECURITY_ROLES_METADATA_FLATTENED, SECURITY_MIGRATION_FRAMEWORK, SECURITY_ROLES_INDEX_NAME);
    }

    @Override
    public Map<NodeFeature, Version> getHistoricalFeatures() {
        return Map.of(SECURITY_PROFILE_ORIGIN_FEATURE, VERSION_SECURITY_PROFILE_ORIGIN);
    }
}
