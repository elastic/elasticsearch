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

import static org.elasticsearch.xpack.security.support.QueryableBuiltInRolesSynchronizer.QUERYABLE_BUILT_IN_ROLES_FEATURE;

public class SecurityFeatures implements FeatureSpecification {
    public static final NodeFeature CERTIFICATE_IDENTITY_FIELD_FEATURE = new NodeFeature("certificate_identity_field");

    public static final NodeFeature SECURITY_STATS_ENDPOINT = new NodeFeature("security_stats_endpoint");

    /**
     * Gates creating user-managed service accounts. A node that does not publish this feature still
     * resolves a service account's privileges from the built-in account of the same name and fails
     * the request when there is none, so accounts must not be created until every node understands
     * them. Deleting is deliberately not gated: it is the remedy for a cluster that already holds
     * such an account.
     */
    public static final NodeFeature USER_MANAGED_SERVICE_ACCOUNTS = new NodeFeature("user_managed_service_accounts");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(
            QUERYABLE_BUILT_IN_ROLES_FEATURE,
            CERTIFICATE_IDENTITY_FIELD_FEATURE,
            SECURITY_STATS_ENDPOINT,
            USER_MANAGED_SERVICE_ACCOUNTS
        );
    }
}
