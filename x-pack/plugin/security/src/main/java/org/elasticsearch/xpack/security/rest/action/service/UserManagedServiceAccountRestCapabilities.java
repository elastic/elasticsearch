/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.service;

import java.util.Set;

/** REST-handler capabilities for the user-managed service account APIs. */
public final class UserManagedServiceAccountRestCapabilities {

    /**
     * Signals that this node serves the user-managed service account APIs. It shares its name with the
     * {@code user_managed_service_accounts} node feature deliberately, but answers a stronger question: the feature
     * says every node understands such accounts, while this says the cluster is also configured to hold them. A
     * multi-project cluster publishes the feature and does not report this capability.
     */
    public static final String USER_MANAGED_SERVICE_ACCOUNTS = "user_managed_service_accounts";

    private static final Set<String> AVAILABLE = Set.of(USER_MANAGED_SERVICE_ACCOUNTS);

    private UserManagedServiceAccountRestCapabilities() {}

    public static Set<String> supportedCapabilities(boolean userManagedServiceAccountsAvailable) {
        return userManagedServiceAccountsAvailable ? AVAILABLE : Set.of();
    }
}
