/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import java.util.Set;

/**
 * An {@link ActionClusterPrivilege} that defines a predefined, built-in cluster privilege, like "manage" or "monitor".
 */
final class BuiltinClusterPrivilege extends ActionClusterPrivilege {
    private final boolean supportedInServerlessMode;

    /**
     * Constructor for {@link BuiltinClusterPrivilege} defining what cluster actions are accessible for the user with this privilege.
     *
     * @param name                  name for the cluster privilege
     * @param allowedActionPatterns a set of cluster action patterns that are allowed for the user with this privilege.
     * @param supportedInServerlessMode whether this privilege is supported in serverless mode, i.e., whether it should be available to
     *                                  end-users
     */
    BuiltinClusterPrivilege(final String name, final Set<String> allowedActionPatterns, final boolean supportedInServerlessMode) {
        this(name, allowedActionPatterns, Set.of(), supportedInServerlessMode);
    }

    BuiltinClusterPrivilege(
        final String name,
        final Set<String> allowedActionPatterns,
        final Set<String> excludedActionPatterns,
        final boolean supportedInServerlessMode
    ) {
        super(name, allowedActionPatterns, excludedActionPatterns);
        this.supportedInServerlessMode = supportedInServerlessMode;
    }

    @Override
    public boolean isSupportedInServerlessMode() {
        return supportedInServerlessMode;
    }
}
