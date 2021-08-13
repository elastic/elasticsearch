/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;

import java.util.Set;

/**
 * A {@link NamedClusterPrivilege} that can be used to define an access to cluster level actions.
 */
public class ActionClusterPrivilege implements NamedClusterPrivilege {
    private final String name;
    private final Set<String> allowedActionPatterns;
    private final Set<String> excludedActionPatterns;
    private final ClusterPermission permission;

    /**
     * Constructor for {@link ActionClusterPrivilege} defining what cluster actions are accessible for the user with this privilege.
     *
     * @param name                  name for the cluster privilege
     * @param allowedActionPatterns a set of cluster action patterns that are allowed for the user with this privilege.
     */
    public ActionClusterPrivilege(final String name, final Set<String> allowedActionPatterns) {
        this(name, allowedActionPatterns, Set.of());
    }

    /**
     * Constructor for {@link ActionClusterPrivilege} that defines what cluster actions are accessible for the
     * user with this privilege after excluding the action patterns {@code excludedActionPatterns} from the allowed action patterns
     * {@code allowedActionPatterns}
     *
     * @param name                   name for the cluster privilege
     * @param allowedActionPatterns  a set of cluster action patterns
     * @param excludedActionPatterns a set of cluster action patterns
     */
    public ActionClusterPrivilege(final String name, final Set<String> allowedActionPatterns, final Set<String> excludedActionPatterns) {
        this.name = name;
        this.allowedActionPatterns = allowedActionPatterns;
        this.excludedActionPatterns = excludedActionPatterns;
        this.permission = buildPermission(ClusterPermission.builder()).build();
    }

    @Override
    public String name() {
        return name;
    }

    public Set<String> getAllowedActionPatterns() {
        return allowedActionPatterns;
    }

    public Set<String> getExcludedActionPatterns() {
        return excludedActionPatterns;
    }

    @Override
    public ClusterPermission.Builder buildPermission(final ClusterPermission.Builder builder) {
        return builder.add(this, allowedActionPatterns, excludedActionPatterns);
    }

    @Override
    public ClusterPermission permission() {
        return permission;
    }
}
