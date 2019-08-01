/*
 *
 *  Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 *  or more contributor license agreements. Licensed under the Elastic License;
 *  you may not use this file except in compliance with the Elastic License.
 *
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
    private final Set<String> excludeActionPatterns;

    public ActionClusterPrivilege(final String name, final Set<String> allowedActionPatterns) {
        this(name, allowedActionPatterns, Set.of());
    }

    public ActionClusterPrivilege(final String name, final Set<String> allowedActionPatterns, final Set<String> excludeActionPatterns) {
        this.name = name;
        this.allowedActionPatterns = allowedActionPatterns;
        this.excludeActionPatterns = excludeActionPatterns;
    }

    @Override
    public String name() {
        return name;
    }

    public Set<String> getAllowedActionPatterns() {
        return allowedActionPatterns;
    }

    public Set<String> getExcludedActionPatterns() {
        return excludeActionPatterns;
    }

    @Override
    public ClusterPermission.Builder buildPermission(final ClusterPermission.Builder builder) {
        return builder.add(this, allowedActionPatterns, excludeActionPatterns);
    }

}
