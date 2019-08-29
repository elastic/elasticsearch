/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;

/**
 * This interface represents a privilege that is used to control access to cluster level actions.
 */
public interface ClusterPrivilege {
    /**
     * Uses {@link ClusterPermission.Builder} to add predicate that later can be used to build a {@link ClusterPermission}.
     * @param builder {@link ClusterPermission.Builder}
     * @return an instance of {@link ClusterPermission.Builder}
     */
    ClusterPermission.Builder buildPermission(ClusterPermission.Builder builder);
}
