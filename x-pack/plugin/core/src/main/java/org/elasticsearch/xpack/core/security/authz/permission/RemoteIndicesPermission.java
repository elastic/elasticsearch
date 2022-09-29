/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.xpack.core.security.support.StringMatcher;

public record RemoteIndicesPermission(StringMatcher targetClusterMatcher, IndicesPermission indicesPermission) {

    public RemoteIndicesPermission(String[] targetClusters, IndicesPermission indicesPermission) {
        this(StringMatcher.of(targetClusters), indicesPermission);
    }

    public boolean checkTargetCluster(final String targetCluster) {
        return targetClusterMatcher.test(targetCluster);
    }
}
