/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.xpack.core.security.support.StringMatcher;

import java.util.Set;

// TODO RemoteIndicesPermissions to wrap List<RemoteIndicesPermission>?
public record RemoteIndicesPermission(
    Set<String> remoteClusterAliases,
    StringMatcher remoteClusterAliasMatcher,
    IndicesPermission indicesPermission
) {

    public RemoteIndicesPermission(Set<String> remoteClusterAliases, IndicesPermission indicesPermission) {
        this(remoteClusterAliases, StringMatcher.of(remoteClusterAliases), indicesPermission);
    }

    public boolean checkRemoteClusterAlias(final String remoteClusterAlias) {
        return remoteClusterAliasMatcher.test(remoteClusterAlias);
    }
}
