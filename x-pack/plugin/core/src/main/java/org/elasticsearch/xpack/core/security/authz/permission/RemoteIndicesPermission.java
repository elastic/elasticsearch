/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.xpack.core.security.support.StringMatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public record RemoteIndicesPermission(List<RemoteIndicesGroup> remoteIndicesGroups) {

    public static final RemoteIndicesPermission NONE = new RemoteIndicesPermission(List.of());

    public RemoteIndicesPermission forCluster(final String remoteClusterAlias) {
        // TODO cache result
        return new RemoteIndicesPermission(
            remoteIndicesGroups.stream()
                // TODO we can merge `indicesPermissionGroups` by `indices` here
                .filter(remoteIndicesGroup -> remoteIndicesGroup.checkRemoteClusterAlias(remoteClusterAlias))
                .toList()
        );
    }

    public static class Builder {
        final List<RemoteIndicesGroup> remoteIndicesGroups;

        public Builder() {
            this.remoteIndicesGroups = new ArrayList<>();
        }

        public Builder addGroup(final Set<String> remoteClusterAliases, final List<IndicesPermission.Group> groups) {
            remoteIndicesGroups.add(new RemoteIndicesGroup(remoteClusterAliases, groups));
            return this;
        }

        public RemoteIndicesPermission build() {
            return remoteIndicesGroups.isEmpty() ? NONE : new RemoteIndicesPermission(remoteIndicesGroups);
        }
    }

    public record RemoteIndicesGroup(
        Set<String> remoteClusterAliases,
        StringMatcher remoteClusterAliasMatcher,
        List<IndicesPermission.Group> indicesPermissionGroups
    ) {
        public RemoteIndicesGroup(Set<String> remoteClusterAliases, List<IndicesPermission.Group> indicesPermissionGroups) {
            this(remoteClusterAliases, StringMatcher.of(remoteClusterAliases), indicesPermissionGroups);
        }

        public boolean checkRemoteClusterAlias(final String remoteClusterAlias) {
            return remoteClusterAliasMatcher.test(remoteClusterAlias);
        }
    }
}
