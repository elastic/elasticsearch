/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authz.RestrictedIndices;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.support.StringMatcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public record RemoteIndicesPermission(
    RestrictedIndices restrictedIndices,
    List<IndicesPermissionWithRemoteClusterAlias> indicesPermissions
) {

    public IndicesPermission indicesPermissionFor(final String remoteClusterAlias) {
        final var builder = new IndicesPermission.Builder(restrictedIndices);
        for (var permissionWithRemoteClusterAlias : indicesPermissions) {
            if (false == permissionWithRemoteClusterAlias.checkRemoteClusterAlias(remoteClusterAlias)) {
                continue;
            }
            // TODO we can merge groups by `indices` here
            for (var group : permissionWithRemoteClusterAlias.indicesPermission().groups()) {
                builder.addGroup(
                    group.privilege(),
                    group.getFieldPermissions(),
                    group.getQuery(),
                    group.allowRestrictedIndices(),
                    group.indices()
                );
            }
        }
        // TODO cache result
        return builder.build();
    }

    public static class Builder {
        final List<IndicesPermissionWithRemoteClusterAlias> indicesPermissions;

        final RestrictedIndices restrictedIndices;

        public Builder(RestrictedIndices restrictedIndices) {
            this.restrictedIndices = restrictedIndices;
            this.indicesPermissions = new ArrayList<>();
        }

        public Builder addIndicesPermission(
            final Set<String> remoteClusterAliases,
            final IndexPrivilege privilege,
            final FieldPermissions fieldPermissions,
            final @Nullable Set<BytesReference> query,
            final boolean allowRestrictedIndices,
            final String... indices
        ) {
            final IndicesPermission.Builder indicesBuilder = new IndicesPermission.Builder(restrictedIndices);
            indicesBuilder.addGroup(privilege, fieldPermissions, query, allowRestrictedIndices, indices);
            indicesPermissions.add(new IndicesPermissionWithRemoteClusterAlias(remoteClusterAliases, indicesBuilder.build()));
            return this;
        }

        public RemoteIndicesPermission build() {
            return new RemoteIndicesPermission(restrictedIndices, Collections.unmodifiableList(indicesPermissions));
        }
    }

    public record IndicesPermissionWithRemoteClusterAlias(
        Set<String> remoteClusterAliases,
        StringMatcher remoteClusterAliasMatcher,
        IndicesPermission indicesPermission
    ) {

        public IndicesPermissionWithRemoteClusterAlias(Set<String> remoteClusterAliases, IndicesPermission indicesPermission) {
            this(remoteClusterAliases, StringMatcher.of(remoteClusterAliases), indicesPermission);
        }

        public boolean checkRemoteClusterAlias(final String remoteClusterAlias) {
            return remoteClusterAliasMatcher.test(remoteClusterAlias);
        }
    }
}
