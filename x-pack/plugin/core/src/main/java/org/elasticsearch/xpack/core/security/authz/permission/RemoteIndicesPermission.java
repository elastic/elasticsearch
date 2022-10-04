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
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.security.support.StringMatcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public record RemoteIndicesPermission(List<RemoteIndicesGroup> remoteIndicesGroups) {

    public static final RemoteIndicesPermission NONE = new RemoteIndicesPermission(List.of());

    // TODO should this return RemoteIndicesPermission?
    public IndicesPermission indicesPermissionFor(final String remoteClusterAlias) {
        final var builder = new IndicesPermission.Builder(new RestrictedIndices(Automatons.EMPTY));
        for (var remoteIndicesGroup : remoteIndicesGroups) {
            if (false == remoteIndicesGroup.checkRemoteClusterAlias(remoteClusterAlias)) {
                continue;
            }
            // TODO we can merge groups by `indices` here
            final var group = remoteIndicesGroup.indicesPermissionGroup();
            builder.addGroup(
                group.privilege(),
                group.getFieldPermissions(),
                group.getQuery(),
                group.allowRestrictedIndices(),
                group.indices()
            );
        }
        // TODO cache result
        return builder.build();
    }

    public static class Builder {
        final List<RemoteIndicesGroup> remoteIndicesGroups;

        public Builder() {
            this.remoteIndicesGroups = new ArrayList<>();
        }

        public Builder addRemoteIndicesGroup(
            final Set<String> remoteClusterAliases,
            final IndexPrivilege privilege,
            final FieldPermissions fieldPermissions,
            final @Nullable Set<BytesReference> query,
            final boolean allowRestrictedIndices,
            final String... indices
        ) {
            remoteIndicesGroups.add(
                new RemoteIndicesGroup(
                    remoteClusterAliases,
                    new IndicesPermission.Group(
                        privilege,
                        fieldPermissions,
                        query,
                        allowRestrictedIndices,
                        new RestrictedIndices(Automatons.EMPTY),
                        indices
                    )
                )
            );
            return this;
        }

        public RemoteIndicesPermission build() {
            return new RemoteIndicesPermission(Collections.unmodifiableList(remoteIndicesGroups));
        }
    }

    public record RemoteIndicesGroup(
        Set<String> remoteClusterAliases,
        StringMatcher remoteClusterAliasMatcher,
        // List<>?
        IndicesPermission.Group indicesPermissionGroup
    ) {

        public RemoteIndicesGroup(Set<String> remoteClusterAliases, IndicesPermission.Group indicesPermissionGroup) {
            this(remoteClusterAliases, StringMatcher.of(remoteClusterAliases), indicesPermissionGroup);
        }

        public boolean checkRemoteClusterAlias(final String remoteClusterAlias) {
            return remoteClusterAliasMatcher.test(remoteClusterAlias);
        }
    }
}
