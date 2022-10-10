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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        final Map<Set<String>, List<IndicesPermission.Group>> remoteIndicesGroups;

        public Builder() {
            this.remoteIndicesGroups = new HashMap<>();
        }

        public Builder addGroup(final Set<String> remoteClusterAliases, final String... indices) {
            return addGroup(remoteClusterAliases, IndexPrivilege.READ, FieldPermissions.DEFAULT, null, false, indices);
        }

        public Builder addGroup(final Set<String> remoteClusterAliases, final boolean allowRestrictedIndices, final String... indices) {
            return addGroup(remoteClusterAliases, IndexPrivilege.READ, FieldPermissions.DEFAULT, null, allowRestrictedIndices, indices);
        }

        public Builder addGroup(
            final Set<String> remoteClusterAliases,
            final IndexPrivilege privilege,
            final FieldPermissions fieldPermissions,
            final @Nullable Set<BytesReference> query,
            final boolean allowRestrictedIndices,
            final String... indices
        ) {
            remoteIndicesGroups.computeIfAbsent(remoteClusterAliases, k -> new ArrayList<>())
                .add(
                    new IndicesPermission.Group(
                        privilege,
                        fieldPermissions,
                        query,
                        allowRestrictedIndices,
                        new RestrictedIndices(Automatons.EMPTY),
                        indices
                    )
                );
            return this;
        }

        public RemoteIndicesPermission build() {
            return new RemoteIndicesPermission(
                remoteIndicesGroups.entrySet().stream().map(entry -> new RemoteIndicesGroup(entry.getKey(), entry.getValue())).toList()
            );
        }
    }

    public static final class RemoteIndicesGroup {
        private final Set<String> remoteClusterAliases;
        private final StringMatcher remoteClusterAliasMatcher;
        private final List<IndicesPermission.Group> indicesPermissionGroups;

        public RemoteIndicesGroup(
            Set<String> remoteClusterAliases,
            StringMatcher remoteClusterAliasMatcher,
            List<IndicesPermission.Group> indicesPermissionGroups
        ) {
            this.remoteClusterAliases = remoteClusterAliases;
            this.remoteClusterAliasMatcher = remoteClusterAliasMatcher;
            this.indicesPermissionGroups = indicesPermissionGroups;
        }

        public RemoteIndicesGroup(Set<String> remoteClusterAliases, List<IndicesPermission.Group> indicesPermissionGroups) {
            this(remoteClusterAliases, StringMatcher.of(remoteClusterAliases), indicesPermissionGroups);
        }

        public boolean checkRemoteClusterAlias(final String remoteClusterAlias) {
            return remoteClusterAliasMatcher.test(remoteClusterAlias);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            RemoteIndicesGroup that = (RemoteIndicesGroup) o;

            if (false == remoteClusterAliases.equals(that.remoteClusterAliases)) return false;
            return indicesPermissionGroups.equals(that.indicesPermissionGroups);
        }

        @Override
        public int hashCode() {
            int result = remoteClusterAliases.hashCode();
            result = 31 * result + indicesPermissionGroups.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "RemoteIndicesGroup["
                + "remoteClusterAliases="
                + remoteClusterAliases
                + ", "
                + "indicesPermissionGroups="
                + indicesPermissionGroups
                + ']';
        }

    }
}
