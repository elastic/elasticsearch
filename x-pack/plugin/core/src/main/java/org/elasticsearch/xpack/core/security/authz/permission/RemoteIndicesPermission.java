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
        return new RemoteIndicesPermission(
            remoteIndicesGroups.stream()
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

        public Builder addGroup(
            final Set<String> remoteClusterAliases,
            final IndexPrivilege privilege,
            final FieldPermissions fieldPermissions,
            final @Nullable Set<BytesReference> query,
            final boolean allowRestrictedIndices,
            final String... indices
        ) {
            assert query == null || query.size() <= 1 : "remote indices groups only support up to one DLS query";
            assert fieldPermissions.getFieldPermissionsDefinitions()
                .stream()
                .noneMatch(groups -> groups.getFieldGrantExcludeGroups().size() > 1)
                : "remote indices groups only support up to one FLS field-grant-exclude group";
            remoteIndicesGroups.computeIfAbsent(remoteClusterAliases, k -> new ArrayList<>())
                .add(
                    new IndicesPermission.Group(
                        privilege,
                        fieldPermissions,
                        query,
                        allowRestrictedIndices,
                        // Deliberately passing EMPTY here since *which* indices are restricted is determined not on the querying cluster
                        // but rather on the fulfilling cluster
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
