/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A generic structure to encapsulate resources to {@link ResourcePrivileges}. Also keeps track of whether the resource privileges allow
 * permissions to all resources.
 */
public final class ResourcePrivilegesMap {

    private final boolean allAllowed;
    private final Map<String, ResourcePrivileges> resourceToResourcePrivileges;

    public ResourcePrivilegesMap(boolean allAllowed, Map<String, ResourcePrivileges> resToResPriv) {
        this.allAllowed = allAllowed;
        this.resourceToResourcePrivileges = Collections.unmodifiableMap(Objects.requireNonNull(resToResPriv));
    }

    public boolean allAllowed() {
        return allAllowed;
    }

    public Map<String, ResourcePrivileges> getResourceToResourcePrivileges() {
        return resourceToResourcePrivileges;
    }

    @Override
    public int hashCode() {
        return Objects.hash(allAllowed, resourceToResourcePrivileges);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ResourcePrivilegesMap other = (ResourcePrivilegesMap) obj;
        return allAllowed == other.allAllowed && Objects.equals(resourceToResourcePrivileges, other.resourceToResourcePrivileges);
    }

    @Override
    public String toString() {
        return "ResourcePrivilegesMap [allAllowed=" + allAllowed + ", resourceToResourcePrivileges=" + resourceToResourcePrivileges + "]";
    }

    public static final class Builder {
        private boolean allowAll = true;
        private Map<String, ResourcePrivileges.Builder> resourceToResourcePrivilegesBuilder = new LinkedHashMap<>();

        public Builder addResourcePrivilege(String resource, String privilege, Boolean allowed) {
            assert resource != null && privilege != null
                    && allowed != null : "resource, privilege and permission(allowed or denied) are required";
            ResourcePrivileges.Builder builder = resourceToResourcePrivilegesBuilder.computeIfAbsent(resource, ResourcePrivileges::builder);
            builder.addPrivilege(privilege, allowed);
            allowAll = allowAll && allowed;
            return this;
        }

        public Builder addResourcePrivilege(String resource, Map<String, Boolean> privilegePermissions) {
            assert resource != null && privilegePermissions != null : "resource, privilege permissions(allowed or denied) are required";
            ResourcePrivileges.Builder builder = resourceToResourcePrivilegesBuilder.computeIfAbsent(resource, ResourcePrivileges::builder);
            builder.addPrivileges(privilegePermissions);
            allowAll = allowAll && privilegePermissions.values().stream().allMatch(b -> Boolean.TRUE.equals(b));
            return this;
        }

        public Builder addResourcePrivilegesMap(ResourcePrivilegesMap resourcePrivilegesMap) {
            resourcePrivilegesMap.getResourceToResourcePrivileges().entrySet().stream()
                    .forEach(e -> this.addResourcePrivilege(e.getKey(), e.getValue().getPrivileges()));
            return this;
        }

        public ResourcePrivilegesMap build() {
            Map<String, ResourcePrivileges> result = resourceToResourcePrivilegesBuilder.entrySet().stream()
                    .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().build()));
            return new ResourcePrivilegesMap(allowAll, result);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Takes an intersection of resource privileges and returns a new instance of {@link ResourcePrivilegesMap}. If one of the resource
     * privileges map does not allow access to a resource then the resulting map would also not allow access.
     *
     * @param left an instance of {@link ResourcePrivilegesMap}
     * @param right an instance of {@link ResourcePrivilegesMap}
     * @return a new instance of {@link ResourcePrivilegesMap}, an intersection of resource privileges.
     */
    public static ResourcePrivilegesMap intersection(final ResourcePrivilegesMap left, final ResourcePrivilegesMap right) {
        Objects.requireNonNull(left);
        Objects.requireNonNull(right);
        final ResourcePrivilegesMap.Builder builder = ResourcePrivilegesMap.builder();
        for (Entry<String, ResourcePrivileges> leftResPrivsEntry : left.getResourceToResourcePrivileges().entrySet()) {
            final ResourcePrivileges leftResPrivs = leftResPrivsEntry.getValue();
            final ResourcePrivileges rightResPrivs = right.getResourceToResourcePrivileges().get(leftResPrivsEntry.getKey());
            builder.addResourcePrivilege(leftResPrivsEntry.getKey(), leftResPrivs.getPrivileges());
            builder.addResourcePrivilege(leftResPrivsEntry.getKey(), rightResPrivs.getPrivileges());
        }
        return builder.build();
    }
}
