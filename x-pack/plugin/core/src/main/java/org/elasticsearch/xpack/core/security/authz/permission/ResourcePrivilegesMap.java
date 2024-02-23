/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * A generic structure to encapsulate resources to {@link ResourcePrivileges}. Also keeps track of whether the resource privileges allow
 * permissions to all resources.
 */
public final class ResourcePrivilegesMap {

    private final Map<String, ResourcePrivileges> resourceToResourcePrivileges;

    public ResourcePrivilegesMap(Map<String, ResourcePrivileges> resToResPriv) {
        this.resourceToResourcePrivileges = Collections.unmodifiableMap(Objects.requireNonNull(resToResPriv));
    }

    public Map<String, ResourcePrivileges> getResourceToResourcePrivileges() {
        return resourceToResourcePrivileges;
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceToResourcePrivileges);
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
        return Objects.equals(resourceToResourcePrivileges, other.resourceToResourcePrivileges);
    }

    @Override
    public String toString() {
        return "ResourcePrivilegesMap [resourceToResourcePrivileges=" + resourceToResourcePrivileges + "]";
    }

    public static final class Builder {
        private Map<String, ResourcePrivileges.Builder> resourceToResourcePrivilegesBuilder = new TreeMap<>();

        public Builder addResourcePrivilege(String resource, String privilege, Boolean allowed) {
            assert resource != null && privilege != null && allowed != null
                : "resource, privilege and permission(allowed or denied) are required";
            ResourcePrivileges.Builder builder = resourceToResourcePrivilegesBuilder.computeIfAbsent(resource, ResourcePrivileges::builder);
            builder.addPrivilege(privilege, allowed);
            return this;
        }

        public Builder addResourcePrivilege(String resource, Map<String, Boolean> privilegePermissions) {
            assert resource != null && privilegePermissions != null : "resource, privilege permissions(allowed or denied) are required";
            ResourcePrivileges.Builder builder = resourceToResourcePrivilegesBuilder.computeIfAbsent(resource, ResourcePrivileges::builder);
            builder.addPrivileges(privilegePermissions);
            return this;
        }

        public Builder addResourcePrivilegesMap(ResourcePrivilegesMap resourcePrivilegesMap) {
            resourcePrivilegesMap.getResourceToResourcePrivileges()
                .forEach((key, value) -> this.addResourcePrivilege(key, value.getPrivileges()));
            return this;
        }

        public ResourcePrivilegesMap build() {
            Map<String, ResourcePrivileges> result = resourceToResourcePrivilegesBuilder.entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().build()));
            return new ResourcePrivilegesMap(result);
        }
    }

    public static Builder builder() {
        return new Builder();
    }
}
