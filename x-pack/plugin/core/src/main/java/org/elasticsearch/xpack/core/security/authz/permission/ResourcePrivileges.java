/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/**
 * A generic structure to encapsulate resource to privileges map.
 */
public final class ResourcePrivileges {

    private final String resource;
    private final Map<String, Boolean> privileges;

    ResourcePrivileges(String resource, Map<String, Boolean> privileges) {
        this.resource = Objects.requireNonNull(resource);
        this.privileges = Collections.unmodifiableMap(privileges);
    }

    public String getResource() {
        return resource;
    }

    public Map<String, Boolean> getPrivileges() {
        return privileges;
    }

    public boolean isAllowed(String privilege) {
        return Boolean.TRUE.equals(privileges.get(privilege));
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" + "resource='" + resource + '\'' + ", privileges=" + privileges + '}';
    }

    @Override
    public int hashCode() {
        int result = resource.hashCode();
        result = 31 * result + privileges.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ResourcePrivileges other = (ResourcePrivileges) o;
        return this.resource.equals(other.resource) && this.privileges.equals(other.privileges);
    }

    public static Builder builder(String resource) {
        return new Builder(resource);
    }

    public static final class Builder {
        private final String resource;
        private Map<String, Boolean> privileges = new HashMap<>();

        private Builder(String resource) {
            this.resource = resource;
        }

        public Builder addPrivilege(String privilege, Boolean allowed) {
            this.privileges.compute(privilege, (k, v) -> ((v == null) ? allowed : v && allowed));
            return this;
        }

        public Builder addPrivileges(Map<String, Boolean> privilegeMap) {
            for (Entry<String, Boolean> entry : privilegeMap.entrySet()) {
                addPrivilege(entry.getKey(), entry.getValue());
            }
            return this;
        }

        public ResourcePrivileges build() {
            return new ResourcePrivileges(resource, privileges);
        }
    }
}
