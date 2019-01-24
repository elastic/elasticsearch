/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * A generic structure to encapsulate resources to {@link ResourcePrivileges}.
 */
public final class ResourcesPrivileges {

    private final boolean allAllowed;
    private final Map<String, ResourcePrivileges> resourceToResourcePrivileges;

    public ResourcesPrivileges(boolean allAllowed, Map<String, ResourcePrivileges> resToResPriv) {
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
        final ResourcesPrivileges other = (ResourcesPrivileges) obj;
        return allAllowed == other.allAllowed && Objects.equals(resourceToResourcePrivileges, other.resourceToResourcePrivileges);
    }

    @Override
    public String toString() {
        return "ResourcesPrivileges [allAllowed=" + allAllowed + ", resourceToResourcePrivileges=" + resourceToResourcePrivileges + "]";
    }
}
