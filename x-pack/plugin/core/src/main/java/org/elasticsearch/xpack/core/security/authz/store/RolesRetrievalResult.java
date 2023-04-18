/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.store;

import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public final class RolesRetrievalResult {

    public static final RolesRetrievalResult EMPTY = new RolesRetrievalResult();
    public static final RolesRetrievalResult SUPERUSER;

    static {
        SUPERUSER = new RolesRetrievalResult();
        SUPERUSER.addDescriptors(Set.of(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR));
    }

    private final Set<RoleDescriptor> roleDescriptors = new HashSet<>();
    private Set<String> missingRoles = Collections.emptySet();
    private boolean success = true;

    public void addDescriptors(Set<RoleDescriptor> descriptors) {
        roleDescriptors.addAll(descriptors);
    }

    public Set<RoleDescriptor> getRoleDescriptors() {
        return roleDescriptors;
    }

    public void setFailure() {
        success = false;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setMissingRoles(Set<String> missingRoles) {
        this.missingRoles = missingRoles;
    }

    public Set<String> getMissingRoles() {
        return missingRoles;
    }
}
