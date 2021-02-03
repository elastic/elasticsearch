/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.util.set.Sets;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Request object to get role mappings
 */
public final class GetRoleMappingsRequest implements Validatable {
    private final Set<String> roleMappingNames;

    public GetRoleMappingsRequest(final String... roleMappingNames) {
        if (roleMappingNames != null) {
           this.roleMappingNames = Collections.unmodifiableSet(Sets.newHashSet(roleMappingNames));
        } else {
           this.roleMappingNames = Collections.emptySet();
        }
    }

    public Set<String> getRoleMappingNames() {
        return roleMappingNames;
    }

    @Override
    public int hashCode() {
        return Objects.hash(roleMappingNames);
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
        final GetRoleMappingsRequest other = (GetRoleMappingsRequest) obj;

        return Objects.equals(roleMappingNames, other.roleMappingNames);
    }

}
