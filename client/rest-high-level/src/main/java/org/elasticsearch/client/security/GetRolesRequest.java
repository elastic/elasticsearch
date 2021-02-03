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
 * Request object to retrieve roles from the native roles store
 */
public final class GetRolesRequest implements Validatable {

    private final Set<String> roleNames;

    public GetRolesRequest(final String... roleNames) {
        if (roleNames != null) {
            this.roleNames = Collections.unmodifiableSet(Sets.newHashSet(roleNames));
        } else {
            this.roleNames = Collections.emptySet();
        }
    }

    public Set<String> getRoleNames() {
        return roleNames;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final GetRolesRequest that = (GetRolesRequest) o;
        return Objects.equals(roleNames, that.roleNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(roleNames);
    }
}
