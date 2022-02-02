/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security.support;

import org.elasticsearch.client.security.user.privileges.Role;

import java.util.Objects;

/**
 * Information about a service account.
 */
public final class ServiceAccountInfo {

    private final String principal;
    private final Role role;

    public ServiceAccountInfo(String principal, Role role) {
        this.principal = principal;
        this.role = role;
    }

    public String getPrincipal() {
        return principal;
    }

    public Role getRole() {
        return role;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServiceAccountInfo that = (ServiceAccountInfo) o;
        return principal.equals(that.principal) && role.equals(that.role);
    }

    @Override
    public int hashCode() {
        return Objects.hash(principal, role);
    }

    @Override
    public String toString() {
        return "ServiceAccountInfo{" + "principal='" + principal + '\'' + ", role=" + role + '}';
    }
}
