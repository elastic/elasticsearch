/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A holder for scoped entitlements.
 */
public class Policy {

    public final String name;
    public final List<Scope> scopes;

    public Policy(String name, List<Scope> scopes) {
        this.name = Objects.requireNonNull(name);
        this.scopes = Collections.unmodifiableList(Objects.requireNonNull(scopes));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Policy policy = (Policy) o;
        return Objects.equals(name, policy.name) && Objects.equals(scopes, policy.scopes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, scopes);
    }

    @Override
    public String toString() {
        return "Policy{" + "name='" + name + '\'' + ", scopes=" + scopes + '}';
    }
}
