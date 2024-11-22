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
 * A holder for entitlements within a single scope.
 */
public class Scope {

    public final String name;
    public final List<Entitlement> entitlements;

    public Scope(String name, List<Entitlement> entitlements) {
        this.name = Objects.requireNonNull(name);
        this.entitlements = Collections.unmodifiableList(Objects.requireNonNull(entitlements));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Scope scope = (Scope) o;
        return Objects.equals(name, scope.name) && Objects.equals(entitlements, scope.entitlements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, entitlements);
    }

    @Override
    public String toString() {
        return "Scope{" + "name='" + name + '\'' + ", entitlements=" + entitlements + '}';
    }
}
