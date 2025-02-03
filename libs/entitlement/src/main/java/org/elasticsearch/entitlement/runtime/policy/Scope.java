/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.entitlement.runtime.policy.entitlements.Entitlement;

import java.util.List;
import java.util.Objects;

/**
 * A holder for entitlements within a single scope.
 */
public record Scope(String moduleName, List<Entitlement> entitlements) {

    public Scope(String moduleName, List<Entitlement> entitlements) {
        this.moduleName = Objects.requireNonNull(moduleName);
        this.entitlements = List.copyOf(entitlements);
    }

}
