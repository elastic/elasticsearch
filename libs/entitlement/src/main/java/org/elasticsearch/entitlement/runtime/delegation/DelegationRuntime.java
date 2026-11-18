/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.delegation;

/**
 * Gives {@link org.elasticsearch.entitlement.runtime.api.EntitlementDelegation} the {@link DelegationPolicy} of the running
 * entitlement system. Entitlement bootstrap installs it.
 */
public final class DelegationRuntime {
    private static volatile DelegationPolicy policy;

    private DelegationRuntime() {}

    public static void install(DelegationPolicy newPolicy) {
        policy = newPolicy;
    }

    /**
     * @return the installed policy, or {@code null} before entitlement bootstrap
     */
    public static DelegationPolicy policy() {
        return policy;
    }
}
