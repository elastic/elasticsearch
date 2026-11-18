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
 * The decisions that {@link org.elasticsearch.entitlement.runtime.api.EntitlementDelegation} needs from the policy manager.
 */
public interface DelegationPolicy {

    /**
     * Whether the checks made by the code of {@code type} may use the entitlements of another class.
     */
    boolean canDelegate(Class<?> type);

    /**
     * Whether a stack frame of {@code type} is skipped when looking for the class whose entitlements to use. These are the
     * classes of the JDK and of the entitlement library.
     */
    boolean isSkippedFrame(Class<?> type);
}
