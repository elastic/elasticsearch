/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.internals;

import org.elasticsearch.entitlement.runtime.checks.EntitlementChecksService;

/**
 * Don't export this from the module. Just don't.
 */
public class EntitlementInternals {
    /**
     * When false, entitlement rules are not enforced; all operations are allowed.
     */
    public static volatile boolean isActive = false;

    /**
     * When true, no changes are allowed to the permissions
     */
    public static volatile boolean isFrozen = false;

    public static void reset() {
        isFrozen = false;
        isActive = false;
        EntitlementChecksService.get().revokeAll();
    }
}
