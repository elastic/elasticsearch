/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.api;

import java.util.List;
import java.util.ServiceLoader;

public class EntitlementProvider {
    private static final EntitlementChecks CHECKS = lookupEntitlementChecksImplementation();

    public static EntitlementChecks checks() {
        return CHECKS;
    }

    private static EntitlementChecks lookupEntitlementChecksImplementation() {
        configureModule();
        List<EntitlementChecks> candidates = ServiceLoader.load(EntitlementChecks.class, ClassLoader.getSystemClassLoader())
            .stream()
            .map(ServiceLoader.Provider::get)
            .toList();
        if (candidates.isEmpty()) {
            throw new IllegalStateException("No EntitlementChecks service");
        } else if (candidates.size() >= 2) {
            throw new IllegalStateException(
                "Multiple EntitlementChecks services: " + candidates.stream().map(e -> e.getClass().getSimpleName()).toList()
            );
        } else {
            return candidates.get(0);
        }
    }

    /**
     * At runtime, this code runs in java.base, so we can't use a normal module-info.java
     */
    private static void configureModule() {
        Module thisModule = EntitlementProvider.class.getModule();
        thisModule.addUses(EntitlementChecks.class);
    }

}
