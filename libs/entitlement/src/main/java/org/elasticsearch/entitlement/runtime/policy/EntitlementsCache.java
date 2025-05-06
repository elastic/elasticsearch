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
import org.elasticsearch.logging.Logger;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Quickly provides entitlement info for a given class. Performance-critical.
 */
public interface EntitlementsCache {
    /**
     * @return true if entitlement checking should be bypassed for the given class;
     *         false if the normal built-in "trivially allowed" rules apply.
     */
    default boolean isAlwaysAllowed(Class<?> key) {
        return false;
    }

    ModuleEntitlements computeIfAbsent(Class<?> key, Function<? super Class<?>, ? extends ModuleEntitlements> mappingFunction);

    /**
     * This class contains all the entitlements by type, plus the {@link FileAccessTree} for the special case of filesystem entitlements.
     * <p>
     * We use layers when computing {@link ModuleEntitlements}; first, we check whether the module we are building it for is in the
     * server layer ({@link PolicyManager#SERVER_LAYER_MODULES}) (*).
     * If it is, we use the server policy, using the same caller class module name as the scope, and read the entitlements for that scope.
     * Otherwise, we use the {@code PluginResolver} to identify the correct plugin layer and find the policy for it (if any).
     * If the plugin is modular, we again use the same caller class module name as the scope, and read the entitlements for that scope.
     * If it's not, we use the single {@code ALL-UNNAMED} scope – in this case there is one scope and all entitlements apply
     * to all the plugin code.
     * </p>
     * <p>
     * (*) implementation detail: this is currently done in an indirect way: we know the module is not in the system layer
     * (otherwise the check would have been already trivially allowed), so we just check that the module is named, and it belongs to the
     * boot {@link ModuleLayer}. We might want to change this in the future to make it more consistent/easier to maintain.
     * </p>
     *
     * @param componentName the plugin name or else one of the special component names like "(server)".
     */
    record ModuleEntitlements(
        String componentName,
        Map<Class<? extends Entitlement>, List<Entitlement>> entitlementsByType,
        FileAccessTree fileAccess,
        Logger logger
    ) {

        public ModuleEntitlements {
            entitlementsByType = Map.copyOf(entitlementsByType);
        }

        public boolean hasEntitlement(Class<? extends Entitlement> entitlementClass) {
            return entitlementsByType.containsKey(entitlementClass);
        }

        public <E extends Entitlement> Stream<E> getEntitlements(Class<E> entitlementClass) {
            var entitlements = entitlementsByType.get(entitlementClass);
            if (entitlements == null) {
                return Stream.empty();
            }
            return entitlements.stream().map(entitlementClass::cast);
        }
    }
}
