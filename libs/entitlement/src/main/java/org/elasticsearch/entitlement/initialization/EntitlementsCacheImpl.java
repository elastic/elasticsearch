/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.initialization;

import org.elasticsearch.entitlement.runtime.policy.EntitlementsCache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * The production {@link EntitlementsCache}. (Tests use {@code EntitlementCacheForTesting}.)
 */
final class EntitlementsCacheImpl extends ConcurrentHashMap<Module, EntitlementsCache.ModuleEntitlements> implements EntitlementsCache {
    @Override
    public ModuleEntitlements computeIfAbsent(Class<?> key, Function<? super Class<?>, ? extends ModuleEntitlements> mappingFunction) {
        // We cache per module rather than per class to make the cache smaller and increase the hit ratio
        return computeIfAbsent(key.getModule(), m -> requireNonNull(mappingFunction.apply(key)));
    }
}
