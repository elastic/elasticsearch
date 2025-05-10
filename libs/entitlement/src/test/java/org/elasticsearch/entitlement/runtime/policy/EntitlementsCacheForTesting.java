/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import java.util.concurrent.ConcurrentHashMap;

/**
 * When testing, we don't use modules, so we cache per-class.
 */
public class EntitlementsCacheForTesting extends ConcurrentHashMap<Class<?>, EntitlementsCache.ModuleEntitlements>
    implements
        EntitlementsCache {
    @Override
    public boolean isAlwaysAllowed(Class<?> key) {
        return key.getPackageName().startsWith("org.gradle") || key.getPackageName().startsWith("org.junit");
    }
}
