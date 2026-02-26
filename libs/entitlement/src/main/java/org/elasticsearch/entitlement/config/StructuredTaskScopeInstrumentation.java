/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.config;

import org.elasticsearch.entitlement.rules.EntitlementRulesBuilder;
import org.elasticsearch.entitlement.rules.Policies;
import org.elasticsearch.entitlement.runtime.registry.InternalInstrumentationRegistry;

import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.ThreadFactory;

@SuppressWarnings("rawtypes")
public class StructuredTaskScopeInstrumentation implements InstrumentationConfig {
    @Override
    public void init(InternalInstrumentationRegistry registry) {
        EntitlementRulesBuilder builder = new EntitlementRulesBuilder(registry);

        builder.on(StructuredTaskScope.class, rule -> {
            rule.protectedCtor().enforce(Policies::manageThreads).elseThrowNotEntitled();
            rule.protectedCtor(String.class, ThreadFactory.class).enforce(Policies::manageThreads).elseThrowNotEntitled();
        });
    }
}
