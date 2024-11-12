/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.initialization;

import org.elasticsearch.core.internal.provider.ProviderLocator;
import org.elasticsearch.entitlement.bootstrap.PluginsResolver;
import org.elasticsearch.entitlement.bridge.EntitlementChecker;
import org.elasticsearch.entitlement.instrumentation.InstrumentationService;
import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.elasticsearch.entitlement.instrumentation.Transformer;
import org.elasticsearch.entitlement.runtime.api.ElasticsearchEntitlementChecker;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

/**
 * Called by the agent during {@code agentmain} to configure the entitlement system,
 * instantiate and configure an {@link EntitlementChecker},
 * make it available to the bootstrap library via {@link #checker()},
 * and then install the {@link org.elasticsearch.entitlement.instrumentation.Instrumenter}
 * to begin injecting our instrumentation.
 */
public class EntitlementInitialization {
    private static ElasticsearchEntitlementChecker manager;

    public static volatile PluginsResolver pluginsResolver;

    // Note: referenced by bridge reflectively
    public static EntitlementChecker checker() {
        return manager;
    }

    // Note: referenced by agent reflectively
    public static void initialize(Instrumentation inst) throws Exception {

        // TODO: build PolicyManager with pluginsResolver
        assert pluginsResolver != null : "PluginsResolver must be set before Entitlement initialization";
        manager = new ElasticsearchEntitlementChecker();

        // TODO: Configure actual entitlement grants instead of this hardcoded one
        Method targetMethod = System.class.getMethod("exit", int.class);
        Method instrumentationMethod = Class.forName("org.elasticsearch.entitlement.bridge.EntitlementChecker")
            .getMethod("checkSystemExit", Class.class, int.class);
        Map<MethodKey, Method> methodMap = Map.of(INSTRUMENTER_FACTORY.methodKeyForTarget(targetMethod), instrumentationMethod);

        inst.addTransformer(new Transformer(INSTRUMENTER_FACTORY.newInstrumenter("", methodMap), Set.of(internalName(System.class))), true);
        inst.retransformClasses(System.class);
    }

    private static String internalName(Class<?> c) {
        return c.getName().replace('.', '/');
    }

    private static final InstrumentationService INSTRUMENTER_FACTORY = new ProviderLocator<>(
        "entitlement",
        InstrumentationService.class,
        "org.elasticsearch.entitlement.instrumentation",
        Set.of()
    ).get();
}
