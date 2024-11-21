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
import org.elasticsearch.entitlement.bridge.EntitlementChecker;
import org.elasticsearch.entitlement.instrumentation.CheckerMethod;
import org.elasticsearch.entitlement.instrumentation.InstrumentationService;
import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.elasticsearch.entitlement.instrumentation.Transformer;
import org.elasticsearch.entitlement.runtime.api.ElasticsearchEntitlementChecker;

import java.lang.instrument.Instrumentation;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Called by the agent during {@code agentmain} to configure the entitlement system,
 * instantiate and configure an {@link EntitlementChecker},
 * make it available to the bootstrap library via {@link #checker()},
 * and then install the {@link org.elasticsearch.entitlement.instrumentation.Instrumenter}
 * to begin injecting our instrumentation.
 */
public class EntitlementInitialization {
    private static ElasticsearchEntitlementChecker manager;

    // Note: referenced by bridge reflectively
    public static EntitlementChecker checker() {
        return manager;
    }

    // Note: referenced by agent reflectively
    public static void initialize(Instrumentation inst) throws Exception {
        manager = new ElasticsearchEntitlementChecker();

        Map<MethodKey, CheckerMethod> methodMap = INSTRUMENTER_FACTORY.lookupMethodsToInstrument(
            "org.elasticsearch.entitlement.bridge.EntitlementChecker"
        );

        var classesToTransform = methodMap.keySet().stream().map(MethodKey::className).collect(Collectors.toSet());

        inst.addTransformer(new Transformer(INSTRUMENTER_FACTORY.newInstrumenter("", methodMap), classesToTransform), true);
        // TODO: should we limit this array somehow?
        var classesToRetransform = classesToTransform.stream().map(EntitlementInitialization::internalNameToClass).toArray(Class[]::new);
        inst.retransformClasses(classesToRetransform);
    }

    private static Class<?> internalNameToClass(String internalName) {
        try {
            return Class.forName(internalName.replace('/', '.'), false, ClassLoader.getPlatformClassLoader());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static final InstrumentationService INSTRUMENTER_FACTORY = new ProviderLocator<>(
        "entitlement",
        InstrumentationService.class,
        "org.elasticsearch.entitlement.instrumentation",
        Set.of()
    ).get();
}
