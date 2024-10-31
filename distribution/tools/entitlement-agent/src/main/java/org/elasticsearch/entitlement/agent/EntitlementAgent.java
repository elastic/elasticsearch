/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.agent;

import java.lang.instrument.Instrumentation;

/**
 * Java agents are not loaded as modules, so we want to run as little code as possible in this bizarro world.
 * Hence, we only act as a shim that calls into the runtime and instrumentation modules to initialize them.
 */
public class EntitlementAgent {

    public static void agentmain(String agentArgs, Instrumentation inst) throws Exception {
        // Initialize runtime first. The runtime must be prepared to be called before we inject any instrumentation.
        Class.forName(
                "org.elasticsearch.entitlement.runtime.init.EntitlementRuntimeInit",
                true,
                ClassLoader.getSystemClassLoader())
            .getMethod("initialize").invoke(null);

        // Let 'er rip.
        Class.forName(
            "org.elasticsearch.entitlement.instrumentation.init.EntitlementInstrumentationInit",
                true,
                ClassLoader.getSystemClassLoader())
            .getMethod("initialize", Instrumentation.class).invoke(null, inst);
    }

}
