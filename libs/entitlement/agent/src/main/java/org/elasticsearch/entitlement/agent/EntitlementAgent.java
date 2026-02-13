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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * A Java Agent that sets up the bytecode instrumentation for the entitlement system.
 * <p>
 * Agents are loaded into the unnamed module, which makes module exports awkward.
 * To work around this, we keep minimal code in the agent itself, and
 * instead use reflection to call into the main entitlement library,
 * which bootstraps by using {@link Module#addExports} to make a single {@code initialize}
 * method available for us to call from here.
 * That method does the rest.
 */
public class EntitlementAgent {

    /**
     * The agent main method
     * @param agentArgs arguments passed to the agent.For our agent, this is the class to load and use for Entitlement Initialization.
     *                  See e.g. {@code EntitlementsBootstrap#loadAgent}
     * @param inst      The {@link Instrumentation} instance to use for injecting Entitlements checks
     */
    public static void agentmain(String agentArgs, Instrumentation inst) {
        final Class<?> initClazz;
        try {
            initClazz = Class.forName(agentArgs);
        } catch (ClassNotFoundException e) {
            throw new AssertionError("entitlement agent does could not find EntitlementInitialization", e);
        }

        final Method initMethod;
        try {
            initMethod = initClazz.getMethod("initialize", Instrumentation.class);
        } catch (NoSuchMethodException e) {
            throw new AssertionError("EntitlementInitialization missing initialize method", e);
        }

        try {
            initMethod.invoke(null, inst);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new AssertionError("entitlement initialization failed", e);
        }
    }
}
