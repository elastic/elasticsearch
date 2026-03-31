/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.instrumentation;

import org.elasticsearch.entitlement.bridge.InstrumentationRegistry;
import org.elasticsearch.entitlement.bridge.InstrumentationRegistryHandle;

public interface Instrumenter {

    /**
     * Instruments the appropriate methods of a class by adding a prologue that checks for entitlements.
     * The prologue:
     * <ol>
     * <li>
     * gets the {@link InstrumentationRegistry} instance from the
     * {@link InstrumentationRegistryHandle} holder;
     * </li>
     * <li>
     * identifies the caller class and pushes it onto the stack;
     * </li>
     * <li>
     * forwards the instrumented function parameters;
     * </li>
     * <li>
     * calls the {@link InstrumentationRegistry#check$} method.
     * </li>
     * </ol>
     * @param className the name of the class to instrument
     * @param classfileBuffer its bytecode
     * @param verify whether we should verify the bytecode before and after instrumentation
     * @return the instrumented class bytes
     */
    byte[] instrumentClass(String className, byte[] classfileBuffer, boolean verify);
}
