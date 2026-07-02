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
     * <p>
     * For each method in the class, the instrumenter first checks for a direct rule matching the class and method.
     * If no direct rule is found and the method is a non-static instance method, it searches the supertype hierarchy
     * (superclasses and interfaces via BFS) for an inherited rule. At most one inherited rule may exist in the
     * hierarchy; this invariant is enforced by the registry's {@code validate()} method at startup.
     * <p>
     * The injected prologue:
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

    /**
     * Reads the direct supertypes (superclass and interfaces) from raw classfile bytes.
     *
     * @param classfileBuffer the raw classfile bytes
     * @return internal names of all direct supertypes (e.g. {@code "java/lang/Object"})
     */
    String[] readDirectSupertypes(byte[] classfileBuffer);

    /**
     * Returns {@code true} if any supertype in the full hierarchy of the class described
     * by the given classfile bytes has entitlement rules defined on it.
     * <p>
     * Performs a BFS traversal of the supertype hierarchy using classpath resource reading,
     * without triggering class loading.
     *
     * @param classfileBuffer the raw classfile bytes of the class to check
     * @return {@code true} if any ancestor has entitlement rules
     */
    boolean hasRuleInHierarchy(byte[] classfileBuffer);
}
