/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.instrumentation;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.lang.instrument.ClassFileTransformer;
import java.security.ProtectionDomain;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link ClassFileTransformer} that applies an {@link Instrumenter} to the appropriate classes.
 * <p>
 * Supports hierarchy-aware instrumentation: classes that extend a type with entitlement rules
 * are automatically detected and instrumented. For each class, the transformer performs a full
 * BFS traversal of the supertype hierarchy (without class loading) to determine if any ancestor
 * has entitlement rules. This approach is resilient to class visitation order.
 */
public class Transformer implements ClassFileTransformer {
    private static final Logger logger = LogManager.getLogger(Transformer.class);

    private final Instrumenter instrumenter;
    private final Set<String> classesWithDirectRules;
    private final AtomicBoolean hadErrors = new AtomicBoolean(false);

    private boolean verifyClasses;

    /**
     * @param instrumenter          the instrumenter to apply to matched classes
     * @param classesWithDirectRules an immutable set of internal class names that have direct entitlement rules
     * @param verifyClasses          whether to verify bytecode before and after instrumentation
     */
    public Transformer(Instrumenter instrumenter, Set<String> classesWithDirectRules, boolean verifyClasses) {
        this.instrumenter = instrumenter;
        this.classesWithDirectRules = classesWithDirectRules;
        this.verifyClasses = verifyClasses;
    }

    public void enableClassVerification() {
        this.verifyClasses = true;
    }

    public boolean hadErrors() {
        return hadErrors.get();
    }

    @Override
    public byte[] transform(
        ClassLoader loader,
        String className,
        Class<?> classBeingRedefined,
        ProtectionDomain protectionDomain,
        byte[] classfileBuffer
    ) {
        if (classesWithDirectRules.contains(className)) {
            return doInstrument(className, classfileBuffer);
        }

        if (isNonJdkClassLoader(loader)) {
            return null;
        }

        if (instrumenter.hasRuleInHierarchy(classfileBuffer)) {
            return doInstrument(className, classfileBuffer);
        }

        return null;
    }

    /**
     * Returns {@code true} if the given classloader indicates a non-JDK class.
     * JDK classes are loaded by the bootstrap classloader ({@code null}) or the platform classloader.
     * Non-JDK classes (plugins, application code) should not have inherited rules applied,
     * because they live in child {@link java.lang.ModuleLayer}s that cannot access the bridge package.
     */
    private static boolean isNonJdkClassLoader(ClassLoader loader) {
        return loader != null && loader != ClassLoader.getPlatformClassLoader();
    }

    private byte[] doInstrument(String className, byte[] classfileBuffer) {
        logger.debug("Transforming [{}]", className);
        try {
            return instrumenter.instrumentClass(className, classfileBuffer, verifyClasses);
        } catch (Throwable t) {
            hadErrors.set(true);
            logger.error("Failed to instrument class [{}]", className, t);
            return null;
        }
    }
}
