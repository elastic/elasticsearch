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
 * are automatically detected and instrumented. The {@link #classesInRuleHierarchy} set is seeded
 * with classes that have direct rules and grows incrementally as subtypes are discovered,
 * relying on the JVM guarantee that supertypes are always loaded before subtypes.
 */
public class Transformer implements ClassFileTransformer {
    private static final Logger logger = LogManager.getLogger(Transformer.class);

    private final Instrumenter instrumenter;
    private final Set<String> classesInRuleHierarchy;
    private final AtomicBoolean hadErrors = new AtomicBoolean(false);

    private boolean verifyClasses;

    /**
     * @param instrumenter          the instrumenter to apply to matched classes
     * @param classesInRuleHierarchy a concurrent set of internal class names that should be instrumented,
     *                               including both classes with direct rules and discovered subtypes.
     *                               This set is grown by the transformer as new subtypes are discovered.
     * @param verifyClasses          whether to verify bytecode before and after instrumentation
     */
    public Transformer(Instrumenter instrumenter, Set<String> classesInRuleHierarchy, boolean verifyClasses) {
        this.instrumenter = instrumenter;
        this.classesInRuleHierarchy = classesInRuleHierarchy;
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
        if (classesInRuleHierarchy.contains(className)) {
            return doInstrument(className, classfileBuffer);
        }

        if (isNonJdkClassLoader(loader)) {
            return null;
        }

        if (hasSupertypeInRuleHierarchy(classBeingRedefined, classfileBuffer)) {
            classesInRuleHierarchy.add(className);
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

    private boolean hasSupertypeInRuleHierarchy(Class<?> classBeingRedefined, byte[] classfileBuffer) {
        if (classBeingRedefined != null) {
            Class<?> sup = classBeingRedefined.getSuperclass();
            if (sup != null && classesInRuleHierarchy.contains(sup.getName().replace('.', '/'))) {
                return true;
            }
            for (Class<?> iface : classBeingRedefined.getInterfaces()) {
                if (classesInRuleHierarchy.contains(iface.getName().replace('.', '/'))) {
                    return true;
                }
            }
            return false;
        }
        for (String supertype : instrumenter.readDirectSupertypes(classfileBuffer)) {
            if (classesInRuleHierarchy.contains(supertype)) {
                return true;
            }
        }
        return false;
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
