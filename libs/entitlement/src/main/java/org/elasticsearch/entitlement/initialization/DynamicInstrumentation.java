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
import org.elasticsearch.entitlement.bridge.InstrumentationRegistry;
import org.elasticsearch.entitlement.instrumentation.InstrumentationService;
import org.elasticsearch.entitlement.instrumentation.Instrumenter;
import org.elasticsearch.entitlement.instrumentation.Transformer;
import org.elasticsearch.entitlement.runtime.registry.InternalInstrumentationRegistry;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.lang.instrument.Instrumentation;
import java.lang.instrument.UnmodifiableClassException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

class DynamicInstrumentation {

    private static final Logger logger = LogManager.getLogger(DynamicInstrumentation.class);

    private static final InstrumentationService INSTRUMENTATION_SERVICE = new ProviderLocator<>(
        "entitlement",
        InstrumentationService.class,
        "org.elasticsearch.entitlement.instrumentation",
        Set.of()
    ).get();

    /**
     * Initializes the dynamic (agent-based) instrumentation:
     * <ol>
     * <li>
     * Creates an {@link Instrumenter} via {@link InstrumentationService#newInstrumenter}, and adds a new {@link Transformer} (derived from
     * {@link java.lang.instrument.ClassFileTransformer}) that uses it. Transformers are invoked when a class is about to load, after its
     * bytes have been deserialized to memory but before the class is initialized.
     * </li>
     * <li>
     * Re-transforms all already loaded classes: we force the {@link Instrumenter} to run on classes that might have been already loaded
     * before entitlement initialization by calling the {@link java.lang.instrument.Instrumentation#retransformClasses} method on all
     * classes that were already loaded.
     * </li>
     * </ol>
     *
     * @param inst             the JVM instrumentation class instance
     * @param verifyBytecode   whether we should perform bytecode verification before and after instrumenting each method
     * @param registry         the instrumentation registry to use for getting instrumented methods
     */
    static void initialize(Instrumentation inst, boolean verifyBytecode, InternalInstrumentationRegistry registry)
        throws UnmodifiableClassException {

        var rulesByClass = registry.getInstrumentedMethods();

        Set<String> classesInRuleHierarchy = ConcurrentHashMap.newKeySet();
        classesInRuleHierarchy.addAll(rulesByClass.keySet());

        Instrumenter instrumenter = INSTRUMENTATION_SERVICE.newInstrumenter(InstrumentationRegistry.class, rulesByClass);
        var transformer = new Transformer(instrumenter, classesInRuleHierarchy, verifyBytecode);
        inst.addTransformer(transformer, true);

        var classesToRetransform = findClassesToRetransform(inst, inst.getAllLoadedClasses(), classesInRuleHierarchy);
        try {
            inst.retransformClasses(classesToRetransform);
        } catch (VerifyError e) {
            // Turn on verification and try to retransform one class at the time to get detailed diagnostic
            transformer.enableClassVerification();

            for (var classToRetransform : classesToRetransform) {
                inst.retransformClasses(classToRetransform);
            }

            // We should have failed already in the loop above, but just in case we did not, rethrow.
            throw e;
        }

        if (transformer.hadErrors()) {
            throw new RuntimeException("Failed to transform JDK classes for entitlements");
        }
    }

    /**
     * Finds already-loaded classes that need retransformation, including subtypes of classes with rules.
     * Classes are sorted by hierarchy depth (supertypes first) so that the {@code classesInRuleHierarchy}
     * set is populated correctly for transitive inheritance (e.g. A extends B extends Socket).
     */
    private static Class<?>[] findClassesToRetransform(Instrumentation inst, Class<?>[] loadedClasses, Set<String> classesInRuleHierarchy) {
        Arrays.sort(loadedClasses, Comparator.comparingInt(DynamicInstrumentation::hierarchyDepth));

        List<Class<?>> retransform = new ArrayList<>();
        for (Class<?> loadedClass : loadedClasses) {
            if (loadedClass.isHidden()) {
                continue;
            }
            String internalName = loadedClass.getName().replace('.', '/');
            boolean directMatch = classesInRuleHierarchy.contains(internalName);
            boolean hierarchyMatch = false;
            if (directMatch == false) {
                ClassLoader cl = loadedClass.getClassLoader();
                if (cl != null && cl != ClassLoader.getPlatformClassLoader()) {
                    continue;
                }
                hierarchyMatch = hasSupertypeInSet(loadedClass, classesInRuleHierarchy);
                if (hierarchyMatch == false) {
                    continue;
                }
            }
            if (inst.isModifiableClass(loadedClass) == false) {
                logger.warn(
                    "Class [{}] matched for instrumentation but is not modifiable (directMatch={}, loader={}, super={}, interfaces={})",
                    loadedClass.getName(),
                    directMatch,
                    loadedClass.getClassLoader(),
                    loadedClass.getSuperclass(),
                    Arrays.toString(loadedClass.getInterfaces())
                );
                continue;
            }
            if (hierarchyMatch) {
                classesInRuleHierarchy.add(internalName);
            }
            retransform.add(loadedClass);
        }
        return retransform.toArray(new Class<?>[0]);
    }

    private static boolean hasSupertypeInSet(Class<?> clazz, Set<String> classNames) {
        Class<?> sup = clazz.getSuperclass();
        if (sup != null && classNames.contains(sup.getName().replace('.', '/'))) {
            return true;
        }
        for (Class<?> iface : clazz.getInterfaces()) {
            if (classNames.contains(iface.getName().replace('.', '/'))) {
                return true;
            }
        }
        return false;
    }

    private static int hierarchyDepth(Class<?> clazz) {
        int depth = 0;
        Class<?> c = clazz;
        while (c != null) {
            depth++;
            c = c.getSuperclass();
        }
        return depth;
    }
}
