/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.instrumentation;

import org.elasticsearch.core.SuppressForbidden;

import java.lang.instrument.ClassFileTransformer;
import java.security.ProtectionDomain;
import java.util.Base64;
import java.util.Set;

/**
 * A {@link ClassFileTransformer} that applies an {@link Instrumenter} to the appropriate classes.
 */
public class Transformer implements ClassFileTransformer {
    private final Instrumenter instrumenter;
    private final Set<String> classesToTransform;

    public Transformer(Instrumenter instrumenter, Set<String> classesToTransform) {
        this.instrumenter = instrumenter;
        this.classesToTransform = classesToTransform;
        // TODO: Should warn if any MethodKey doesn't match any methods
    }

    @SuppressForbidden(reason = "debugging cloud")
    @Override
    public byte[] transform(
        ClassLoader loader,
        String className,
        Class<?> classBeingRedefined,
        ProtectionDomain protectionDomain,
        byte[] classfileBuffer
    ) {
        if (classesToTransform.contains(className)) {
            var dumpClass = className.equals("sun/net/www/protocol/https/AbstractDelegateHttpsURLConnection");
            if (dumpClass) {
                System.out.println(
                    "Before: Transformed AbstractDelegateHttpsURLConnection:" + Base64.getEncoder().encodeToString(classfileBuffer)
                );
            }
            // System.out.println("Transforming " + className);
            byte[] bytes = instrumenter.instrumentClass(className, classfileBuffer);
            if (dumpClass) {
                System.out.println("After: Transformed AbstractDelegateHttpsURLConnection:" + Base64.getEncoder().encodeToString(bytes));
            }
            return bytes;
        } else {
            // System.out.println("Not transforming " + className);
            return classfileBuffer;
        }
    }

    // private static final Logger LOGGER = LogManager.getLogger(Transformer.class);
}
