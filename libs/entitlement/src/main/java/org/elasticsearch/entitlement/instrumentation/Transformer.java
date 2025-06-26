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
 */
public class Transformer implements ClassFileTransformer {
    private static final Logger logger = LogManager.getLogger(Transformer.class);
    private final Instrumenter instrumenter;
    private final Set<String> classesToTransform;
    private final AtomicBoolean hadErrors = new AtomicBoolean(false);

    private boolean verifyClasses;

    public Transformer(Instrumenter instrumenter, Set<String> classesToTransform, boolean verifyClasses) {
        this.instrumenter = instrumenter;
        this.classesToTransform = classesToTransform;
        this.verifyClasses = verifyClasses;
        // TODO: Should warn if any MethodKey doesn't match any methods
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
        if (classesToTransform.contains(className)) {
            logger.debug("Transforming " + className);
            try {
                return instrumenter.instrumentClass(className, classfileBuffer, verifyClasses);
            } catch (Throwable t) {
                hadErrors.set(true);
                logger.error("Failed to instrument class " + className, t);
                // throwing an exception from a transformer results in the exception being swallowed,
                // effectively the same as returning null anyways, so we instead log it here completely
                return null;
            }
        } else {
            logger.trace("Not transforming " + className);
            return null;
        }
    }
}
