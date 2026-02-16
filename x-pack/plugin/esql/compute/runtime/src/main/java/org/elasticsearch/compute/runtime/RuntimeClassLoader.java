/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.runtime;

import org.elasticsearch.bootstrap.BootstrapInfo;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;

import java.net.MalformedURLException;
import java.net.URL;
import java.security.CodeSource;
import java.security.SecureClassLoader;
import java.security.cert.Certificate;

/**
 * A secure class loader for defining runtime-generated classes (evaluators and aggregators).
 * <p>
 * This follows the same pattern as Painless's Compiler.Loader, using a
 * SecureClassLoader with restricted code source for security.
 * </p>
 * <p>
 * The parent classloader should be the plugin's classloader, which allows
 * the generated classes to access both the user's function classes and
 * the compute module's public classes.
 * </p>
 */
public final class RuntimeClassLoader extends SecureClassLoader {

    /**
     * Code source for generated classes - uses untrusted codebase for security.
     */
    private static final CodeSource CODESOURCE;

    static {
        try {
            CODESOURCE = new CodeSource(new URL("file:" + BootstrapInfo.UNTRUSTED_CODEBASE), (Certificate[]) null);
        } catch (MalformedURLException e) {
            throw new RuntimeException("Failed to create CodeSource for runtime class loader", e);
        }
    }

    /**
     * Creates a new RuntimeClassLoader with the given parent.
     *
     * @param parent the parent class loader, typically the ES|QL plugin's class loader
     */
    public RuntimeClassLoader(ClassLoader parent) {
        super(parent);
    }

    /**
     * Defines a class from bytecode.
     *
     * @param name the fully qualified class name
     * @param bytecode the class bytecode
     * @return the defined class
     * @throws ClassFormatError if the bytecode is invalid
     */
    public Class<?> defineClass(String name, byte[] bytecode) {
        return defineClass(name, bytecode, 0, bytecode.length, CODESOURCE);
    }

    /**
     * Defines an evaluator class from bytecode.
     *
     * @param name the fully qualified class name
     * @param bytecode the class bytecode
     * @return the defined class, which implements ExpressionEvaluator
     * @throws ClassFormatError if the bytecode is invalid
     */
    public Class<? extends ExpressionEvaluator> defineEvaluator(String name, byte[] bytecode) {
        Class<?> defined = defineClass(name, bytecode);
        return defined.asSubclass(ExpressionEvaluator.class);
    }
}
