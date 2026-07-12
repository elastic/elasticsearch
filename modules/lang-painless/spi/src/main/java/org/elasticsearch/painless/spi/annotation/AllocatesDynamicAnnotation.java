/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless.spi.annotation;

/**
 * Marks an allowlisted constructor or method whose allocation size depends on its arguments. An <i>estimator</i> — a
 * {@code public static long} method taking the target's full Java signature (receiver first for instance methods) — is invoked
 * at runtime and its result charged against the per-context allocation limit before the call executes. The estimator is named
 * by its {@code class} (fully-qualified binary name; JVM {@code $} form for inner classes) and {@code method}, resolved at
 * allowlist load time through the annotated class's class loader so plugins may ship their own.
 *
 * @param estimatorClassName fully-qualified binary class name declaring the estimator
 * @param estimatorMethodName name of the estimator method
 */
public record AllocatesDynamicAnnotation(String estimatorClassName, String estimatorMethodName) {

    public static final String NAME = "allocates_dynamic";
}
