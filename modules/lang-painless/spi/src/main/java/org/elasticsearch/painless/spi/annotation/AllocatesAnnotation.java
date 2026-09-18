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
 * Marks an allowlisted method/constructor that allocates, charged against the per-context limit before the call. The size is
 * always produced by an <i>estimator</i> — a {@code public static long} method taking the target's Java signature (receiver
 * first for instance methods) — named by {@code class} and {@code method} and resolved at allowlist load time through the
 * annotated class's class loader (so plugins may ship their own). A fixed cost is just an estimator that ignores its arguments
 * and returns a constant.
 *
 * @param estimatorClassName estimator's fully-qualified binary class name
 * @param estimatorMethodName estimator method name
 */
public record AllocatesAnnotation(String estimatorClassName, String estimatorMethodName) {

    public static final String NAME = "allocates";
}
