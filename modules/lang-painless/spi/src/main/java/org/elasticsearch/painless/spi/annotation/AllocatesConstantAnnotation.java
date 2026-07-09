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
 * Marks an allowlisted constructor or method that allocates a fixed number of {@code bytes}, charged against the per-context
 * allocation limit before the call executes. The declared cost is the total heap allocation attributable to the call,
 * including transitive JDK-internal allocations. Use {@link AllocatesDynamicAnnotation} when the size is argument-dependent.
 * {@code 0} is a valid no-op ("audited: does not allocate") and emits no pre-check; negatives are rejected at allowlist load time.
 */
public record AllocatesConstantAnnotation(long bytes) {

    public static final String NAME = "allocates_constant";
}
