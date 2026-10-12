/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.foreign.MemorySegment;

/**
 * Marks a {@code @FunctionalInterface} as a native callback (upcall) type. Any parameter of a
 * {@link LibrarySpecification @LibrarySpecification} method whose declared type carries this
 * annotation is marshaled by installing a global-lifetime FFM upcall stub for the callback before
 * the native call is made. The stub lives for the remainder of the JVM's life, which is correct
 * for callbacks that are registered persistently (for example, a signal or console-control handler).
 *
 * <p>The single abstract method's return type and every parameter type must be a supported
 * scalar type ({@code int}, {@code long}, {@code short}, {@code byte}, {@code boolean},
 * {@code float}, or {@code double}) or {@link MemorySegment}; {@code void} is allowed as a return
 * type. Nested upcalls, {@code String}, and struct ({@code Addressable}) types are not supported
 * in a callback signature.
 *
 * <p>{@code @Upcall} is source-retention, so the processor can only see it when the annotated
 * interface is compiled in the same module as the {@code @LibrarySpecification} that references
 * it. This is always true in practice, since callback interfaces are declared alongside the
 * bindings that use them.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.TYPE)
public @interface Upcall {
}
