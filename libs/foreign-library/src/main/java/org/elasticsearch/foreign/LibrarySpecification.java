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

/**
 * Marks an interface as a native library binding. The annotation processor generates a
 * {@code $Impl} class implementing the interface with FFM-based method handles, plus a
 * {@code $Provider} class that exposes it through {@link LibraryProvider}.
 *
 * <p>The annotated type must be an interface. Every method on the interface must be annotated
 * with {@link Function @Function} naming a C symbol; the processor reports a compile error
 * otherwise.
 *
 * <p>Example binding the system zlib compression library:
 *
 * <pre>{@code
 * @LibrarySpecification(name = "z")
 * public interface Zlib {
 *
 *     @Function("compressBound")
 *     long compressBound(long sourceLen);
 *
 *     @Function("compress")
 *     int compress(MemorySegment dest, MemorySegment destLen, MemorySegment source, long sourceLen);
 * }
 * }</pre>
 *
 * Look up the implementation via {@link LibraryProvider#lookupLibrary(Class)}:
 *
 * <pre>{@code
 * Zlib zlib = LibraryProvider.lookupLibrary(Zlib.class);
 * long bound = zlib.compressBound(srcLen);
 * }</pre>
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.TYPE)
public @interface LibrarySpecification {
    /** Native library to load; empty means system/default lookup only. */
    String name() default "";
}
