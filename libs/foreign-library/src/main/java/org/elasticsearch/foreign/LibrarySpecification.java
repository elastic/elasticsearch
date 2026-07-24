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
 * Marks an interface as a native library binding. The annotation processor generates an
 * implementation of the interface backed by native (FFM) method handles, and registers it with
 * {@link LibraryProvider} so it can be looked up at runtime.
 *
 * <p>The annotated type must be an interface. Every abstract method must be annotated with
 * either {@link Function @Function} (a native symbol binding) or {@link StructFactory
 * @StructFactory} (constructs a nested {@link StructSpecification @StructSpecification} struct);
 * the processor reports a compile error otherwise. The interface may also enclose
 * {@code @StructSpecification} records and interfaces that describe C struct layouts referenced
 * by its methods.
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
 *
 * <p>To restrict the library to specific platforms, list the platforms where it is unavailable:
 *
 * <pre>{@code
 * @LibrarySpecification(name = "vec", unavailableOn = { Platform.WINDOWS_X64 })
 * public interface VectorLibrary { ... }
 * }</pre>
 *
 * <p>When the current platform matches an entry in {@link #unavailableOn()}, {@link LibraryProvider#lookupLibrary(Class)}
 * returns {@code null} for this library without attempting any native load.
 *
 * <p>To customize how native symbols are resolved specify a custom {@link SymbolResolver} via
 * {@link #symbolResolver()}:
 *
 * <pre>{@code
 * public class PrefixResolver implements SymbolResolver {
 *     public MemorySegment resolve(String symbolName, SymbolLookup lookup) {
 *         return lookup.find("mylib_" + symbolName).orElseThrow(
 *             () -> new UnsatisfiedLinkError(symbolName));
 *     }
 * }
 *
 * @LibrarySpecification(name = "mylib", symbolResolver = PrefixResolver.class)
 * public interface MyLib {
 *     @Function("compress") // Resolves to "mylib_compress"
 *     int compress(MemorySegment src, int len);
 * }
 * }</pre>
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.TYPE)
public @interface LibrarySpecification {
    /** Native library to load; empty means system/default lookup only. */
    String name() default "";

    /**
     * Platforms where this library is not available. When the current platform matches any entry,
     * {@link LibraryProvider#lookupLibrary(Class)} returns {@code null} for this library without
     * attempting a native load. An empty array (the default) means the library is available on
     * all platforms.
     */
    Platform[] unavailableOn() default {};

    /**
     * Custom symbol resolver for this library. The resolver maps symbol names from
     * {@link Function @Function} annotations to native function pointers at class-init time.
     * Defaults to {@link DefaultSymbolResolver}, which looks up symbols by their exact name.
     */
    Class<? extends SymbolResolver> symbolResolver() default DefaultSymbolResolver.class;
}
