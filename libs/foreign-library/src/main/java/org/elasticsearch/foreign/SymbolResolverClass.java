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
 * Specifies a custom symbol resolver class for a {@link LibrarySpecification @LibrarySpecification} interface.
 * When present, the generated {@code $Impl} class calls the resolver's {@code resolve} method at class-init
 * time instead of the default {@link LinkerHelper#downcallHandle(String, java.lang.foreign.FunctionDescriptor,
 * java.lang.foreign.Linker.Option...) LinkerHelper.downcallHandle}.
 *
 * <p>The resolver class must declare a {@code public static} method with this exact signature:
 *
 * <pre>{@code
 * public static MethodHandle resolve(String functionName, FunctionDescriptor descriptor, Linker.Option... options)
 * }</pre>
 *
 * <p>This allows libraries to implement custom lookup strategies, e.g. fallback in case of missing symbols, name
 * mangling/unmangling, add/remove prefix/suffix etc.
 *
 * <p>Example:
 *
 * <pre>{@code
 * @LibrarySpecification(name = "my_native_lib")
 * @SymbolResolverClass(MySymbolResolver.class)
 * public interface VecLib {
 *     @Function("plain_op_name")
 *     int callOp(MemorySegment a, MemorySegment b, int length);
 * }
 * }</pre>
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.TYPE)
public @interface SymbolResolverClass {
    /** The class providing the custom symbol resolution logic. Defaults to {@link LinkerHelper}. */
    Class<?> value() default LinkerHelper.class;
}
