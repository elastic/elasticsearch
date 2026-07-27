/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.invoke.MethodHandle;

/**
 * Produces the final {@link MethodHandle} for a native binding from a
 * previously-resolved symbol. Custom implementations can adjust the
 * descriptor (for example prepending an argument bound via
 * {@code MethodHandles.insertArguments}) based on the actual symbol name
 * chosen by the {@link SymbolResolver}.
 *
 * <p>Implementing classes must have a public no-arg constructor.
 */
@FunctionalInterface
public interface MethodHandleResolver {
    /**
     * Produces a {@link MethodHandle} for the given resolved symbol.
     *
     * @param symbol     the resolved native symbol (name and address)
     * @param descriptor the Java-side function descriptor for the binding
     * @param linker     the native linker to use for creating downcall handles
     * @param options    additional linker options (e.g., errno capture)
     * @return the method handle for calling the native function (must not be null)
     */
    MethodHandle resolve(ResolvedSymbol symbol, FunctionDescriptor descriptor, Linker linker, Linker.Option... options);
}
