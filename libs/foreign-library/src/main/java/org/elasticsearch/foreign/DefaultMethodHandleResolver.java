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
 * Default method handle resolver that calls {@link Linker#downcallHandle} directly using the
 * symbol address returned by {@link SymbolResolver}. Used when no custom
 * {@link MethodHandleResolver} is specified on a {@link LibrarySpecification @LibrarySpecification}.
 */
public final class DefaultMethodHandleResolver implements MethodHandleResolver {

    @Override
    public MethodHandle resolve(ResolvedSymbol symbol, FunctionDescriptor descriptor, Linker linker, Linker.Option... options) {
        return linker.downcallHandle(symbol.address(), descriptor, options);
    }
}
