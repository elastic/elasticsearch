/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.lib;

import org.elasticsearch.foreign.MethodHandleResolver;
import org.elasticsearch.foreign.ResolvedSymbol;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

import static java.lang.foreign.ValueLayout.JAVA_INT;

/**
 * Links {@code __fxstat} (the {@code fstat64} fallback resolved by {@link PosixSymbolResolver})
 * by prepending the C {@code int vers} argument to the descriptor and pre-binding it to the
 * arch-specific stat structure version. All other symbols link normally.
 */
public final class PosixMethodHandleResolver implements MethodHandleResolver {

    private static final int STAT_VER = System.getProperty("os.arch").equals("aarch64") ? 0 : 1;

    @Override
    public MethodHandle resolve(ResolvedSymbol symbol, FunctionDescriptor descriptor, Linker linker, Linker.Option... options) {
        if ("__fxstat".equals(symbol.name())) {
            List<MemoryLayout> argLayouts = new ArrayList<>();
            argLayouts.add(JAVA_INT); // vers
            argLayouts.addAll(descriptor.argumentLayouts());
            FunctionDescriptor extDesc = FunctionDescriptor.of(
                descriptor.returnLayout().orElseThrow(() -> new AssertionError("__fxstat must have a return layout")),
                argLayouts.toArray(MemoryLayout[]::new)
            );
            MethodHandle handle = linker.downcallHandle(symbol.address(), extDesc, options);
            // With @CaptureSystemError, arg 0 is the errno-state segment; arg 1 is vers.
            return MethodHandles.insertArguments(handle, 1, STAT_VER);
        }
        return linker.downcallHandle(symbol.address(), descriptor, options);
    }
}
