/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static java.lang.foreign.MemoryLayout.PathElement.groupElement;

/**
 * Utility methods for calling into the native linker.
 */
public class LinkerHelper {
    private static final Linker LINKER = Linker.nativeLinker();
    private static final boolean IS_WINDOWS = Platform.current() == Platform.WINDOWS_X64;
    private static final SymbolLookup SYMBOL_LOOKUP;

    static {
        // We first check the loader lookup, which contains libs loaded by System.load and System.loadLibrary.
        // If the symbol isn't found there, we fall back to the default lookup, which is "common libraries" for
        // the platform, typically eg libc on POSIX. On Windows the default lookup covers ucrtbase.dll and
        // ntdll.dll but not kernel32.dll, so we add an explicit kernel32 lookup on that platform.
        SymbolLookup loaderLookup = SymbolLookup.loaderLookup();
        if (IS_WINDOWS) {
            SymbolLookup kernel32 = kernel32Lookup();
            SYMBOL_LOOKUP = name -> loaderLookup.find(name).or(() -> LINKER.defaultLookup().find(name)).or(() -> kernel32.find(name));
        } else {
            SYMBOL_LOOKUP = name -> loaderLookup.find(name).or(() -> LINKER.defaultLookup().find(name));
        }
    }

    @SuppressWarnings("restricted") // SymbolLookup.libraryLookup is a restricted native-access method; kernel32 is an OS library.
    private static SymbolLookup kernel32Lookup() {
        return SymbolLookup.libraryLookup("kernel32.dll", Arena.global());
    }

    public static SymbolLookup defaultLookup() {
        return SYMBOL_LOOKUP;
    }

    public static MemorySegment functionAddress(String function) {
        return SYMBOL_LOOKUP.find(function).orElseThrow(() -> new LinkageError("Native function " + function + " could not be found"));
    }

    public static MemorySegment functionAddressOrNull(String function) {
        return SYMBOL_LOOKUP.find(function).orElse(null);
    }

    @SuppressWarnings("restricted") // Linker.downcallHandle is a restricted native-access method; this helper exists to call it.
    public static MethodHandle downcallHandle(String function, FunctionDescriptor functionDescriptor, Linker.Option... options) {
        return LINKER.downcallHandle(functionAddress(function), functionDescriptor, options);
    }

    @SuppressWarnings("restricted") // Linker.downcallHandle is a restricted native-access method; this helper exists to call it.
    public static MethodHandle downcallHandle(
        MemorySegment functionAddress,
        FunctionDescriptor functionDescriptor,
        Linker.Option... options
    ) {
        return LINKER.downcallHandle(functionAddress, functionDescriptor, options);
    }

    /**
     * Shared capture-state buffer for {@code @CaptureSystemError} calls. A single segment is enough on
     * any platform: {@link Linker.Option#captureStateLayout()} spans every capture state the platform
     * supports, and only one system-error mechanism ({@code errno} on POSIX, {@code GetLastError} on
     * Windows) is ever captured per platform.
     */
    private static final MemorySegment SYSTEM_ERROR_STATE = Arena.ofAuto().allocate(Linker.Option.captureStateLayout());

    // errno is a valid capture-state group element on every platform (including the Windows CRT), so
    // its VarHandle can resolve eagerly.
    private static final VarHandle ERRNO_VH = MemoryLayoutVarHandles.varHandleWithoutOffset(
        Linker.Option.captureStateLayout(),
        groupElement("errno")
    );

    // "GetLastError" is only a valid captureStateLayout() group element on Windows; resolving the
    // VarHandle eagerly as a LinkerHelper field would fail LinkerHelper's own class-init on every
    // other platform and permanently poison the class for unrelated callers. Holding it in a nested
    // class defers that resolution until the Windows-only read path in systemError() touches it.
    private static final class LastErrorHolder {
        private static final VarHandle LAST_ERROR_VH = MemoryLayoutVarHandles.varHandleWithoutOffset(
            Linker.Option.captureStateLayout(),
            groupElement("GetLastError")
        );
    }

    /**
     * Returns the operating system's last-error value captured by the most recent
     * {@code @CaptureSystemError} call on the current thread — POSIX {@code errno}, or Win32
     * {@code GetLastError} on Windows.
     *
     * @see <a href="https://man7.org/linux/man-pages/man3/errno.3.html">errno manpage</a>
     * @see <a href="https://learn.microsoft.com/en-us/windows/win32/api/errhandlingapi/nf-errhandlingapi-getlasterror">GetLastError docs</a>
     */
    public static int systemError() {
        VarHandle vh = IS_WINDOWS ? LastErrorHolder.LAST_ERROR_VH : ERRNO_VH;
        return (int) vh.get(SYSTEM_ERROR_STATE);
    }

    /** Returns the shared system-error capture buffer. Used by generated {@code $Impl} classes. */
    public static MemorySegment systemErrorState() {
        return SYSTEM_ERROR_STATE;
    }

    /**
     * Builds a downcall handle that captures the platform's system-error value ({@code errno} on
     * POSIX, {@code GetLastError} on Windows) into the shared buffer, binding that buffer as the
     * leading argument.
     */
    @SuppressWarnings("restricted") // Linker.downcallHandle is a restricted native-access method; this helper exists to call it.
    public static MethodHandle downcallHandleWithSystemError(
        String function,
        FunctionDescriptor functionDescriptor,
        Linker.Option... options
    ) {
        Linker.Option[] allOptions = new Linker.Option[options.length + 1];
        allOptions[0] = Linker.Option.captureCallState(IS_WINDOWS ? "GetLastError" : "errno");
        System.arraycopy(options, 0, allOptions, 1, options.length);
        MethodHandle originalHandle = LINKER.downcallHandle(functionAddress(function), functionDescriptor, allOptions);
        return MethodHandles.insertArguments(originalHandle, 0, SYSTEM_ERROR_STATE);
    }

    public static MethodHandle upcallHandle(
        MethodHandles.Lookup lookup,
        Class<?> clazz,
        String methodName,
        FunctionDescriptor functionDescriptor
    ) {
        try {
            return lookup.findVirtual(clazz, methodName, functionDescriptor.toMethodType());
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @SuppressWarnings("restricted") // Linker.upcallStub is a restricted native-access method; this helper exists to call it.
    public static <T> MemorySegment upcallStub(MethodHandle mh, T instance, FunctionDescriptor functionDescriptor, Arena arena) {
        try {
            mh = mh.bindTo(instance);
            return LINKER.upcallStub(mh, functionDescriptor, arena);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }
}
