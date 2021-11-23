/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import jdk.incubator.foreign.Addressable;
import jdk.incubator.foreign.CLinker;
import jdk.incubator.foreign.FunctionDescriptor;
import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemoryAddress;
import jdk.incubator.foreign.MemoryLayout;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import jdk.incubator.foreign.SegmentAllocator;
import jdk.incubator.foreign.SymbolLookup;
import jdk.incubator.foreign.ValueLayout;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.util.Optional;

import static jdk.incubator.foreign.CLinker.C_INT;
import static jdk.incubator.foreign.CLinker.C_LONG_LONG;
import static jdk.incubator.foreign.CLinker.C_POINTER;

/**
 * java mapping to some libc functions
 */
final class PanamaCLibrary implements CLibrary {

    private static final Logger logger = LogManager.getLogger(PanamaCLibrary.class); // TODO: do some logging if things fail

    private static final PanamaCLibrary INSTANCE = instanceOrNull();

    private static PanamaCLibrary instanceOrNull() {
        return new PanamaCLibrary(); // TODO: what if class cannot be initialized?
    }

    static Optional<CLibrary> instance() {
        return Optional.ofNullable(INSTANCE);
    }

    private PanamaCLibrary() {}

    private static final CLinker LINKER = CLinker.getInstance();
    private static final ClassLoader LOADER = PanamaCLibrary.class.getClassLoader();
    private static final SymbolLookup LIBRARIES = CLinker.systemLookup();

    private static MethodHandle downcallHandle(SymbolLookup LOOKUP, String name, String desc, FunctionDescriptor fdesc) {
        return LOOKUP.lookup(name).map(addr -> {
            MethodType mt = MethodType.fromMethodDescriptorString(desc, LOADER);
            return LINKER.downcallHandle(addr, mt, fdesc);
        }).orElseThrow(() -> new UnsatisfiedLinkError("unresolved symbol: " + name));
    }

    // -- resource
    static final MethodHandle getrlimit$MH = downcallHandle(
        LIBRARIES,
        "getrlimit",
        "(ILjdk/incubator/foreign/MemoryAddress;)I",
        FunctionDescriptor.of(C_INT, C_INT, C_POINTER)
    );
    static final MethodHandle setrlimit$MH = downcallHandle(
        LIBRARIES,
        "setrlimit",
        "(ILjdk/incubator/foreign/MemoryAddress;)I",
        FunctionDescriptor.of(C_INT, C_INT, C_POINTER)
    );

    @Override
    public int getrlimit(int resource, Rlimit rlimit) {
        return getrlimit0(resource, ((PanamaRlimit) rlimit).segment.address());
    }

    private static int getrlimit0(int x0, Addressable x1) {
        try {
            return (int) getrlimit$MH.invokeExact(x0, x1.address());
        } catch (Throwable ex$) {
            throw new AssertionError("should not reach here", ex$);
        }
    }

    @Override
    public int setrlimit(int resource, Rlimit rlimit) {
        return setrlimit0(resource, ((PanamaRlimit) rlimit).segment.address());
    }

    private static int setrlimit0(int x0, Addressable x1) {
        try {
            return (int) setrlimit$MH.invokeExact(x0, x1.address());
        } catch (Throwable ex$) {
            throw new AssertionError("should not reach here", ex$);
        }
    }

    @Override
    public Rlimit newRlimit() {
        return new PanamaRlimit();
    }

    static class PanamaRlimit implements Rlimit {
        private final ResourceScope scope = ResourceScope.newConfinedScope();
        private final MemorySegment segment = rlimit.allocate(scope);

        @Override
        public long rlim_cur() {
            return rlimit.rlim_cur$get(segment);
        }

        @Override
        public long rlim_max() {
            return rlimit.rlim_max$get(segment);
        }

        @Override
        public void setrlim_cur(long value) {
            rlimit.rlim_cur$set(segment, value);
        }

        @Override
        public void setrlim_max(long value) {
            rlimit.rlim_max$set(segment, value);
        }

        @Override
        public void close() {
            scope.close();
        }
    }

    static class rlimit {
        static final MemoryLayout $struct$LAYOUT = MemoryLayout.structLayout(
            C_LONG_LONG.withName("rlim_cur"),
            C_LONG_LONG.withName("rlim_max")
        ).withName("rlimit");

        public static MemoryLayout $LAYOUT() {
            return $struct$LAYOUT;
        }

        public static long sizeof() {
            return $LAYOUT().byteSize();
        }

        public static MemorySegment allocate(SegmentAllocator allocator) {
            return allocator.allocate($LAYOUT());
        }

        public static MemorySegment allocate(ResourceScope scope) {
            return allocate(SegmentAllocator.ofScope(scope));
        }

        static final VarHandle rlim_cur$VH = $struct$LAYOUT.varHandle(long.class, MemoryLayout.PathElement.groupElement("rlim_cur"));

        public static long rlim_cur$get(MemorySegment seg) {
            return (long) rlim_cur$VH.get(seg);
        }

        public static void rlim_cur$set(MemorySegment seg, long x) {
            rlim_cur$VH.set(seg, x);
        }

        static final VarHandle rlim_max$VH = $struct$LAYOUT.varHandle(long.class, MemoryLayout.PathElement.groupElement("rlim_max"));

        public static long rlim_max$get(MemorySegment seg) {
            return (long) rlim_max$VH.get(seg);
        }

        public static void rlim_max$set(MemorySegment seg, long x) {
            rlim_max$VH.set(seg, x);
        }
    }

    // -- mlockall support
    static final MethodHandle mlockall$MH = downcallHandle(LIBRARIES, "mlockall", "(I)I", FunctionDescriptor.of(C_INT, C_INT));

    @Override
    public int mlockall(int x0) {
        try {
            return (int) mlockall$MH.invokeExact(x0);
        } catch (Throwable ex$) {
            throw new AssertionError("should not reach here", ex$);
        }
    }

    // -- error support
    static final MethodHandle __error$MH = downcallHandle(
        LIBRARIES,
        "__error",
        "()Ljdk/incubator/foreign/MemoryAddress;",
        FunctionDescriptor.of(C_POINTER)
    );

    private static ValueLayout errno_t = C_INT;

    private static MemoryAddress __error() {
        try {
            return (MemoryAddress) __error$MH.invokeExact();
        } catch (Throwable ex$) {
            throw new AssertionError("should not reach here", ex$);
        }
    }

    @Override
    public int getLastError() {
        return MemoryAccess.getIntAtOffset(MemorySegment.globalNativeSegment(), __error().toRawLongValue());
    }

    static final MethodHandle strerror$MH = downcallHandle(
        LIBRARIES,
        "strerror",
        "(I)Ljdk/incubator/foreign/MemoryAddress;",
        FunctionDescriptor.of(C_POINTER, C_INT)
    );

    @Override
    public String strerror(int errno) {
        return CLinker.toJavaString(_strerror(errno));
    }

    private static MemoryAddress _strerror(int x0) {
        try {
            return (MemoryAddress) strerror$MH.invokeExact(x0);
        } catch (Throwable ex$) {
            throw new AssertionError("should not reach here", ex$);
        }
    }

    // -- geteuid

    static final MethodHandle geteuid$MH = downcallHandle(LIBRARIES, "geteuid", "()I", FunctionDescriptor.of(C_INT));

    @Override
    public int geteuid() {
        try {
            return (int) geteuid$MH.invokeExact();
        } catch (Throwable ex$) {
            throw new AssertionError("should not reach here", ex$);
        }
    }
}
