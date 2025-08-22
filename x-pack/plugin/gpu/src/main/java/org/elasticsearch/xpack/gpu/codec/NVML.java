/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu.codec;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.GroupLayout;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

import static java.lang.foreign.MemoryLayout.PathElement.groupElement;

class NVML {

    private static final SymbolLookup SYMBOL_LOOKUP = SymbolLookup.libraryLookup("libnvidia-ml.so.1", Arena.ofAuto())
        .or(SymbolLookup.loaderLookup())
        .or(Linker.nativeLinker().defaultLookup());

    public static final int NVML_SUCCESS = 0;

    /**
     * nvmlReturn_t nvmlInit_v2 ( void )
     */
    static final MethodHandle nvmlInit_v2$mh = Linker.nativeLinker().downcallHandle(
        findOrThrow("nvmlInit_v2"),
        FunctionDescriptor.of(ValueLayout.JAVA_INT)
    );

    /**
     * nvmlReturn_t nvmlShutdown ( void )
     */
    static final MethodHandle nvmlShutdown$mh = Linker.nativeLinker().downcallHandle(
        findOrThrow("nvmlShutdown"),
        FunctionDescriptor.of(ValueLayout.JAVA_INT)
    );

    /**
     * const DECLDIR char* nvmlErrorString ( nvmlReturn_t result )
     */
    static final MethodHandle nvmlErrorString$mh = Linker.nativeLinker().downcallHandle(
        findOrThrow("nvmlErrorString"),
        FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_INT)
    );

    /**
     * nvmlReturn_t nvmlDeviceGetHandleByIndex_v2 ( unsigned int  index, nvmlDevice_t* device )
     */
    static final MethodHandle nvmlDeviceGetHandleByIndex_v2$mh = Linker.nativeLinker().downcallHandle(
        findOrThrow("nvmlDeviceGetHandleByIndex_v2"),
        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.ADDRESS)
    );

    /**
     * nvmlReturn_t nvmlDeviceGetUtilizationRates ( nvmlDevice_t device, nvmlUtilization_t* utilization )
     */
    static final MethodHandle nvmlDeviceGetUtilizationRates$mh = Linker.nativeLinker().downcallHandle(
        findOrThrow("nvmlDeviceGetUtilizationRates"),
        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS)
    );

    public static class nvmlUtilization_t {

        nvmlUtilization_t() {
            // Should not be called directly
        }

        private static final GroupLayout $LAYOUT = MemoryLayout.structLayout(
            ValueLayout.JAVA_INT.withName("gpu"),
            ValueLayout.JAVA_INT.withName("memory")
        );

        /**
         * The layout of this struct
         */
        public static GroupLayout layout() {
            return $LAYOUT;
        }

        private static final ValueLayout.OfInt gpu$LAYOUT = (ValueLayout.OfInt)$LAYOUT.select(groupElement("gpu"));

        /**
         * Getter for field: gpu
         * Percent of time over the past sample period during which one or more kernels was executing on the GPU.
         */
        public static int gpu(MemorySegment struct) {
            return struct.get(gpu$LAYOUT, 0);
        }

        private static final ValueLayout.OfInt memory$LAYOUT = (ValueLayout.OfInt)$LAYOUT.select(groupElement("memory"));

        /**
         * Getter for field: memory
         * Percent of time over the past sample period during which global (device) memory was being read or written.
         */
        public static int memory(MemorySegment struct) {
            return struct.get(memory$LAYOUT, 4);
        }
    }

    private static MemorySegment findOrThrow(String symbol) {
        return SYMBOL_LOOKUP.find(symbol).orElseThrow(() -> new UnsatisfiedLinkError("unresolved symbol: " + symbol));
    }

    public static void nvmlInit_v2() {
        int res;
        try {
            res = (int)nvmlInit_v2$mh.invokeExact();
        } catch (Throwable ex$) {
            throw new AssertionError("should not reach here", ex$);
        }
        if (res != NVML_SUCCESS) {
            throw buildException(res);
        }
    }

    public static void nvmlShutdown() {
        int res;
        try {
            res = (int)nvmlShutdown$mh.invokeExact();
        } catch (Throwable ex$) {
            throw new AssertionError("should not reach here", ex$);
        }
        if (res != NVML_SUCCESS) {
            throw buildException(res);
        }
    }

    public static MemorySegment nvmlDeviceGetHandleByIndex_v2(int index) {
        int res;
        MemorySegment nvmlDevice;
        try (var localArena = Arena.ofConfined()) {
            MemorySegment devicePtr = localArena.allocate(ValueLayout.ADDRESS);
            res = (int)nvmlDeviceGetHandleByIndex_v2$mh.invokeExact(index,devicePtr);
            nvmlDevice = devicePtr.get(ValueLayout.ADDRESS, 0);
        } catch (Throwable ex$) {
            throw new AssertionError("should not reach here", ex$);
        }
        if (res != NVML_SUCCESS) {
            throw buildException(res);
        }
        return nvmlDevice;
    }

    public static void nvmlDeviceGetUtilizationRates(MemorySegment nvmlDevice, MemorySegment nvmlUtilizationPtr) {
        int res;
        try  {
            res = (int)nvmlDeviceGetUtilizationRates$mh.invokeExact(nvmlDevice, nvmlUtilizationPtr);
        } catch (Throwable ex$) {
            throw new AssertionError("should not reach here", ex$);
        }
        if (res != NVML_SUCCESS) {
            throw buildException(res);
        }
    }

    private static RuntimeException buildException(int res) {
        return new RuntimeException("Error invoking NVML: " + res + "[" + nvmlErrorString(res) + "]");
    }

    public static String nvmlErrorString(int result) {
        try {
            var seg = (MemorySegment) nvmlErrorString$mh.invokeExact(result);
            if (seg.equals(MemorySegment.NULL)) {
                return "no last error text";
            }
            return MemorySegmentUtil.getString(seg,0);
        } catch (Throwable ex$) {
            throw new AssertionError("should not reach here", ex$);
        }
    }
}
