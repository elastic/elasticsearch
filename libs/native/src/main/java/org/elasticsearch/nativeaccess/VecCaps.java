/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.foreign.LoaderHelper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.lang.foreign.FunctionDescriptor;

import static java.lang.foreign.ValueLayout.JAVA_INT;
import static org.elasticsearch.foreign.LinkerHelper.downcallHandle;

/**
 * Native functions in the native simdvec library can have multiple implementations, one for each "capability level".
 * A capability level of "0" means that there is no native function for that platform.
 * Capability levels maps to the availability of advanced vector instructions sets for a platform. For example, for x64 we currently
 * define 3 capability levels:
 * <ol>
 *     <li>base, processor supports AVX2</li>
 *     <li>processor supports AVX-512 with VNNI and VPOPCNT</li>
 *     <li>processor supports AVX-512 with BF16</li>
 * </ol>
 * <p>
 * This class detects and holds the capability level for the current architecture.
 */
public final class VecCaps {
    private static final Logger logger = LogManager.getLogger(VecCaps.class);
    private static final int VEC_CAPS_OVERRIDE = getVecCapsOverride();

    /**
     * Effective level: raw vec_caps() clamped by es.vec_caps_override.
     */
    private static final int CAPS = vecCaps();

    /**
     * Try to get a vec_caps override value from the {@code es.vec_caps_override} system property.
     * This value is used to override the vector capabilities value returned by the native call
     * to {@code vec_caps}; if the override is defined and valid (>= 0), and it is less then the
     * one returned by {@code vec_caps}, the override is used to determine which functions to bind.
     * This can be used to force binding to functions from a lower tier (e.g. AVX2 on an AVX-512
     * capable processor), or to disable native functions completely (by passing 0).
     * Usage: {@code -Des.vec_caps_override=1}.
     * For benchmarks, add {@code --jvmArgsPrepend "--add-modules=jdk.incubator.vector -Des.vec_caps_override=..."}
     * to {@code --args}. Note: {@code --jvmArgsPrepend} on the CLI replaces the {@code @Fork} annotation's
     * {@code jvmArgsPrepend}, so {@code --add-modules=jdk.incubator.vector} must be included explicitly.
     *
     * @return the caps override value, or -1 if the property is not defined or invalid.
     */
    private static int getVecCapsOverride() {
        try {
            var capsOverrideString = System.getProperty("es.vec_caps_override", "-1");
            try {
                return Integer.parseInt(capsOverrideString);
            } catch (NumberFormatException e) {
                logger.warn("Invalid es.vec_caps_override value [{}]", capsOverrideString);
                return -1;
            }
        } catch (Throwable t) {
            logger.warn("Cannot read es.vec_caps_override value", t);
        }
        return -1;
    }

    private static int vecCaps() {
        LoaderHelper.loadLibrary("vec");
        var vecCaps$mh = downcallHandle("vec_caps", FunctionDescriptor.of(JAVA_INT));

        try {
            int vecCaps = (int) vecCaps$mh.invokeExact();
            final int finalVecCaps;
            if (VEC_CAPS_OVERRIDE >= 0) {
                finalVecCaps = Math.min(vecCaps, VEC_CAPS_OVERRIDE);
                logger.info("vec_caps={}; es.vec_caps_override={}; using [{}]", vecCaps, VEC_CAPS_OVERRIDE, finalVecCaps);
            } else {
                finalVecCaps = vecCaps;
                logger.info("vec_caps={}", finalVecCaps);
            }

            return finalVecCaps;
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    public static int caps() {
        return CAPS;
    }
}
