/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.internal;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.foreign.LibraryProvider;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Optional;

/**
 * Loads and holds the {@link SimdJsonLibrary} singleton for stage 1 native indexing.
 */
public final class SimdJsonNativeSupport {

    private static final Logger logger = LogManager.getLogger(SimdJsonNativeSupport.class);

    /** -Dorg.elasticsearch.simdjson.enableSimdJsonLibrary=false to disable. */
    static final String ENABLE_SIMD_JSON_LIBRARY = "org.elasticsearch.simdjson.enableSimdJsonLibrary";

    private static final SimdJsonLibrary LIBRARY = tryLoad();

    private SimdJsonNativeSupport() {}

    public static boolean isLoaded() {
        return LIBRARY != null;
    }

    public static SimdJsonLibrary library() {
        return LIBRARY;
    }

    private static SimdJsonLibrary tryLoad() {
        if (isNativeLibSupported() && checkEnableSystemProperty()) {
            try {
                SimdJsonLibrary lib = LibraryProvider.lookupLibrary(SimdJsonLibrary.class);
                if (lib != null) {
                    logger.info("Loaded simdjson native library");
                    return lib;
                }
            } catch (UnsatisfiedLinkError e) {
                logger.info("simdjson native library not available: {}", e.getMessage());
            } catch (Exception e) {
                logger.warn("Failed to instantiate simdjson native library", e);
            }
        }
        return null;
    }

    static boolean isNativeLibSupported() {
        return isMacOrLinuxAarch64() || isLinuxAmd64();
    }

    /**
     * Returns true iff the architecture is x64 (amd64) and the OS Linux (the OS we currently support for the native lib).
     */
    static boolean isLinuxAmd64() {
        String name = System.getProperty("os.name");
        return (name.startsWith("Linux")) && System.getProperty("os.arch").equals("amd64");
    }

    /** Returns true iff the OS is Mac or Linux, and the architecture is aarch64. */
    static boolean isMacOrLinuxAarch64() {
        String name = System.getProperty("os.name");
        return (name.startsWith("Mac") || name.startsWith("Linux")) && System.getProperty("os.arch").equals("aarch64");
    }

    @SuppressForbidden(
        reason = "TODO Deprecate any lenient usage of Boolean#parseBoolean https://github.com/elastic/elasticsearch/issues/128993"
    )
    static boolean checkEnableSystemProperty() {
        return Optional.ofNullable(System.getProperty(ENABLE_SIMD_JSON_LIBRARY)).map(Boolean::valueOf).orElse(Boolean.TRUE);
    }
}
