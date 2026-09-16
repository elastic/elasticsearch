/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.internal;

import org.elasticsearch.core.Booleans;
import org.elasticsearch.foreign.LibraryProvider;
import org.elasticsearch.foreign.Platform;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

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

    public static boolean isNativeLibSupported() {
        return Platform.current().equals(Platform.DARWIN_AARCH64)
            || Platform.current().equals(Platform.LINUX_AARCH64)
            || Platform.current().equals(Platform.LINUX_X64);
    }

    static boolean checkEnableSystemProperty() {
        return Booleans.parseBoolean(System.getProperty(ENABLE_SIMD_JSON_LIBRARY), true);
    }
}
