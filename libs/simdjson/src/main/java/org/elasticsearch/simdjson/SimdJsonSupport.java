/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson;

import org.elasticsearch.simdjson.internal.SimdJsonNativeSupport;
import org.elasticsearch.simdjson.internal.parsers.SimdJsonVectorSupport;

import java.util.Optional;

/**
 * Entry point for simdjson availability checks on the exported API.
 *
 * <p>The static initializer adds the module read edge to {@code jdk.incubator.vector},
 * initializes vector support, and triggers native library loading. Call
 * {@link #isSupported()} before constructing a {@link SimdJsonParser}.
 */
public final class SimdJsonSupport {

    static {
        Optional.ofNullable(SimdJsonSupport.class.getModule().getLayer())
            .orElse(ModuleLayer.boot())
            .findModule("jdk.incubator.vector")
            .ifPresent(vec -> {
                SimdJsonSupport.class.getModule().addReads(vec);
                SimdJsonVectorSupport.init();
            });
        SimdJsonNativeSupport.isLoaded();
    }

    private SimdJsonSupport() {}

    /**
     * Returns {@code true} if simdjson is fully operational: the native C++ library is
     * loaded and the incubating vector API is available. This is the single availability
     * check that callers should use before constructing a {@link SimdJsonParser}.
     */
    public static boolean isSupported() {
        return SimdJsonNativeSupport.isLoaded() && SimdJsonVectorSupport.isAvailable();
    }
}
