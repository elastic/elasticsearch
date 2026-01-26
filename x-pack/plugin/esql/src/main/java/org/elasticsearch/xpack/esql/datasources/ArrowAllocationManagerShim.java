/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.DefaultAllocationManagerOption;
import org.apache.arrow.memory.unsafe.UnsafeAllocationManager;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Configures Arrow to use the unsafe memory allocator.
 * <p>
 * Arrow's default allocation manager tries to load the netty allocator first, which has
 * version incompatibility issues with ES's internal Netty. This shim injects the unsafe
 * allocator factory before Arrow tries to load the default one.
 * <p>
 * This must be called before any Arrow memory allocation classes are used.
 *
 * @see DefaultAllocationManagerOption
 */
public final class ArrowAllocationManagerShim {

    private static final Logger logger = LogManager.getLogger(ArrowAllocationManagerShim.class);
    private static volatile boolean initialized = false;

    private ArrowAllocationManagerShim() {
        // Utility class - no instantiation
    }

    /**
     * Initialize Arrow to use the unsafe memory allocator.
     * This method is idempotent - it can be called multiple times safely.
     */
    @SuppressForbidden(reason = "Inject the Arrow memory allocation manager to use unsafe allocator")
    public static synchronized void init() {
        if (initialized) {
            return;
        }

        logger.debug("Configuring Arrow to use unsafe memory allocator");
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            try {
                Field field = DefaultAllocationManagerOption.class.getDeclaredField("DEFAULT_ALLOCATION_MANAGER_FACTORY");
                field.setAccessible(true);
                field.set(null, UnsafeAllocationManager.FACTORY);
                logger.info("Arrow configured to use UnsafeAllocationManager");
            } catch (Exception e) {
                logger.error("Failed to configure Arrow allocation manager", e);
                throw new AssertionError("Can't configure Arrow allocation manager", e);
            }
            return null;
        });

        initialized = true;
    }
}
