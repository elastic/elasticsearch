/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.arrow;

import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.DefaultAllocationManagerOption;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * An Arrow memory allocation manager that always fails.
 * <p>
 * We don't actually use Arrow's memory manager as we stream dataframe buffers directly from ESQL blocks.
 * But Arrow won't initialize properly unless it has one (and requires either the arrow-memory-netty or arrow-memory-unsafe libraries).
 * It also does some fancy classpath scanning and calls to {@code setAccessible} which will be rejected by the security manager.
 * <p>
 * So we configure an allocation manager that will fail on any attempt to allocate memory.
 *
 * @see DefaultAllocationManagerOption
 */
public class AllocationManagerShim implements AllocationManager.Factory {

    private static final Logger logger = LogManager.getLogger(AllocationManagerShim.class);

    /**
     * Initialize the Arrow memory allocation manager shim.
     */
    @SuppressForbidden(reason = "Inject the default Arrow memory allocation manager")
    public static void init() {
        try {
            Class.forName("org.elasticsearch.test.ESTestCase");
            logger.info("We're in tests, not disabling Arrow memory manager so we can use a real runtime for testing");
        } catch (ClassNotFoundException notfound) {
            logger.debug("Disabling Arrow's allocation manager");
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                try {
                    Field field = DefaultAllocationManagerOption.class.getDeclaredField("DEFAULT_ALLOCATION_MANAGER_FACTORY");
                    field.setAccessible(true);
                    field.set(null, new AllocationManagerShim());
                } catch (Exception e) {
                    throw new AssertionError("Can't init Arrow", e);
                }
                return null;
            });
        }
    }

    @Override
    public AllocationManager create(BufferAllocator accountingAllocator, long size) {
        throw new UnsupportedOperationException("Arrow memory manager is disabled");
    }

    @Override
    public ArrowBuf empty() {
        throw new UnsupportedOperationException("Arrow memory manager is disabled");
    }
}
