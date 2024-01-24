/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.arrow.shim;

import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.DefaultAllocationManagerOption;
import org.elasticsearch.logging.LogManager;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * We don't actually <strong>use</strong> Arrow's memory manager, but , arrow
 * won't initialize properly unless we configure one. We configure an "empty"
 * one here.
 */
public class Shim implements AllocationManager.Factory {
    /**
     * Initialize the Arrow shim. Arrow does some interesting reflection stuff on
     * initialization. We can avoid it if we
     */
    public static void init() {
        try {
            Class.forName("org.elasticsearch.test.ESTestCase");
            LogManager.getLogger(Shim.class)
                .info("we're in tests, disabling the arrow shim so we can use a real apache arrow runtime for testing");
        } catch (ClassNotFoundException notfound) {
            LogManager.getLogger(Shim.class).debug("shimming arrow's allocation manager");
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                try {
                    Field field = DefaultAllocationManagerOption.class.getDeclaredField("DEFAULT_ALLOCATION_MANAGER_FACTORY");
                    field.setAccessible(true);
                    field.set(null, new Shim());
                } catch (Exception e) {
                    throw new AssertionError("can't init arrow", e);
                }
                return null;
            });
        }
    }

    @Override
    public AllocationManager create(BufferAllocator accountingAllocator, long size) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ArrowBuf empty() {
        throw new UnsupportedOperationException();
    }
}
