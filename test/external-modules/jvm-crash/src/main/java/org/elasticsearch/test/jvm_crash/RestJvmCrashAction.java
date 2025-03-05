/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.jvm_crash;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestJvmCrashAction implements RestHandler {

    // Turns out, it's actually quite hard to get the JVM to crash...
    private static Method FREE_MEMORY;
    private static Object UNSAFE;
    static {
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<?>) () -> {
                Class<?> unsafe = Class.forName("sun.misc.Unsafe");

                FREE_MEMORY = unsafe.getMethod("freeMemory", long.class);
                Field f = unsafe.getDeclaredField("theUnsafe");
                f.setAccessible(true);
                UNSAFE = f.get(null);
                return null;
            });
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    RestJvmCrashAction() {}

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_crash"));
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        // BIG BADDA BOOM
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<?>) () -> FREE_MEMORY.invoke(UNSAFE, 1L));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
