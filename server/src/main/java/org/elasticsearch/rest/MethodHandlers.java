/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.elasticsearch.core.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Encapsulate multiple handlers for the same path, allowing different handlers for different HTTP verbs.
 */
final class MethodHandlers {

    private final String path;
    private final Map<RestRequest.Method, RestHandler> methodHandlers;

    MethodHandlers(String path) {
        this.path = path;
        // by setting the loadFactor to 1, these maps are resized only when they *must* be, and the vast majority of these
        // maps contain only 1 or 2 entries anyway, so most of these maps are never resized at all and waste only 1 or 0
        // array references, while those few that contain 3 or 4 elements will have been resized just once and will still
        // waste only 1 or 0 array references
        this.methodHandlers = new HashMap<>(2, 1);
    }

    /**
     * Add a handler for an additional array of methods. Note that {@code MethodHandlers}
     * does not allow replacing the handler for an already existing method.
     */
    MethodHandlers addMethod(RestRequest.Method method, RestHandler handler) {
        RestHandler existing = methodHandlers.putIfAbsent(method, handler);
        if (existing != null) {
            throw new IllegalArgumentException("Cannot replace existing handler for [" + path + "] for method: " + method);
        }
        return this;
    }

    /**
     * Returns the handler for the given method or {@code null} if none exists.
     */
    @Nullable
    RestHandler getHandler(RestRequest.Method method) {
        return methodHandlers.get(method);
    }

    /**
     * Return a set of all valid HTTP methods for the particular path
     */
    Set<RestRequest.Method> getValidMethods() {
        return methodHandlers.keySet();
    }
}
