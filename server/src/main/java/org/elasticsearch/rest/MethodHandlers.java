/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.elasticsearch.core.RestApiVersion;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Encapsulate multiple handlers for the same path, allowing different handlers for different HTTP verbs and versions.
 */
final class MethodHandlers {

    private final String path;
    private final Map<RestRequest.Method, Map<RestApiVersion, RestHandler>> methodHandlers;

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
    MethodHandlers addMethod(RestRequest.Method method, RestApiVersion version, RestHandler handler) {
        RestHandler existing = methodHandlers
            // same sizing notes as 'methodHandlers' above, except that having a size here that's more than 1 is vanishingly
            // rare, so an initialCapacity of 1 with a loadFactor of 1 is perfect
            .computeIfAbsent(method, k -> new HashMap<>(1, 1))
            .putIfAbsent(version, handler);
        if (existing != null) {
            throw new IllegalArgumentException("Cannot replace existing handler for [" + path + "] for method: " + method);
        }
        return this;
    }

    /**
     * Returns the handler for the given method and version.
     *
     * If a handler for given version do not exist, a handler for Version.CURRENT will be returned.
     * The reasoning behind is that in a minor a new API could be added passively, therefore new APIs are compatible
     * (as opposed to non-compatible/breaking)
     * or {@code null} if none exists.
     */
    RestHandler getHandler(RestRequest.Method method, RestApiVersion version) {
        Map<RestApiVersion, RestHandler> versionToHandlers = methodHandlers.get(method);
        if (versionToHandlers == null) {
            return null; //method not found
        }
        final RestHandler handler = versionToHandlers.get(version);
        return handler == null ? versionToHandlers.get(RestApiVersion.current()) : handler;
    }

    /**
     * Return a set of all valid HTTP methods for the particular path
     */
    Set<RestRequest.Method> getValidMethods() {
        return methodHandlers.keySet();
    }
}
