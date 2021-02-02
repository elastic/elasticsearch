/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest;

import org.elasticsearch.Version;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Encapsulate multiple handlers for the same path, allowing different handlers for different HTTP verbs and versions.
 */
final class MethodHandlers {

    private final String path;
    private final Map<RestRequest.Method, Map<Version, RestHandler>> methodHandlers;

    MethodHandlers(String path, RestHandler handler, RestRequest.Method... methods) {
        this.path = path;
        this.methodHandlers = new HashMap<>(methods.length);
        for (RestRequest.Method method : methods) {
            methodHandlers.computeIfAbsent(method, k -> new HashMap<>())
                .put(handler.compatibleWithVersion(), handler);
        }
    }

    /**
     * Add a handler for an additional array of methods. Note that {@code MethodHandlers}
     * does not allow replacing the handler for an already existing method.
     */
    MethodHandlers addMethods(RestHandler handler, RestRequest.Method... methods) {
        for (RestRequest.Method method : methods) {
            RestHandler existing = methodHandlers.computeIfAbsent(method, k -> new HashMap<>())
                .putIfAbsent(handler.compatibleWithVersion(), handler);
            if (existing != null) {
                throw new IllegalArgumentException("Cannot replace existing handler for [" + path + "] for method: " + method);
            }
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
    RestHandler getHandler(RestRequest.Method method, Version version) {
        Map<Version, RestHandler> versionToHandlers = methodHandlers.get(method);
        if (versionToHandlers == null) {
            return null; //method not found
        }
        final RestHandler handler = versionToHandlers.get(version);
        return handler == null ? versionToHandlers.get(Version.CURRENT) : handler;

    }

    /**
     * Return a set of all valid HTTP methods for the particular path
     */
    Set<RestRequest.Method> getValidMethods() {
        return methodHandlers.keySet();
    }
}
