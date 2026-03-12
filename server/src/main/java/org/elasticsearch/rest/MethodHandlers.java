/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.http.HttpRouteStats;
import org.elasticsearch.http.HttpRouteStatsTracker;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

/**
 * Encapsulate multiple handlers for the same path, allowing different handlers for different HTTP verbs and versions.
 */
public final class MethodHandlers {

    private final String path;
    private final Map<RestRequest.Method, Map<RestApiVersion, RestHandler>> methodHandlers;

    @SuppressWarnings("unused") // only accessed via #STATS_TRACKER_HANDLE, lazy initialized because instances consume non-trivial heap
    private HttpRouteStatsTracker statsTracker;

    private static final VarHandle STATS_TRACKER_HANDLE;

    static {
        try {
            STATS_TRACKER_HANDLE = MethodHandles.lookup().findVarHandle(MethodHandlers.class, "statsTracker", HttpRouteStatsTracker.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    MethodHandlers(String path) {
        this.path = path;
        this.methodHandlers = new EnumMap<>(RestRequest.Method.class);
    }

    public String getPath() {
        return path;
    }

    /**
     * Add a handler for an additional array of methods. Note that {@code MethodHandlers}
     * does not allow replacing the handler for an already existing method.
     */
    MethodHandlers addMethod(RestRequest.Method method, RestApiVersion version, RestHandler handler) {
        RestHandler existing = methodHandlers.computeIfAbsent(method, k -> new EnumMap<>(RestApiVersion.class))
            .putIfAbsent(version, handler);
        if (existing != null) {
            throw new IllegalArgumentException("Cannot replace existing handler for [" + path + "] for method: " + method);
        }
        return this;
    }

    /**
     * Returns the handler for the given method and version.
     *
     * If a handler for given version do not exist, a handler for RestApiVersion.current() will be returned.
     * The reasoning behind is that in a minor a new API could be added passively, therefore new APIs are compatible
     * (as opposed to non-compatible/breaking)
     * or {@code null} if none exists.
     */
    RestHandler getHandler(RestRequest.Method method, RestApiVersion version) {
        Map<RestApiVersion, RestHandler> versionToHandlers = methodHandlers.get(method);
        if (versionToHandlers == null) {
            return null; // method not found
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

    public HttpRouteStats getStats() {
        var tracker = existingStatsTracker();
        if (tracker == null) {
            return HttpRouteStats.EMPTY;
        }
        return tracker.getStats();
    }

    public HttpRouteStatsTracker statsTracker() {
        var tracker = existingStatsTracker();
        if (tracker == null) {
            var newTracker = new HttpRouteStatsTracker();
            if ((tracker = (HttpRouteStatsTracker) STATS_TRACKER_HANDLE.compareAndExchange(this, null, newTracker)) == null) {
                tracker = newTracker;
            }
        }
        return tracker;
    }

    private HttpRouteStatsTracker existingStatsTracker() {
        return (HttpRouteStatsTracker) STATS_TRACKER_HANDLE.getAcquire(this);
    }
}
