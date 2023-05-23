/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.usage;

import org.elasticsearch.action.admin.cluster.node.usage.NodeUsage;
import org.elasticsearch.rest.BaseRestHandler;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * A service to monitor usage of Elasticsearch features.
 */
public class UsageService {

    private final Map<String, BaseRestHandler> handlers;
    private final SearchUsageHolder searchUsageHolder;

    public UsageService() {
        this.handlers = new HashMap<>();
        this.searchUsageHolder = new SearchUsageHolder();
    }

    /**
     * Add a REST handler to this service.
     *
     * @param handler the {@link BaseRestHandler} to add to the usage service.
     */
    public void addRestHandler(BaseRestHandler handler) {
        Objects.requireNonNull(handler);
        if (handler.getName() == null) {
            throw new IllegalArgumentException("handler of type [" + handler.getClass().getName() + "] does not have a name");
        }
        final BaseRestHandler maybeHandler = handlers.put(handler.getName(), handler);
        /*
         * Handlers will be registered multiple times, once for each route that the handler handles. This means that we will see handlers
         * multiple times, so we do not have a conflict if we are seeing the same instance multiple times. So, we only reject if a handler
         * with the same name was registered before, and it is not the same instance as before.
         */
        if (maybeHandler != null && maybeHandler != handler) {
            final String message = String.format(
                Locale.ROOT,
                "handler of type [%s] conflicts with handler of type [%s] as they both have the same name [%s]",
                handler.getClass().getName(),
                maybeHandler.getClass().getName(),
                handler.getName()
            );
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Get the current usage statistics for this node.
     *
     * @return the {@link NodeUsage} representing the usage statistics for this
     * node
     */
    public Map<String, Long> getRestUsageStats() {
        Map<String, Long> restUsageMap;
        restUsageMap = new HashMap<>();
        handlers.values().forEach(handler -> {
            long usageCount = handler.getUsageCount();
            if (usageCount > 0) {
                restUsageMap.put(handler.getName(), usageCount);
            }
        });
        return restUsageMap;
    }

    /**
     * Returns the search usage holder
     */
    public SearchUsageHolder getSearchUsageHolder() {
        return searchUsageHolder;
    }
}
