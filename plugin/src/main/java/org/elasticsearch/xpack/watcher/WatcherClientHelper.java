/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.watcher.watch.Watch;

import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ClientHelper.WATCHER_ORIGIN;
import static org.elasticsearch.xpack.ClientHelper.stashWithOrigin;

/**
 * A helper class which decides if we should run via the xpack user and set watcher as origin or
 * if we should use the run_as functionality by setting the correct headers
 */
public class WatcherClientHelper {

    /**
     * Execute a client operation and return the response, try to run with least privileges, when headers exist
     *
     * @param watch     The watch in which context this method gets executed in
     * @param client    The client used to query
     * @param supplier  The action to run
     * @param <T>       The client response class this should return
     * @return          An instance of the response class
     */
    public static <T extends ActionResponse> T execute(Watch watch, Client client, Supplier<T> supplier) {
        // no headers, we will have to use the xpack internal user for our execution by specifying the watcher origin
        if (watch.status().getHeaders().isEmpty()) {
            try (ThreadContext.StoredContext ignore = stashWithOrigin(client.threadPool().getThreadContext(), WATCHER_ORIGIN)) {
                return supplier.get();
            }
        } else {
            try (ThreadContext.StoredContext ignored = client.threadPool().getThreadContext().stashContext()) {
                Map<String, String> filteredHeaders = watch.status().getHeaders().entrySet().stream()
                        .filter(e -> Watcher.HEADER_FILTERS.contains(e.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                client.threadPool().getThreadContext().copyHeaders(filteredHeaders.entrySet());
                return supplier.get();
            }
        }
    }
}
