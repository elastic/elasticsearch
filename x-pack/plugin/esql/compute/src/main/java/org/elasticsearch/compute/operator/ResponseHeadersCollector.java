/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * A helper class that can be used to collect and merge response headers from multiple child requests.
 */
public final class ResponseHeadersCollector {
    private final ThreadContext threadContext;
    private final Queue<Map<String, List<String>>> collected = ConcurrentCollections.newQueue();

    public ResponseHeadersCollector(ThreadContext threadContext) {
        this.threadContext = threadContext;
    }

    /**
     * Called when a child request is completed to collect the response headers of the responding thread
     */
    public void collect() {
        Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();
        if (responseHeaders.isEmpty() == false) {
            collected.add(responseHeaders);
        }
    }

    /**
     * Called when all child requests are completed. This will merge all collected response headers
     * from the child requests and restore to the current thread.
     */
    public void finish() {
        final Map<String, Set<String>> merged = new HashMap<>();
        Map<String, List<String>> resp;
        while ((resp = collected.poll()) != null) {
            for (Map.Entry<String, List<String>> e : resp.entrySet()) {
                // Use LinkedHashSet to retain the order of the values
                merged.computeIfAbsent(e.getKey(), k -> new LinkedHashSet<>(e.getValue().size())).addAll(e.getValue());
            }
        }
        for (Map.Entry<String, Set<String>> e : merged.entrySet()) {
            for (String v : e.getValue()) {
                threadContext.addResponseHeader(e.getKey(), v);
            }
        }
    }
}
