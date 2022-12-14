/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import java.util.HashSet;
import java.util.Set;

public class RuntimeExceptionHandler {

    private final Set<String> continueOnErrorFields = new HashSet<>();
    // private final ConcurrentMap<String, AtomicInteger> errorCounter = ConcurrentCollections.newConcurrentMap();

    public void handleError(RuntimeException ex, String fieldname) {
        if (continueOnErrorFields.contains(fieldname)) {
            // TODO count errors per field here
            // errorCounter.computeIfAbsent(fieldname, key -> new AtomicInteger(0)).incrementAndGet();
        } else {
            throw ex;
        }
    }

    public void continueOnErrorForField(String fieldname) {
        this.continueOnErrorFields.add(fieldname);
    }

    // public Map<String, AtomicInteger> getErrorCounters() {
    // return Collections.unmodifiableMap(errorCounter);
    // }
}
