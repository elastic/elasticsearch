/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.watcher;

import org.elasticsearch.client.Validatable;

import java.util.Objects;

/**
 * A request to explicitly activate a watch.
 */
public final class ActivateWatchRequest implements Validatable {

    private final String watchId;

    public ActivateWatchRequest(String watchId) {
        this.watchId  = Objects.requireNonNull(watchId, "Watch identifier is required");
        if (PutWatchRequest.isValidId(this.watchId) == false) {
            throw new IllegalArgumentException("Watch identifier contains whitespace");
        }
    }

    /**
     * @return The ID of the watch to be activated.
     */
    public String getWatchId() {
        return watchId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ActivateWatchRequest that = (ActivateWatchRequest) o;
        return Objects.equals(watchId, that.watchId);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(watchId);
        return result;
    }
}
