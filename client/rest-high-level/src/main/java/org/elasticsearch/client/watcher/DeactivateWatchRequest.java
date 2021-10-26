/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.watcher;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.watcher.PutWatchRequest;

import java.util.Objects;

public class DeactivateWatchRequest implements Validatable {
    private final String watchId;

    public DeactivateWatchRequest(String watchId) {
        Objects.requireNonNull(watchId, "watch id is missing");
        if (PutWatchRequest.isValidId(watchId) == false) {
            throw new IllegalArgumentException("watch id contains whitespace");
        }

        this.watchId = watchId;
    }

    public String getWatchId() {
        return watchId;
    }
}

