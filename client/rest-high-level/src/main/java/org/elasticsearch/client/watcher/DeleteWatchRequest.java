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
 * A delete watch request to delete an watch by name (id)
 */
public class DeleteWatchRequest implements Validatable {

    private final String id;

    public DeleteWatchRequest(String id) {
        Objects.requireNonNull(id, "watch id is missing");
        if (PutWatchRequest.isValidId(id) == false) {
            throw new IllegalArgumentException("watch id contains whitespace");
        }
        this.id = id;
    }

    /**
     * @return The name of the watch to be deleted
     */
    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return "delete [" + id + "]";
    }
}
