/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.watcher;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;

/**
 * The request to get the watch by name (id)
 */
public final class GetWatchRequest implements Validatable {

    private final String id;

    public GetWatchRequest(String watchId) {
        validateId(watchId);
        this.id = watchId;
    }

    private void validateId(String id) {
        ValidationException exception = new ValidationException();
        if (id == null) {
            exception.addValidationError("watch id is missing");
        } else if (PutWatchRequest.isValidId(id) == false) {
            exception.addValidationError("watch id contains whitespace");
        }
        if (exception.validationErrors().isEmpty() == false) {
            throw exception;
        }
    }

    /**
     * @return The name of the watch to retrieve
     */
    public String getId() {
        return id;
    }
}
