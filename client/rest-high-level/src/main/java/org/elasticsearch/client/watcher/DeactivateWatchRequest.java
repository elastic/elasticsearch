/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.client.watcher;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.protocol.xpack.watcher.PutWatchRequest;

import java.util.Optional;

public class DeactivateWatchRequest implements Validatable {
    private final String watchId;

    public DeactivateWatchRequest(String watchId) {
        this.watchId = watchId;
    }

    public String getWatchId() {
        return watchId;
    }

    @Override
    public Optional<ValidationException> validate() {
        ValidationException exception = new ValidationException();

        if (watchId == null) {
            exception.addValidationError("watch id is missing");
        } else if (PutWatchRequest.isValidId(watchId) == false) {
            exception.addValidationError("watch id contains whitespace");
        }

        return exception.validationErrors().isEmpty()
            ? Optional.empty()  // empty indicates no validation errors
            : Optional.of(exception);
    }
}
