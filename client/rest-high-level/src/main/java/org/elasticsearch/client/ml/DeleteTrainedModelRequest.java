/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;

import java.util.Objects;
import java.util.Optional;

/**
 * Request to delete a data frame analytics config
 */
public class DeleteTrainedModelRequest implements Validatable {

    private final String id;

    public DeleteTrainedModelRequest(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    @Override
    public Optional<ValidationException> validate() {
        if (id == null) {
            return Optional.of(ValidationException.withError("trained model id must not be null"));
        }
        return Optional.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DeleteTrainedModelRequest other = (DeleteTrainedModelRequest) o;
        return Objects.equals(id, other.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
