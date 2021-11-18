/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;

import java.util.Objects;
import java.util.Optional;

/**
 * Request to delete a transform
 */
public class DeleteTransformRequest implements Validatable {

    public static final String FORCE = "force";

    private final String id;
    private Boolean force;

    public DeleteTransformRequest(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public Boolean getForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    @Override
    public Optional<ValidationException> validate() {
        if (id == null) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationError("transform id must not be null");
            return Optional.of(validationException);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, force);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DeleteTransformRequest other = (DeleteTransformRequest) obj;
        return Objects.equals(id, other.id) && Objects.equals(force, other.force);
    }
}
