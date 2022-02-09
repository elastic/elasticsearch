/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client;

import java.util.Optional;

/**
 * Defines a validation layer for Requests.
 */
public interface Validatable {

    Validatable EMPTY = new Validatable() {
    };

    /**
     * Perform validation. This method does not have to be overridden in the event that no validation needs to be done,
     * or the validation was done during object construction time. A {@link ValidationException} that is not null is
     * assumed to contain validation errors and will be thrown.
     *
     * @return An {@link Optional} {@link ValidationException} that contains a list of validation errors.
     */
    default Optional<ValidationException> validate() {
        return Optional.empty();
    }
}
