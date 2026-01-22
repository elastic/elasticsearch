/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.index.Index;

import java.util.Collection;
import java.util.Optional;

public class RequestValidators<T extends ActionRequest> {

    private final Collection<RequestValidator<T>> validators;

    public RequestValidators(Collection<RequestValidator<T>> validators) {
        this.validators = validators;
    }

    public Optional<Exception> validateRequest(final T request, final ProjectMetadata projectMetadata, final Index[] indices) {
        Exception exception = null;
        for (final var validator : validators) {
            final Optional<Exception> maybeException = validator.validateRequest(request, projectMetadata, indices);
            if (maybeException.isEmpty()) continue;
            if (exception == null) {
                exception = maybeException.get();
            } else {
                exception.addSuppressed(maybeException.get());
            }
        }
        return Optional.ofNullable(exception);
    }

    /**
     * A validator that validates an request associated with indices before executing it.
     */
    public interface RequestValidator<T extends ActionRequest> {

        /**
         * Validates a given request with its associated concrete indices and the current state.
         *
         * @param request           the request to validate
         * @param projectMetadata   the current project metadata
         * @param indices           the concrete indices that associated with the given request
         * @return an optional exception indicates a reason that the given request should be aborted, otherwise empty
         */
        Optional<Exception> validateRequest(T request, ProjectMetadata projectMetadata, Index[] indices);

    }

}
