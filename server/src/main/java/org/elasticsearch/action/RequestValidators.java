/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.index.Index;

import java.util.Collection;
import java.util.Optional;

public class RequestValidators<T extends ActionRequest> {

        private final Collection<RequestValidator<T>> validators;

        public RequestValidators(Collection<RequestValidator<T>> validators) {
            this.validators = validators;
        }

        public Optional<Exception> validateRequest(final T request, final ClusterState state, final Index[] indices) {
            Exception exception = null;
            for (final var validator : validators) {
                final Optional<Exception> maybeException = validator.validateRequest(request, state, indices);
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
         * @param request the request to validate
         * @param state   the current cluster state
         * @param indices the concrete indices that associated with the given request
         * @return an optional exception indicates a reason that the given request should be aborted, otherwise empty
         */
        Optional<Exception> validateRequest(T request, ClusterState state, Index[] indices);

    }

}
