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
package org.elasticsearch.client;

import java.util.Optional;

/**
 * Defines a validation layer for Requests.
 */
public interface Validatable {

    Validatable EMPTY = new Validatable() {};

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
