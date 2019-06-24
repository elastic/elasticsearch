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

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.hamcrest.OptionalMatchers;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class RequestValidatorsTests extends ESTestCase {

    private final RequestValidators.RequestValidator<PutMappingRequest> EMPTY = (request, state, indices) -> Optional.empty();
    private final RequestValidators.RequestValidator<PutMappingRequest> FAIL =
            (request, state, indices) -> Optional.of(new Exception("failure"));

    public void testValidates() {
        final int numberOfValidations = randomIntBetween(0, 8);
        final List<RequestValidators.RequestValidator<PutMappingRequest>> validators = new ArrayList<>(numberOfValidations);
        for (int i = 0; i < numberOfValidations; i++) {
            validators.add(EMPTY);
        }
        final RequestValidators<PutMappingRequest> requestValidators = new RequestValidators<>(validators);
        assertThat(requestValidators.validateRequest(null, null, null), OptionalMatchers.isEmpty());
    }

    public void testFailure() {
        final RequestValidators<PutMappingRequest> validators = new RequestValidators<>(List.of(FAIL));
        assertThat(validators.validateRequest(null, null, null), OptionalMatchers.isPresent());
    }

    public void testValidatesAfterFailure() {
        final RequestValidators<PutMappingRequest> validators = new RequestValidators<>(List.of(FAIL, EMPTY));
        assertThat(validators.validateRequest(null, null, null), OptionalMatchers.isPresent());
    }

    public void testMultipleFailures() {
        final int numberOfFailures = randomIntBetween(2, 8);
        final List<RequestValidators.RequestValidator<PutMappingRequest>> validators = new ArrayList<>(numberOfFailures);
        for (int i = 0; i < numberOfFailures; i++) {
            validators.add(FAIL);
        }
        final RequestValidators<PutMappingRequest> requestValidators = new RequestValidators<>(validators);
        final Optional<Exception> e = requestValidators.validateRequest(null, null, null);
        assertThat(e, OptionalMatchers.isPresent());
        // noinspection OptionalGetWithoutIsPresent
        assertThat(e.get().getSuppressed(), Matchers.arrayWithSize(numberOfFailures - 1));
    }

    public void testRandom() {
        final int numberOfValidations = randomIntBetween(0, 8);
        final int numberOfFailures = randomIntBetween(0, 8);
        final List<RequestValidators.RequestValidator<PutMappingRequest>> validators =
                new ArrayList<>(numberOfValidations + numberOfFailures);
        for (int i = 0; i < numberOfValidations; i++) {
            validators.add(EMPTY);
        }
        for (int i = 0; i < numberOfFailures; i++) {
            validators.add(FAIL);
        }
        Randomness.shuffle(validators);
        final RequestValidators<PutMappingRequest> requestValidators = new RequestValidators<>(validators);
        final Optional<Exception> e = requestValidators.validateRequest(null, null, null);
        if (numberOfFailures == 0) {
            assertThat(e, OptionalMatchers.isEmpty());
        } else {
            assertThat(e, OptionalMatchers.isPresent());
            // noinspection OptionalGetWithoutIsPresent
            assertThat(e.get().getSuppressed(), Matchers.arrayWithSize(numberOfFailures - 1));
        }
    }

}
