/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresent;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresentWith;
import static org.hamcrest.Matchers.arrayWithSize;

public class RequestValidatorsTests extends ESTestCase {

    private final RequestValidators.RequestValidator<PutMappingRequest> EMPTY = (request, projectMetadata, indices) -> Optional.empty();
    private final RequestValidators.RequestValidator<PutMappingRequest> FAIL = (request, projectMetadata, indices) -> Optional.of(
        new Exception("failure")
    );

    public void testValidates() {
        final int numberOfValidations = randomIntBetween(0, 8);
        final List<RequestValidators.RequestValidator<PutMappingRequest>> validators = new ArrayList<>(numberOfValidations);
        for (int i = 0; i < numberOfValidations; i++) {
            validators.add(EMPTY);
        }
        final RequestValidators<PutMappingRequest> requestValidators = new RequestValidators<>(validators);
        assertThat(requestValidators.validateRequest(null, null, null), isEmpty());
    }

    public void testFailure() {
        final RequestValidators<PutMappingRequest> validators = new RequestValidators<>(List.of(FAIL));
        assertThat(validators.validateRequest(null, null, null), isPresent());
    }

    public void testValidatesAfterFailure() {
        final RequestValidators<PutMappingRequest> validators = new RequestValidators<>(List.of(FAIL, EMPTY));
        assertThat(validators.validateRequest(null, null, null), isPresent());
    }

    public void testMultipleFailures() {
        final int numberOfFailures = randomIntBetween(2, 8);
        final List<RequestValidators.RequestValidator<PutMappingRequest>> validators = new ArrayList<>(numberOfFailures);
        for (int i = 0; i < numberOfFailures; i++) {
            validators.add(FAIL);
        }
        final RequestValidators<PutMappingRequest> requestValidators = new RequestValidators<>(validators);
        final Optional<Exception> e = requestValidators.validateRequest(null, null, null);
        assertThat(e, isPresentWith(transformedMatch(Exception::getSuppressed, arrayWithSize(numberOfFailures - 1))));
    }

    public void testRandom() {
        final int numberOfValidations = randomIntBetween(0, 8);
        final int numberOfFailures = randomIntBetween(0, 8);
        final List<RequestValidators.RequestValidator<PutMappingRequest>> validators = new ArrayList<>(
            numberOfValidations + numberOfFailures
        );
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
            assertThat(e, isEmpty());
        } else {
            assertThat(e, isPresentWith(transformedMatch(Exception::getSuppressed, arrayWithSize(numberOfFailures - 1))));
        }
    }

}
