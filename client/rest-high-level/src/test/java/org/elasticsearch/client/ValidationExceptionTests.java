/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;

public class ValidationExceptionTests extends ESTestCase {

    private static final String ERROR = "some-error";
    private static final String OTHER_ERROR = "some-other-error";

    public void testWithError() {
        ValidationException e = ValidationException.withError(ERROR, OTHER_ERROR);
        assertThat(e.validationErrors(), hasSize(2));
        assertThat(e.validationErrors(), contains(ERROR, OTHER_ERROR));
    }

    public void testWithErrors() {
        ValidationException e = ValidationException.withErrors(Arrays.asList(ERROR, OTHER_ERROR));
        assertThat(e.validationErrors(), hasSize(2));
        assertThat(e.validationErrors(), contains(ERROR, OTHER_ERROR));
    }

    public void testAddValidationError() {
        ValidationException e = new ValidationException();
        assertThat(e.validationErrors(), hasSize(0));
        e.addValidationError(ERROR);
        assertThat(e.validationErrors(), hasSize(1));
        assertThat(e.validationErrors(), contains(ERROR));
        e.addValidationError(OTHER_ERROR);
        assertThat(e.validationErrors(), hasSize(2));
        assertThat(e.validationErrors(), contains(ERROR, OTHER_ERROR));
    }
}
