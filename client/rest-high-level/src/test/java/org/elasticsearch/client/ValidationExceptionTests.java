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
