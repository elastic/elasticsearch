/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform;

import org.elasticsearch.client.ValidationException;
import org.elasticsearch.test.ESTestCase;

import java.util.Optional;

import static org.hamcrest.Matchers.containsString;

public class StopTransformRequestTests extends ESTestCase {
    public void testValidate_givenNullId() {
        StopTransformRequest request = new StopTransformRequest(null);
        Optional<ValidationException> validate = request.validate();
        assertTrue(validate.isPresent());
        assertThat(validate.get().getMessage(), containsString("transform id must not be null"));
    }

    public void testValidate_givenValid() {
        StopTransformRequest request = new StopTransformRequest("foo");
        Optional<ValidationException> validate = request.validate();
        assertFalse(validate.isPresent());
    }
}
