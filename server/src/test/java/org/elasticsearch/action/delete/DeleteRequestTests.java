/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.delete;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;

public class DeleteRequestTests extends ESTestCase {

    public void testValidation() {
        {
            final DeleteRequest request = new DeleteRequest("index4", "0");
            final ActionRequestValidationException validate = request.validate();

            assertThat(validate, nullValue());
        }

        {
            final DeleteRequest request = new DeleteRequest("index4", null);
            final ActionRequestValidationException validate = request.validate();

            assertThat(validate, not(nullValue()));
            assertThat(validate.validationErrors(), hasItems("id is missing"));
        }

    }
}
