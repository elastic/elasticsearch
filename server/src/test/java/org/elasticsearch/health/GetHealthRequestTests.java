/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class GetHealthRequestTests extends ESTestCase {

    public void testValidation() {
        var req = new GetHealthAction.Request(true, -1);
        ActionRequestValidationException validationException = req.validate();
        assertThat(validationException, notNullValue());
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors().get(0), containsString("The size parameter must be a positive integer"));
    }
}
