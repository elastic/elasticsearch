/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PostConnectorActionTests extends ESTestCase {

    public void testValidate_WhenConnectorIdAndIndexNamePresent_ExpectNoValidationError() {
        PostConnectorAction.Request request = new PostConnectorAction.Request(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomBoolean(),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10)
        );
        ActionRequestValidationException exception = request.validate();

        assertThat(exception, nullValue());
    }

    public void testValidate_WhenMalformedIndexName_ExpectValidationError() {
        PostConnectorAction.Request requestWithMissingConnectorId = new PostConnectorAction.Request(
            randomAlphaOfLength(10),
            "_illegal-index-name",
            randomBoolean(),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10)
        );
        ActionRequestValidationException exception = requestWithMissingConnectorId.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString("Invalid index name [_illegal-index-name]"));
    }
}
