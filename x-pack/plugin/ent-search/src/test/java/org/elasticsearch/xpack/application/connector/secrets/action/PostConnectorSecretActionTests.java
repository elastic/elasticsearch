/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.secrets.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.application.connector.secrets.ConnectorSecretsTestUtils;

import static org.elasticsearch.xpack.application.connector.ConnectorTestUtils.NULL_STRING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PostConnectorSecretActionTests extends ESTestCase {

    public void testValidate_WhenConnectorSecretIdIsPresent_ExpectNoValidationError() {
        PostConnectorSecretRequest request = ConnectorSecretsTestUtils.getRandomPostConnectorSecretRequest();
        ActionRequestValidationException exception = request.validate();

        assertThat(exception, nullValue());
    }

    public void testValidate_WhenConnectorSecretIdIsNull_ExpectValidationError() {
        PostConnectorSecretRequest requestWithNullValue = new PostConnectorSecretRequest(NULL_STRING);
        ActionRequestValidationException exception = requestWithNullValue.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString("[value] of the connector secret cannot be [null] or [\"\"]"));
    }

    public void testValidate_WhenConnectorSecretIdIsBlank_ExpectValidationError() {
        PostConnectorSecretRequest requestWithMissingValue = new PostConnectorSecretRequest("");
        ActionRequestValidationException exception = requestWithMissingValue.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString("[value] of the connector secret cannot be [null] or [\"\"]"));
    }
}
